package view

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/galaxy-io/gnat/internal/nats"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const browserPageBytes = 8 * 1024 * 1024  // 8 MB byte budget per page
const browserPageMsgs  = 50               // hard cap on messages per page
const previewMaxBytes  = 1 * 1024 * 1024  // 1 MB byte budget for preview fetches
const previewMaxMsgs   = 5                // hard cap on preview messages

type MessageBrowser struct {
	*components.MasterDetailView
	app        *App
	streamName string

	table   *components.Table
	preview *tview.TextView
	navBar  *tview.TextView

	mu       sync.Mutex
	messages []*nats.RawMessage
	totalMsgs uint64
	firstSeq  uint64
	lastSeq   uint64

	stopped int32

	// JSON path / pipeline filter for preview pane
	jsonFilter string
	pipeline   *Pipeline
}

func NewMessageBrowser(app *App, streamName string) *MessageBrowser {
	mb := &MessageBrowser{
		app:        app,
		streamName: streamName,
	}

	mb.table = components.NewTable().
		SetHeaders("SEQ", "SUBJECT", "TIME", "SIZE").
		ConfigureEmpty(theme.IconList, "No Messages", "")

	mb.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	mb.preview.SetBackgroundColor(theme.Bg())
	theme.Register(mb.preview)

	mb.navBar = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)
	mb.navBar.SetBackgroundColor(theme.Bg())
	theme.Register(mb.navBar)

	mb.table.SetSelectionChangedFunc(func(row, col int) {
		mb.renderPreview(row)
	})

	masterContent := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(mb.navBar, 1, 0, false).
		AddItem(mb.table, 0, 1, true)
	masterContent.SetBackgroundColor(theme.Bg())
	theme.Register(masterContent)

	mb.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle(fmt.Sprintf("Browse: %s", streamName)).
		SetDetailTitle("Message Detail").
		SetMasterContent(masterContent).
		SetDetailContent(mb.preview).
		SetRatio(0.5)

	return mb
}

func (mb *MessageBrowser) CommandContext() CommandViewContext {
	return CommandViewContext{Stream: mb.streamName}
}

func (mb *MessageBrowser) Name() string { return fmt.Sprintf("Browse: %s", mb.streamName) }

func (mb *MessageBrowser) Start() {
	atomic.StoreInt32(&mb.stopped, 0)
	go mb.loadInitial()
}

func (mb *MessageBrowser) Stop() {
	atomic.StoreInt32(&mb.stopped, 1)
}

func (mb *MessageBrowser) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "G", Description: "Last msg"},
		{Key: "0", Description: "First msg"},
		{Key: "w", Description: "Publish"},
		{Key: "R", Description: "Republish"},
		{Key: "y", Description: "Yank"},
		{Key: "f", Description: "JSON filter"},
		{Key: "F", Description: "Pipeline"},
		{Key: "x", Description: "Export page"},
		{Key: "p", Description: "Preview"},
		{Key: "r", Description: "Refresh"},
		{Key: "Esc", Description: "Back"},
	}
}

func (mb *MessageBrowser) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return mb.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		if event.Key() == tcell.KeyEscape {
			if mb.pipeline != nil {
				mb.pipeline = nil
				mb.SetDetailTitle("Message Detail")
				row, _ := mb.table.GetSelection()
				mb.renderPreview(row)
				return
			}
			if mb.jsonFilter != "" {
				mb.jsonFilter = ""
				mb.SetDetailTitle("Message Detail")
				row, _ := mb.table.GetSelection()
				mb.renderPreview(row)
				return
			}
		}
		switch {
		case event.Rune() == 'f':
			mb.showJSONFilterInput()
		case event.Rune() == 'F':
			mb.showPipelineInput()
		case event.Rune() == 'G':
			go mb.loadInitial()
		case event.Rune() == '0':
			go mb.loadFromSeq(mb.firstSeq)
		case event.Rune() == 'w':
			mb.showPublish()
		case event.Rune() == 'R':
			mb.mu.Lock()
			msgs := mb.messages
			mb.mu.Unlock()
			row, _ := mb.table.GetSelection()
			idx := row - 1
			if idx >= 0 && idx < len(msgs) {
				msg := msgs[idx]
				mb.showRepublishModal(msg)
			}
		case event.Rune() == 'y':
			mb.mu.Lock()
			msgs := mb.messages
			mb.mu.Unlock()
			row, _ := mb.table.GetSelection()
			idx := row - 1
			if idx >= 0 && idx < len(msgs) {
				msg := msgs[idx]
				if err := clipboard.Copy(string(msg.Data)); err != nil {
					mb.app.ShowError("Clipboard: " + err.Error())
				} else {
					mb.app.ShowSuccess(fmt.Sprintf("Copied %s", formatBytes(uint64(len(msg.Data)))))
				}
			}
		case event.Rune() == 'x':
			mb.mu.Lock()
			msgs := mb.messages
			mb.mu.Unlock()
			if len(msgs) > 0 {
				var lines []string
				for _, msg := range msgs {
					entry := map[string]interface{}{
						"sequence": msg.Sequence,
						"subject":  msg.Subject,
						"time":     msg.Time.Format(time.RFC3339),
						"headers":  msg.Headers,
					}
					var payload interface{}
					if json.Unmarshal(msg.Data, &payload) == nil {
						entry["payload"] = payload
					} else {
						entry["payload"] = string(msg.Data)
					}
					line, _ := json.Marshal(entry)
					lines = append(lines, string(line))
				}
				if err := clipboard.Copy(strings.Join(lines, "\n")); err != nil {
					mb.app.ShowError("Clipboard: " + err.Error())
				} else {
					mb.app.ShowSuccess(fmt.Sprintf("Exported %d messages to clipboard", len(msgs)))
				}
			}
		case event.Rune() == 'p':
			mb.ToggleDetail()
		case event.Rune() == 'r':
			go mb.loadInitial()
		default:
			if handler := mb.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (mb *MessageBrowser) loadInitial() {
	if atomic.LoadInt32(&mb.stopped) == 1 {
		return
	}

	provider := mb.app.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	info, err := provider.GetStreamInfo(ctx, mb.streamName)
	if err != nil {
		mb.app.QueueUpdateDraw(func() {
			mb.table.ConfigureEmpty(theme.IconError, "Error", err.Error())
		})
		return
	}

	mb.mu.Lock()
	mb.totalMsgs = info.State.Msgs
	mb.firstSeq = info.State.FirstSeq
	mb.lastSeq = info.State.LastSeq
	mb.mu.Unlock()

	if mb.totalMsgs == 0 {
		mb.app.QueueUpdateDraw(func() {
			mb.table.ConfigureEmpty(theme.IconList, "Empty Stream", "")
			mb.updateNavBar()
		})
		return
	}

	mb.fetchPageReverse(ctx, provider, mb.lastSeq)
}

func (mb *MessageBrowser) loadFromSeq(seq uint64) {
	if atomic.LoadInt32(&mb.stopped) == 1 {
		return
	}

	provider := mb.app.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	mb.mu.Lock()
	lastSeq := mb.lastSeq
	mb.mu.Unlock()

	mb.fetchPageForward(ctx, provider, seq, lastSeq)
}

func (mb *MessageBrowser) fetchPageForward(ctx context.Context, provider nats.Provider, startSeq, endSeq uint64) {
	var msgs []*nats.RawMessage
	var totalBytes int
	skipped := 0

	for seq := startSeq; seq <= endSeq; seq++ {
		if atomic.LoadInt32(&mb.stopped) == 1 {
			return
		}

		msg, err := provider.GetMessage(ctx, mb.streamName, seq)
		if err != nil {
			skipped++
			if skipped > 500 {
				break
			}
			continue
		}
		totalBytes += len(msg.Data)
		msgs = append(msgs, msg)
		if totalBytes >= browserPageBytes || len(msgs) >= browserPageMsgs {
			break
		}
	}

	if atomic.LoadInt32(&mb.stopped) == 1 {
		return
	}

	mb.mu.Lock()
	mb.messages = msgs
	mb.mu.Unlock()

	mb.app.QueueUpdateDraw(func() {
		mb.populateTable()
		mb.updateNavBar()
	})
}

func (mb *MessageBrowser) fetchPageReverse(ctx context.Context, provider nats.Provider, endSeq uint64) {
	var msgs []*nats.RawMessage
	var totalBytes int
	skipped := 0

	mb.mu.Lock()
	firstSeq := mb.firstSeq
	mb.mu.Unlock()

	for seq := endSeq; seq >= firstSeq && seq > 0; seq-- {
		if atomic.LoadInt32(&mb.stopped) == 1 {
			return
		}

		msg, err := provider.GetMessage(ctx, mb.streamName, seq)
		if err != nil {
			skipped++
			if skipped > 500 {
				break
			}
			continue
		}
		totalBytes += len(msg.Data)
		msgs = append(msgs, msg)
		if totalBytes >= browserPageBytes || len(msgs) >= browserPageMsgs {
			break
		}
	}

	// Reverse so messages are in ascending sequence order.
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	if atomic.LoadInt32(&mb.stopped) == 1 {
		return
	}

	mb.mu.Lock()
	mb.messages = msgs
	mb.mu.Unlock()

	mb.app.QueueUpdateDraw(func() {
		mb.populateTable()
		mb.updateNavBar()
	})
}

func (mb *MessageBrowser) populateTable() {
	mb.table.ClearRows()

	mb.mu.Lock()
	msgs := mb.messages
	mb.mu.Unlock()

	if len(msgs) == 0 {
		mb.table.ConfigureEmpty(theme.IconList, "No Messages", "")
		return
	}

	for _, msg := range msgs {
		mb.table.AddRow(
			fmt.Sprintf("%d", msg.Sequence),
			msg.Subject,
			msg.Time.Format("15:04:05"),
			formatBytes(uint64(len(msg.Data))),
		)
	}

	mb.table.SelectRow(0)
	mb.renderPreview(1)
}

func (mb *MessageBrowser) updateNavBar() {
	mb.mu.Lock()
	total := mb.totalMsgs
	first := mb.firstSeq
	last := mb.lastSeq
	msgs := mb.messages
	mb.mu.Unlock()

	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	if len(msgs) == 0 {
		mb.navBar.SetText(fmt.Sprintf("[%s]%s[-] [%s]%s total msgs, seq %d–%d[-]",
			accent, mb.streamName, dim, formatNumber(total), first, last))
		return
	}

	pageFirst := msgs[0].Sequence
	pageLast := msgs[len(msgs)-1].Sequence

	mb.navBar.SetText(fmt.Sprintf("[%s]%s[-] [%s]showing %d–%d of %s msgs (seq %d–%d)[-]",
		accent, mb.streamName, dim, pageFirst, pageLast, formatNumber(total), first, last))
}

func (mb *MessageBrowser) renderPreview(row int) {
	mb.mu.Lock()
	msgs := mb.messages
	mb.mu.Unlock()

	idx := row - 1
	if idx < 0 || idx >= len(msgs) {
		mb.preview.SetText("")
		return
	}
	msg := msgs[idx]

	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Subject:[-]   [%s]%s[-]\n", dim, accent, msg.Subject)
	fmt.Fprintf(&b, "[%s]Sequence:[-]  %d\n", dim, msg.Sequence)
	fmt.Fprintf(&b, "[%s]Time:[-]      %s\n", dim, msg.Time.Format("2006-01-02 15:04:05.000"))
	fmt.Fprintf(&b, "[%s]Size:[-]      %s\n", dim, formatBytes(uint64(len(msg.Data))))

	if len(msg.Headers) > 0 {
		fmt.Fprintf(&b, "\n[%s]Headers:[-]\n", dim)
		for k, v := range msg.Headers {
			fmt.Fprintf(&b, "  [%s]%s:[-] %s\n", dim, k, strings.Join(v, ", "))
		}
	}

	// Apply pipeline if active
	if mb.pipeline != nil && json.Valid(msg.Data) {
		fmt.Fprintf(&b, "\n[%s]Pipeline:[-]  [yellow]%s[-]\n", dim, mb.pipeline.String())
		result, err := mb.pipeline.Execute(msg.Data)
		if err != nil {
			fmt.Fprintf(&b, "\n[red]Pipeline error: %s[-]\n", err.Error())
			fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
		} else {
			fmt.Fprintf(&b, "\n[%s]Result:[-]\n", dim)
			result = strings.ReplaceAll(result, "[", "[[")
			b.WriteString(result)

			mb.preview.SetText(b.String())
			mb.preview.ScrollToBeginning()
			return
		}
	}

	// Apply JSON path filter if active
	if mb.jsonFilter != "" && json.Valid(msg.Data) {
		fmt.Fprintf(&b, "\n[%s]Filter:[-]    [yellow]%s[-]\n", dim, mb.jsonFilter)
		result, err := evaluateJSONPath(msg.Data, mb.jsonFilter)
		if err != nil {
			fmt.Fprintf(&b, "\n[red]Filter error: %s[-]\n", err.Error())
			fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
		} else {
			fmt.Fprintf(&b, "\n[%s]Result:[-]\n", dim)
			result = strings.ReplaceAll(result, "[", "[[")
			b.WriteString(result)

			mb.preview.SetText(b.String())
			mb.preview.ScrollToBeginning()
			return
		}
	}

	fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
	data := string(msg.Data)
	if json.Valid(msg.Data) {
		var prettyJSON bytes.Buffer
		if err := json.Indent(&prettyJSON, msg.Data, "", "  "); err == nil {
			data = prettyJSON.String()
		}
	}
	data = strings.ReplaceAll(data, "[", "[[")
	b.WriteString(data)

	mb.preview.SetText(b.String())
	mb.preview.ScrollToBeginning()
}

func (mb *MessageBrowser) showJSONFilterInput() {
	mb.app.statusBar.SetCommandPrompt("jq: ")
	mb.app.statusBar.SetCommandPlaceholder(".field.nested")
	mb.app.statusBar.EnterCommandMode()
	if mb.jsonFilter != "" {
		mb.app.statusBar.GetCommandInput().SetText(mb.jsonFilter)
	}
	mb.app.app.SetFocus(mb.app.statusBar.GetCommandInput())

	mb.app.statusBar.SetOnCommandSubmit(func(text string) {
		mb.app.statusBar.ExitCommandMode()
		mb.jsonFilter = text
		if text != "" {
			mb.SetDetailTitle(fmt.Sprintf("Detail (jq: %s)", text))
		} else {
			mb.SetDetailTitle("Message Detail")
		}
		row, _ := mb.table.GetSelection()
		mb.renderPreview(row)
		mb.app.app.SetFocus(mb.MasterDetailView)
	})
	mb.app.statusBar.SetOnCommandCancel(func() {
		mb.app.statusBar.ExitCommandMode()
		mb.app.app.SetFocus(mb.MasterDetailView)
	})
}

func (mb *MessageBrowser) showPipelineInput() {
	mb.app.statusBar.SetCommandPrompt("pipeline: ")
	mb.app.statusBar.SetCommandPlaceholder(".data | select(.status == \"active\") | map(.name)")
	mb.app.statusBar.EnterCommandMode()
	if mb.pipeline != nil {
		mb.app.statusBar.GetCommandInput().SetText(mb.pipeline.String())
	}
	mb.app.app.SetFocus(mb.app.statusBar.GetCommandInput())

	mb.app.statusBar.SetOnCommandSubmit(func(text string) {
		mb.app.statusBar.ExitCommandMode()
		if text == "" {
			mb.pipeline = nil
			mb.SetDetailTitle("Message Detail")
		} else {
			p, err := ParsePipeline(text)
			if err != nil {
				mb.app.ShowError("Pipeline: " + err.Error())
				mb.app.app.SetFocus(mb.MasterDetailView)
				return
			}
			mb.pipeline = p
			mb.SetDetailTitle(fmt.Sprintf("Detail (pipeline: %s)", text))
		}
		row, _ := mb.table.GetSelection()
		mb.renderPreview(row)
		mb.app.app.SetFocus(mb.MasterDetailView)
	})
	mb.app.statusBar.SetOnCommandCancel(func() {
		mb.app.statusBar.ExitCommandMode()
		mb.app.app.SetFocus(mb.MasterDetailView)
	})
}

func (mb *MessageBrowser) showRepublishModal(msg *nats.RawMessage) {
	modal := components.NewFormBuilder().
		Text("subject", "Subject").
		Value(msg.Subject).
		Placeholder("subject").
		Done().
		TextArea("payload", "Payload").
		Value(string(msg.Data)).
		Done().
		OnSubmit(func(values map[string]any) {
			subject := getString(values, "subject")
			if subject == "" {
				mb.app.ShowError("Subject is required")
				return
			}
			payload := getString(values, "payload")
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := mb.app.Provider().Publish(ctx, subject, []byte(payload), nil); err != nil {
					mb.app.ShowError(err.Error())
				} else {
					mb.app.ShowSuccess("Published to " + subject)
				}
			}()
		}).
		AsFormModal("Republish Message", 60, 14)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Publish"},
		{Key: "Esc", Description: "Cancel"},
	})

	mb.app.app.Pages().Push(modal)
}

func (mb *MessageBrowser) showPublish() {
	s := mb.streamName
	modal := components.NewFormBuilder().
		Text("subject", "Subject").
		Placeholder("subject").
		Done().
		TextArea("payload", "Payload").
		Placeholder("message body").
		Done().
		OnSubmit(func(values map[string]any) {
			subject := getString(values, "subject")
			if subject == "" {
				mb.app.ShowError("Subject is required")
				return
			}
			payload := getString(values, "payload")
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := mb.app.Provider().Publish(ctx, subject, []byte(payload), nil); err != nil {
					mb.app.ShowError(err.Error())
				} else {
					mb.app.ShowSuccess("Published to " + subject)
				}
			}()
		}).
		AsFormModal(fmt.Sprintf("Publish to %s", s), 60, 14)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Publish"},
		{Key: "Esc", Description: "Cancel"},
	})

	mb.app.app.Pages().Push(modal)
}
