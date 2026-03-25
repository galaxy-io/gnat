package view

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/galaxy-io/gnat/internal/nats"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type requestEntry struct {
	Subject  string
	Payload  string
	Headers  map[string][]string
	Timeout  time.Duration
	Response *nats.RequestResponse
	Error    error
	Latency  time.Duration
	Time     time.Time
}

type requestReplyState struct {
	history []requestEntry
	current int // index into history for display, -1 = none
}

// RequestReply provides an interactive request/reply tester.
type RequestReply struct {
	*components.Split
	app *App

	// Left pane: form
	subjectInput *tview.InputField
	payloadArea  *tview.TextArea
	timeoutInput *tview.InputField
	headerKey    *tview.InputField
	headerVal    *tview.InputField
	formFlex     *tview.Flex

	// Right pane: response + history
	responseView *tview.TextView
	historyTable *components.Table
	rightFlex    *tview.Flex

	state *binding.Value[requestReplyState]

	// Focus cycling
	focusItems []tview.Primitive
	focusIdx   int
}

func NewRequestReply(app *App, subject string) *RequestReply {
	rr := &RequestReply{app: app}

	// Form inputs
	rr.subjectInput = tview.NewInputField().
		SetLabel("Subject: ").
		SetFieldWidth(0).
		SetPlaceholder("orders.get")
	rr.subjectInput.SetBackgroundColor(theme.Bg())
	rr.subjectInput.SetFieldBackgroundColor(theme.Bg())
	theme.Register(rr.subjectInput)
	if subject != "" {
		rr.subjectInput.SetText(subject)
	}

	rr.payloadArea = tview.NewTextArea().
		SetPlaceholder("request payload...")
	rr.payloadArea.SetBackgroundColor(theme.Bg())
	theme.Register(rr.payloadArea)

	rr.timeoutInput = tview.NewInputField().
		SetLabel("Timeout: ").
		SetFieldWidth(0).
		SetPlaceholder("5s").
		SetText("5s")
	rr.timeoutInput.SetBackgroundColor(theme.Bg())
	rr.timeoutInput.SetFieldBackgroundColor(theme.Bg())
	theme.Register(rr.timeoutInput)

	rr.headerKey = tview.NewInputField().
		SetLabel("Header: ").
		SetFieldWidth(0).
		SetPlaceholder("key")
	rr.headerKey.SetBackgroundColor(theme.Bg())
	rr.headerKey.SetFieldBackgroundColor(theme.Bg())
	theme.Register(rr.headerKey)

	rr.headerVal = tview.NewInputField().
		SetLabel("= ").
		SetFieldWidth(0).
		SetPlaceholder("value")
	rr.headerVal.SetBackgroundColor(theme.Bg())
	rr.headerVal.SetFieldBackgroundColor(theme.Bg())
	theme.Register(rr.headerVal)

	headerRow := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(rr.headerKey, 0, 1, false).
		AddItem(rr.headerVal, 0, 1, false)
	headerRow.SetBackgroundColor(theme.Bg())
	theme.Register(headerRow)

	// Left pane layout
	rr.formFlex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(rr.subjectInput, 1, 0, true).
		AddItem(rr.payloadArea, 0, 1, false).
		AddItem(headerRow, 1, 0, false).
		AddItem(rr.timeoutInput, 1, 0, false)
	rr.formFlex.SetBackgroundColor(theme.Bg())
	theme.Register(rr.formFlex)

	formPanel := components.NewPanel().SetTitle("Request").SetContent(rr.formFlex)

	// Right pane: response view + history table
	rr.responseView = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	rr.responseView.SetBackgroundColor(theme.Bg())
	theme.Register(rr.responseView)
	rr.responseView.SetText(fmt.Sprintf("[%s]Send a request with Ctrl+S[-]", theme.TagFgDim()))

	rr.historyTable = components.NewTable().
		SetHeaders("TIME", "SUBJECT", "STATUS", "LATENCY").
		ConfigureEmpty(theme.IconSignal, "No History", "")

	rr.historyTable.SetSelectionChangedFunc(func(row, col int) {
		rr.showHistoryEntry(row - 1)
	})

	rr.rightFlex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(rr.responseView, 0, 3, false).
		AddItem(rr.historyTable, 0, 1, false)
	rr.rightFlex.SetBackgroundColor(theme.Bg())
	theme.Register(rr.rightFlex)

	responsePanel := components.NewPanel().SetTitle("Response").SetContent(rr.rightFlex)

	rr.Split = components.NewSplit().
		SetDirection(components.SplitVertical).
		SetRatio(0.4).
		SetLeft(formPanel).
		SetRight(responsePanel)

	// Focus cycling order
	rr.focusItems = []tview.Primitive{
		rr.subjectInput, rr.payloadArea, rr.headerKey, rr.headerVal, rr.timeoutInput, rr.historyTable,
	}

	// Setup binding
	rr.state = binding.NewValue(requestReplyState{current: -1})
	rr.state.BindToWithDraw(func(s requestReplyState) {
		rr.renderHistory(s)
	})

	// Enter on subject = send
	rr.subjectInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			go rr.sendRequest()
		}
	})

	return rr
}

func (rr *RequestReply) Name() string { return "Request/Reply" }
func (rr *RequestReply) Start()       {}
func (rr *RequestReply) Stop()        {}

func (rr *RequestReply) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Ctrl+S", Description: "Send"},
		{Key: "Tab", Description: "Next field"},
		{Key: "y", Description: "Yank response"},
		{Key: "c", Description: "Clear"},
		{Key: "Esc", Description: "Back"},
	}
}

func (rr *RequestReply) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return rr.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Key() == tcell.KeyCtrlS:
			go rr.sendRequest()
			return
		case event.Key() == tcell.KeyTab:
			rr.focusIdx = (rr.focusIdx + 1) % len(rr.focusItems)
			rr.app.app.SetFocus(rr.focusItems[rr.focusIdx])
			return
		case event.Key() == tcell.KeyBacktab:
			rr.focusIdx--
			if rr.focusIdx < 0 {
				rr.focusIdx = len(rr.focusItems) - 1
			}
			rr.app.app.SetFocus(rr.focusItems[rr.focusIdx])
			return
		case event.Rune() == 'y' && rr.historyTable.HasFocus():
			s := rr.state.Get()
			if s.current >= 0 && s.current < len(s.history) {
				entry := s.history[s.current]
				if entry.Response != nil {
					if err := clipboard.Copy(string(entry.Response.Data)); err != nil {
						rr.app.ShowError("Clipboard: " + err.Error())
					} else {
						rr.app.ShowSuccess("Copied response payload")
					}
				}
			}
			return
		case event.Rune() == 'c' && rr.historyTable.HasFocus():
			rr.state.SetAndDraw(requestReplyState{current: -1})
			rr.responseView.SetText(fmt.Sprintf("[%s]Send a request with Ctrl+S[-]", theme.TagFgDim()))
			return
		}

		// Delegate to focused primitive
		focused := rr.focusItems[rr.focusIdx]
		if handler := focused.InputHandler(); handler != nil {
			handler(event, setFocus)
		}
	})
}

func (rr *RequestReply) sendRequest() {
	subject := rr.subjectInput.GetText()
	if subject == "" {
		rr.app.ShowWarning("Subject is required")
		return
	}

	payload := rr.payloadArea.GetText()
	timeoutStr := rr.timeoutInput.GetText()
	if timeoutStr == "" {
		timeoutStr = "5s"
	}
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		rr.app.ShowError("Invalid timeout: " + err.Error())
		return
	}

	headers := make(map[string][]string)
	hk := rr.headerKey.GetText()
	hv := rr.headerVal.GetText()
	if hk != "" {
		headers[hk] = []string{hv}
	}

	rr.app.QueueUpdateDraw(func() {
		rr.responseView.SetText("[yellow]Sending request...[-]")
	})

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout+2*time.Second)
	defer cancel()

	resp, reqErr := rr.app.Provider().Request(ctx, subject, []byte(payload), headers, timeout)
	latency := time.Since(start)

	entry := requestEntry{
		Subject:  subject,
		Payload:  payload,
		Headers:  headers,
		Timeout:  timeout,
		Response: resp,
		Error:    reqErr,
		Latency:  latency,
		Time:     time.Now(),
	}

	s := rr.state.Get()
	history := append([]requestEntry{entry}, s.history...)
	if len(history) > 50 {
		history = history[:50]
	}

	rr.state.SetAndDraw(requestReplyState{
		history: history,
		current: 0,
	})

	rr.app.QueueUpdateDraw(func() {
		rr.renderResponse(entry)
		if reqErr != nil {
			rr.app.ShowError("Request failed: " + reqErr.Error())
		} else {
			rr.app.ShowSuccess(fmt.Sprintf("Response in %s", latency.Round(time.Millisecond)))
		}
	})
}

func (rr *RequestReply) renderResponse(entry requestEntry) {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder

	if entry.Error != nil {
		fmt.Fprintf(&b, "[red]Error:[-] %s\n", entry.Error.Error())
		fmt.Fprintf(&b, "[%s]Latency:[-] %s\n", dim, entry.Latency.Round(time.Millisecond))
		rr.responseView.SetText(b.String())
		return
	}

	resp := entry.Response
	fmt.Fprintf(&b, "[%s]Subject:[-]  [%s]%s[-]\n", dim, accent, resp.Subject)
	fmt.Fprintf(&b, "[%s]Latency:[-]  %s\n", dim, entry.Latency.Round(time.Millisecond))
	fmt.Fprintf(&b, "[%s]Size:[-]     %s\n", dim, formatBytes(uint64(len(resp.Data))))

	if len(resp.Headers) > 0 {
		fmt.Fprintf(&b, "\n[%s]Headers:[-]\n", dim)
		for k, v := range resp.Headers {
			fmt.Fprintf(&b, "  [%s]%s:[-] %s\n", dim, k, strings.Join(v, ", "))
		}
	}

	fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
	data := string(resp.Data)
	if json.Valid(resp.Data) {
		var prettyJSON bytes.Buffer
		if err := json.Indent(&prettyJSON, resp.Data, "", "  "); err == nil {
			data = prettyJSON.String()
		}
	}
	data = strings.ReplaceAll(data, "[", "[[")
	b.WriteString(data)

	rr.responseView.SetText(b.String())
	rr.responseView.ScrollToBeginning()
}

func (rr *RequestReply) renderHistory(s requestReplyState) {
	rr.historyTable.ClearRows()
	for _, entry := range s.history {
		status := "[green]OK[-]"
		if entry.Error != nil {
			status = "[red]ERR[-]"
		}
		rr.historyTable.AddRow(
			entry.Time.Format("15:04:05"),
			entry.Subject,
			status,
			entry.Latency.Round(time.Millisecond).String(),
		)
	}
}

func (rr *RequestReply) showHistoryEntry(idx int) {
	s := rr.state.Get()
	if idx < 0 || idx >= len(s.history) {
		return
	}
	s.current = idx
	rr.renderResponse(s.history[idx])
}
