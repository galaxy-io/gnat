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

	"github.com/atterpac/gnat/internal/clipboard"
	"github.com/atterpac/gnat/internal/nats"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const maxMessages = 500

// monitorState is the reactive snapshot pushed to the UI via binding.Value.
// It is built by the processor goroutine and consumed by the render callback
// on the main goroutine. There are no shared mutable fields—every update
// creates a new snapshot value.
type monitorState struct {
	messages []nats.LiveMessage // all collected messages (newest last)
	filtered []nats.LiveMessage // after filter applied
	filter   string
	status   monitorStatus
	subject  string
	mode     string // "JS", "NATS", etc.
	policy   string // human-readable delivery policy label
	count    int    // total message count
	errMsg   string // non-empty on subscribe error
}

type monitorStatus int

const (
	statusIdle monitorStatus = iota
	statusSubscribing
	statusLive
	statusError
)

// MessageMonitor shows live messages for a subscribed subject.
type MessageMonitor struct {
	*components.MasterDetailView
	app *App

	// UI primitives (only touched on main goroutine)
	table        *components.Table
	preview      *tview.TextView
	subjectInput *tview.InputField
	statusText   *tview.TextView
	modeText     *tview.TextView
	topBar       *tview.Flex

	// Reactive state — processor goroutine writes, main goroutine reads via callback.
	state *binding.Value[monitorState]

	// Processor goroutine communication — the only shared mutable data.
	mu           sync.Mutex // guards sub, msgChan, gen, useJS, policy, filterText
	sub          nats.Subscription
	msgChan      chan nats.LiveMessage
	gen          int64 // subscription generation; incremented on every subscribe/unsubscribe
	useJetStream bool
	deliverPolicy nats.DeliverPolicy
	filterText   string

	// Lifecycle
	stopped     int32
	stopProcess chan struct{} // signals processor goroutine to exit

	// Initial subject (for auto-subscribe on Start)
	initSubject string

	// JSON path / pipeline filter for preview pane
	jsonFilter string
	pipeline   *Pipeline
}

// NewMessageMonitor creates the message monitor view.
func NewMessageMonitor(app *App) *MessageMonitor {
	return NewMessageMonitorWithSubject(app, "")
}

// NewMessageMonitorWithSubject creates the message monitor with a pre-filled subject.
func NewMessageMonitorWithSubject(app *App, subject string) *MessageMonitor {
	mm := &MessageMonitor{
		app:           app,
		useJetStream:  true,
		deliverPolicy: nats.DeliverAll,
		initSubject:   subject,
		stopProcess:   make(chan struct{}),
	}

	mm.buildUI(subject)
	mm.setupBinding()
	return mm
}

// ── UI construction (called once at init, on main goroutine) ───────────────

func (mm *MessageMonitor) buildUI(subject string) {
	// Table
	mm.table = components.NewTable().
		SetHeaders("TIME", "SUBJECT", "SIZE").
		ConfigureEmpty(theme.IconSignal, "No Messages", "")

	mm.table.SetSelectionChangedFunc(func(row, col int) {
		mm.renderPreview(row - 1) // offset for header
	})

	// Preview
	mm.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	mm.preview.SetBackgroundColor(theme.Bg())
	theme.Register(mm.preview)

	// Subject input
	mm.subjectInput = tview.NewInputField().
		SetLabel("Subject: ").
		SetFieldWidth(40).
		SetPlaceholder(">")
	mm.subjectInput.SetBackgroundColor(theme.Bg())
	mm.subjectInput.SetFieldBackgroundColor(theme.Bg())
	theme.Register(mm.subjectInput)
	mm.subjectInput.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			subj := mm.subjectInput.GetText()
			// subscribe is goroutine-safe; dispatch off main goroutine so
			// the NATS handshake cannot block the event loop.
			go mm.subscribe(subj)
		} else if key == tcell.KeyEscape {
			mm.app.app.SetFocus(mm.MasterDetailView)
		}
	})
	if subject != "" {
		mm.subjectInput.SetText(subject)
	}

	// Mode indicator
	mm.modeText = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignRight)
	mm.modeText.SetBackgroundColor(theme.Bg())
	theme.Register(mm.modeText)

	// Status text
	mm.statusText = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)
	mm.statusText.SetBackgroundColor(theme.Bg())
	theme.Register(mm.statusText)
	mm.statusText.SetText(fmt.Sprintf("[%s]Not subscribed[-]", theme.TagFgDim()))

	// Top bar
	mm.topBar = tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(mm.subjectInput, 0, 1, true).
		AddItem(mm.modeText, 20, 0, false).
		AddItem(mm.statusText, 25, 0, false)
	mm.topBar.SetBackgroundColor(theme.Bg())
	theme.Register(mm.topBar)

	// Master pane
	masterContent := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(mm.topBar, 1, 0, false).
		AddItem(mm.table, 0, 1, true)
	masterContent.SetBackgroundColor(theme.Bg())
	theme.Register(masterContent)

	// Master-detail view
	mm.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Messages").
		SetDetailTitle("Message Detail").
		SetMasterContent(masterContent).
		SetDetailContent(mm.preview).
		SetRatio(0.5)

	// Search integration
	mm.MasterDetailView.EnableSearch(func(currentText string, callbacks components.SearchCallbacks) {
		mm.app.statusBar.SetCommandPrompt("Filter: ")
		mm.app.statusBar.SetCommandPlaceholder("search messages...")
		mm.app.statusBar.EnterCommandMode()
		if currentText != "" {
			mm.app.statusBar.GetCommandInput().SetText(currentText)
		}
		mm.app.app.SetFocus(mm.app.statusBar.GetCommandInput())

		mm.app.statusBar.SetOnCommandSubmit(func(text string) {
			mm.app.statusBar.ExitCommandMode()
			callbacks.OnSubmit(text)
			mm.app.app.SetFocus(mm.MasterDetailView)
		})
		mm.app.statusBar.SetOnCommandCancel(func() {
			mm.app.statusBar.ExitCommandMode()
			callbacks.OnCancel()
			mm.app.app.SetFocus(mm.MasterDetailView)
		})
	})

	mm.MasterDetailView.SetOnSearch(func(query string) {
		go mm.setFilter(query)
	})
	mm.MasterDetailView.SetOnSearchSubmit(func(query string) {
		go mm.setFilter(query)
	})
	mm.MasterDetailView.SetOnSearchCancel(func() {})

	mm.renderModeText()
}

// ── Reactive binding ───────────────────────────────────────────────────────

func (mm *MessageMonitor) setupBinding() {
	mm.state = binding.NewValue(monitorState{})
	// BindToWithDraw fires the callback inside QueueUpdateDraw — safe for UI.
	mm.state.BindToWithDraw(func(s monitorState) {
		mm.renderState(s)
	})
}

// ── nav.Component lifecycle ────────────────────────────────────────────────

func (mm *MessageMonitor) CommandContext() CommandViewContext {
	if msg, ok := mm.getSelectedMessage(); ok {
		return CommandViewContext{Subject: msg.Subject}
	}
	s := mm.state.Get()
	return CommandViewContext{Subject: s.subject}
}

func (mm *MessageMonitor) Name() string {
	s := mm.state.Get()
	if s.subject != "" {
		return fmt.Sprintf("Monitor: %s", s.subject)
	}
	return "Monitor"
}

func (mm *MessageMonitor) Start() {
	// Start() runs on main goroutine — safe to touch UI directly.
	atomic.StoreInt32(&mm.stopped, 0)

	// Re-subscribe to whatever subject was active (or the initial one).
	subject := mm.state.Get().subject
	if subject == "" {
		subject = mm.initSubject
	}
	if subject != "" {
		go mm.subscribe(subject)
	}
}

func (mm *MessageMonitor) Stop() {
	atomic.StoreInt32(&mm.stopped, 1)
	// Signal the processor goroutine to exit.
	select {
	case mm.stopProcess <- struct{}{}:
	default:
	}
	mm.doUnsubscribe()
}

func (mm *MessageMonitor) Hints() []components.KeyHint {
	mm.mu.Lock()
	hasSub := mm.sub != nil
	useJS := mm.useJetStream
	mm.mu.Unlock()

	hints := []components.KeyHint{
		{Key: "Enter", Description: "View"},
		{Key: "/", Description: "Filter"},
		{Key: "f", Description: "JSON filter"},
		{Key: "F", Description: "Pipeline"},
		{Key: "p", Description: "Toggle preview"},
	}

	if hasSub {
		hints = append(hints, components.KeyHint{Key: "y", Description: "Yank"})
		hints = append(hints, components.KeyHint{Key: "w", Description: "Publish"})
		hints = append(hints, components.KeyHint{Key: "c", Description: "Clear"})
		hints = append(hints, components.KeyHint{Key: "u", Description: "Unsubscribe"})
		if useJS {
			hints = append(hints, components.KeyHint{Key: "v", Description: "Stream"})
			hints = append(hints, components.KeyHint{Key: "b", Description: "Browse"})
		}
	}

	if useJS {
		hints = append(hints, components.KeyHint{Key: "m", Description: "-> Core NATS"})
	} else {
		hints = append(hints, components.KeyHint{Key: "m", Description: "-> JetStream"})
	}

	if useJS && !hasSub {
		hints = append(hints, components.KeyHint{Key: "d", Description: "Delivery"})
	}

	return hints
}

// ── Input handling ─────────────────────────────────────────────────────────
//
// CRITICAL: input handlers run on the main goroutine. Calling QueueUpdateDraw
// from here deadlocks because the event loop is blocked processing this key.
// All methods that need QueueUpdateDraw (subscribe, unsubscribe, togglePause,
// etc.) are dispatched via `go` so they run outside the event loop.

func (mm *MessageMonitor) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return mm.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		// Subject input has focus — delegate or escape
		if mm.subjectInput.HasFocus() {
			if event.Key() == tcell.KeyEscape {
				setFocus(mm.MasterDetailView)
				return
			}
			if handler := mm.subjectInput.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
			return
		}

		// Escape — clear pipeline first, then JSON filter, then search filter, then back-nav
		if event.Key() == tcell.KeyEscape {
			if mm.pipeline != nil {
				mm.pipeline = nil
				mm.SetDetailTitle("Message Detail")
				row, _ := mm.table.GetSelection()
				mm.renderPreview(row - 1)
				return
			}
			if mm.jsonFilter != "" {
				mm.jsonFilter = ""
				mm.SetDetailTitle("Message Detail")
				row, _ := mm.table.GetSelection()
				mm.renderPreview(row - 1)
				return
			}
			if mm.GetSearchText() != "" {
				mm.ClearSearch()
				go mm.setFilter("")
				return
			}
			return
		}

		// Search key handling
		if mm.HandleSearchKey(event) {
			return
		}

		switch event.Rune() {
		case '/':
			mm.mu.Lock()
			hasSub := mm.sub != nil
			mm.mu.Unlock()
			if !hasSub {
				setFocus(mm.subjectInput)
				return
			}
			mm.ShowSearch()
			return
		case 'c':
			go mm.clearMessages()
			return
		case 'p':
			mm.ToggleDetail()
			return
		case 'u':
			go mm.unsubscribe()
			return
		case 'm':
			go mm.toggleMode()
			return
		case 'y':
			if msg, ok := mm.getSelectedMessage(); ok {
				if err := clipboard.Copy(string(msg.Data)); err != nil {
					mm.app.ShowError("Clipboard: " + err.Error())
				} else {
					mm.app.ShowSuccess(fmt.Sprintf("Copied %s", formatBytes(uint64(len(msg.Data)))))
				}
			}
			return
		case 'Y':
			if msg, ok := mm.getSelectedMessage(); ok {
				full := map[string]any{
					"subject":   msg.Subject,
					"timestamp": msg.Timestamp.Format(time.RFC3339),
					"sequence":  msg.Sequence,
					"headers":   msg.Headers,
				}
				var payload any
				if json.Unmarshal(msg.Data, &payload) == nil {
					full["payload"] = payload
				} else {
					full["payload"] = string(msg.Data)
				}
				if data, err := json.MarshalIndent(full, "", "  "); err == nil {
					if err := clipboard.Copy(string(data)); err != nil {
						mm.app.ShowError("Clipboard: " + err.Error())
					} else {
						mm.app.ShowSuccess("Copied full message")
					}
				}
			}
			return
		case 'R':
			if msg, ok := mm.getSelectedMessage(); ok {
				mm.showRepublishModal(msg)
			}
			return
		case 'w':
			mm.showPublishModal()
			return
		case 'v':
			if msg, ok := mm.getSelectedMessage(); ok && msg.Stream != "" {
				mm.app.NavigateToStreamDetail(msg.Stream)
			}
			return
		case 'n':
			if msg, ok := mm.getSelectedMessage(); ok && msg.Stream != "" {
				mm.app.NavigateToConsumers(msg.Stream)
			}
			return
		case 'b':
			if msg, ok := mm.getSelectedMessage(); ok && msg.Stream != "" {
				mm.app.NavigateToMessageBrowser(msg.Stream)
			}
			return
		case 'f':
			mm.showJSONFilterInput()
			return
		case 'F':
			mm.showPipelineInput()
			return
		case 'd':
			go mm.cycleDeliverPolicy()
			return
		}

		// Delegate unhandled keys to master-detail
		if handler := mm.MasterDetailView.InputHandler(); handler != nil {
			handler(event, setFocus)
		}
	})
}

// ── Subscribe / Unsubscribe ────────────────────────────────────────────────

func (mm *MessageMonitor) subscribe(subject string) {
	if subject == "" || atomic.LoadInt32(&mm.stopped) == 1 {
		return
	}

	// Tear down any existing subscription first.
	mm.doUnsubscribe()

	provider := mm.app.Provider()
	if provider == nil {
		return
	}

	// Prepare a fresh message channel and bump the generation.
	msgChan := make(chan nats.LiveMessage, 1000)

	mm.mu.Lock()
	mm.gen++
	myGen := mm.gen
	mm.msgChan = msgChan
	mm.filterText = ""
	useJS := mm.useJetStream
	policy := mm.deliverPolicy
	mm.mu.Unlock()

	// Push "subscribing" state.
	mm.state.SetAndDraw(monitorState{
		status:  statusSubscribing,
		subject: subject,
		mode:    mm.modeLabel(),
		policy:  mm.policyLabel(),
	})

	// Start processor goroutine for this subscription.
	go mm.processMessages(myGen, msgChan, subject)

	// Handler: never blocks, never holds locks. Channel send is guarded by
	// recover so a closed-channel panic in the narrow unsubscribe race is safe.
	handler := func(msg nats.LiveMessage) {
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}
		defer func() { recover() }() //nolint:errcheck
		select {
		case msgChan <- msg:
		default:
		}
	}

	// Perform the NATS subscription (blocking network call — must be off main goroutine).
	var sub nats.Subscription
	var err error
	var modeLabel string

	if useJS {
		sub, err = provider.SubscribeJetStream(context.Background(), subject, policy, handler)
		modeLabel = "JetStream"
	} else {
		sub, err = provider.Subscribe(context.Background(), subject, handler)
		modeLabel = "Core NATS"
	}

	// Fallback to core NATS if JetStream fails.
	if err != nil && useJS {
		sub, err = provider.Subscribe(context.Background(), subject, handler)
		if err == nil {
			modeLabel = "Core NATS (fallback)"
			mm.app.QueueUpdateDraw(func() {
				mm.app.ShowWarning("No JetStream stream found, using Core NATS")
			})
		}
	}

	if err != nil {
		mm.state.SetAndDraw(monitorState{
			status:  statusError,
			subject: subject,
			errMsg:  err.Error(),
			mode:    mm.modeLabel(),
			policy:  mm.policyLabel(),
		})
		mm.app.QueueUpdateDraw(func() {
			mm.app.ShowError(fmt.Sprintf("Subscribe failed: %v", err))
		})
		return
	}

	// Verify generation hasn't changed (concurrent unsubscribe while we waited).
	mm.mu.Lock()
	if mm.gen != myGen {
		mm.mu.Unlock()
		_ = sub.Unsubscribe()
		return
	}
	mm.sub = sub
	mm.mu.Unlock()

	mm.app.QueueUpdateDraw(func() {
		mm.app.ShowSuccess(fmt.Sprintf("Subscribed to %s (%s)", subject, modeLabel))
	})
}

// processMessages runs in its own goroutine for the lifetime of one subscription.
// It reads from msgChan, accumulates messages, and pushes state snapshots via
// the binding. No UI primitives are touched directly.
func (mm *MessageMonitor) processMessages(myGen int64, msgChan chan nats.LiveMessage, subject string) {
	var messages []nats.LiveMessage
	batchDone := false

	// batchTimer fires after a gap in initial message replay.
	batchTimer := time.NewTimer(500 * time.Millisecond)
	defer batchTimer.Stop()

	// renderTicker throttles live updates to avoid overwhelming the UI.
	renderTicker := time.NewTicker(200 * time.Millisecond)
	defer renderTicker.Stop()

	dirty := false // true when messages changed since last render

	pushState := func() {
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}
		mm.mu.Lock()
		if mm.gen != myGen {
			mm.mu.Unlock()
			return
		}
		filter := mm.filterText
		mm.mu.Unlock()

		// Build filtered view.
		filtered := filterMessages(messages, filter)

		// SetAndDraw is safe from any goroutine (queues internally).
		mm.state.SetAndDraw(monitorState{
			messages: messages,
			filtered: filtered,
			filter:   filter,
			status:   statusLive,
			subject:  subject,
			mode:     mm.modeLabel(),
			policy:   mm.policyLabel(),
			count:    len(messages),
		})
		dirty = false
	}

	for {
		select {
		case <-mm.stopProcess:
			return

		case msg, ok := <-msgChan:
			if !ok {
				// Channel closed — subscription torn down.
				return
			}

			messages = append(messages, msg)
			if len(messages) > maxMessages {
				messages = messages[len(messages)-maxMessages:]
			}
			dirty = true

			if !batchDone {
				// During initial replay, reset the batch timer on each message.
				batchTimer.Reset(500 * time.Millisecond)
				// If we've hit the cap, flush immediately.
				if len(messages) >= maxMessages {
					batchDone = true
					pushState()
				}
			}

		case <-batchTimer.C:
			// Initial replay gap detected — render the batch and switch to live mode.
			batchDone = true
			if dirty {
				pushState()
			}

		case <-renderTicker.C:
			// Throttled live render.
			if batchDone && dirty {
				pushState()
			}
		}
	}
}

// doUnsubscribe tears down the current subscription without UI feedback.
// Safe to call from any goroutine.
func (mm *MessageMonitor) doUnsubscribe() {
	mm.mu.Lock()
	sub := mm.sub
	ch := mm.msgChan
	mm.sub = nil
	mm.msgChan = nil
	mm.gen++
	mm.mu.Unlock()

	// Signal processor to exit.
	select {
	case mm.stopProcess <- struct{}{}:
	default:
	}
	// Re-create the stop channel for the next processor goroutine.
	mm.stopProcess = make(chan struct{})

	// Unsubscribe and close channel off any critical path.
	if sub != nil {
		_ = sub.Unsubscribe()
	}
	if ch != nil {
		close(ch)
	}
}

func (mm *MessageMonitor) unsubscribe() {
	mm.mu.Lock()
	hadSub := mm.sub != nil
	subject := ""
	s := mm.state.Get()
	subject = s.subject
	mm.mu.Unlock()

	mm.doUnsubscribe()

	if hadSub && atomic.LoadInt32(&mm.stopped) == 0 {
		mm.state.SetAndDraw(monitorState{
			status: statusIdle,
			mode:   mm.modeLabel(),
			policy: mm.policyLabel(),
		})
		mm.app.QueueUpdateDraw(func() {
			mm.app.ShowInfo(fmt.Sprintf("Unsubscribed from %s", subject))
		})
	}
}

// ── State rendering (runs on main goroutine via BindToWithDraw) ────────────

func (mm *MessageMonitor) renderState(s monitorState) {
	// Update status text
	dim := theme.TagFgDim()
	switch s.status {
	case statusIdle:
		mm.statusText.SetText(fmt.Sprintf("[%s]Not subscribed[-]", dim))
		mm.SetMasterTitle("Messages")
		mm.table.ConfigureEmpty(theme.IconSignal, "No Messages", "")
		mm.table.ClearRows()
		mm.preview.SetText("")
	case statusSubscribing:
		mm.statusText.SetText(fmt.Sprintf("[yellow]Subscribing...[-]"))
		mm.SetMasterTitle("Messages - Subscribing...")
	case statusError:
		mm.statusText.SetText(fmt.Sprintf("[red]Error[-]"))
		mm.SetMasterTitle("Messages")
		mm.table.ConfigureEmpty(theme.IconError, "Subscription Failed", s.errMsg)
	case statusLive:
		if s.filter != "" {
			mm.statusText.SetText(fmt.Sprintf("[green]Live[-] [%s]%d/%d[-]", dim, len(s.filtered), s.count))
		} else {
			mm.statusText.SetText(fmt.Sprintf("[green]Live[-] [%s]%d msgs[-]", dim, s.count))
		}
		mm.SetMasterTitle(fmt.Sprintf("Messages - %s", s.subject))
		mm.populateTable(s.filtered)
	}

	// Update mode indicator
	mm.renderModeText()
}

func (mm *MessageMonitor) populateTable(msgs []nats.LiveMessage) {
	mm.table.ClearRows()

	if len(msgs) == 0 {
		mm.table.ConfigureEmpty(theme.IconSignal, "Waiting for Messages", "Listening...")
		return
	}

	// Show newest first
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		mm.table.AddRow(
			msg.Timestamp.Format("15:04:05.000"),
			msg.Subject,
			formatBytes(uint64(len(msg.Data))),
		)
	}

	mm.table.SelectRow(0)
	mm.renderPreview(0)
}

func (mm *MessageMonitor) renderPreview(displayIdx int) {
	// displayIdx is in display order (newest first).
	// Get the current filtered messages from the binding snapshot.
	s := mm.state.Get()
	msgs := s.filtered

	if displayIdx < 0 || displayIdx >= len(msgs) {
		mm.preview.SetText("")
		return
	}

	// Convert display index (newest first) to slice index (oldest first).
	actualIdx := len(msgs) - 1 - displayIdx
	if actualIdx < 0 || actualIdx >= len(msgs) {
		mm.preview.SetText("")
		return
	}
	msg := msgs[actualIdx]

	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder

	fmt.Fprintf(&b, "[%s]Subject:[-]   [%s]%s[-]\n", dim, accent, msg.Subject)
	fmt.Fprintf(&b, "[%s]Time:[-]      %s\n", dim, msg.Timestamp.Format("2006-01-02 15:04:05.000"))
	fmt.Fprintf(&b, "[%s]Size:[-]      %s\n", dim, formatBytes(uint64(len(msg.Data))))

	if msg.Stream != "" {
		fmt.Fprintf(&b, "[%s]Stream:[-]    %s\n", dim, msg.Stream)
	}
	if msg.Sequence > 0 {
		fmt.Fprintf(&b, "[%s]Sequence:[-]  %d\n", dim, msg.Sequence)
	}

	if len(msg.Headers) > 0 {
		fmt.Fprintf(&b, "\n[%s]Headers:[-]\n", dim)
		for k, v := range msg.Headers {
			fmt.Fprintf(&b, "  [%s]%s:[-] %s\n", dim, k, strings.Join(v, ", "))
		}
	}

	// Apply pipeline if active
	if mm.pipeline != nil && json.Valid(msg.Data) {
		fmt.Fprintf(&b, "\n[%s]Pipeline:[-]  [yellow]%s[-]\n", dim, mm.pipeline.String())
		result, err := mm.pipeline.Execute(msg.Data)
		if err != nil {
			fmt.Fprintf(&b, "\n[red]Pipeline error: %s[-]\n", err.Error())
			fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
		} else {
			fmt.Fprintf(&b, "\n[%s]Result:[-]\n", dim)
			result = strings.ReplaceAll(result, "[", "[[")
			b.WriteString(result)

			mm.preview.SetText(b.String())
			mm.preview.ScrollToBeginning()
			mm.SetDetailContent(mm.preview)
			return
		}
	}

	// Apply JSON path filter if active
	if mm.jsonFilter != "" && json.Valid(msg.Data) {
		fmt.Fprintf(&b, "\n[%s]Filter:[-]    [yellow]%s[-]\n", dim, mm.jsonFilter)
		result, err := evaluateJSONPath(msg.Data, mm.jsonFilter)
		if err != nil {
			fmt.Fprintf(&b, "\n[red]Filter error: %s[-]\n", err.Error())
			fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
		} else {
			fmt.Fprintf(&b, "\n[%s]Result:[-]\n", dim)
			result = strings.ReplaceAll(result, "[", "[[")
			b.WriteString(result)

			mm.preview.SetText(b.String())
			mm.preview.ScrollToBeginning()
			mm.SetDetailContent(mm.preview)
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

	mm.preview.SetText(b.String())
	mm.preview.ScrollToBeginning()
	mm.SetDetailContent(mm.preview)
}

// ── Actions (dispatched via `go` from input handlers) ──────────────────────

func (mm *MessageMonitor) clearMessages() {
	mm.state.SetAndDraw(monitorState{
		status:  mm.state.Get().status,
		subject: mm.state.Get().subject,
		mode:    mm.modeLabel(),
		policy:  mm.policyLabel(),
	})
}

func (mm *MessageMonitor) setFilter(query string) {
	mm.mu.Lock()
	mm.filterText = query
	mm.mu.Unlock()

	// Re-filter from the current state snapshot.
	s := mm.state.Get()
	s.filter = query
	s.filtered = filterMessages(s.messages, query)
	mm.state.SetAndDraw(s)
}

func (mm *MessageMonitor) toggleMode() {
	mm.mu.Lock()
	hasSub := mm.sub != nil
	mm.mu.Unlock()

	if hasSub {
		mm.app.QueueUpdateDraw(func() {
			mm.app.ShowWarning("Unsubscribe first to change mode (press 'u')")
		})
		return
	}

	mm.mu.Lock()
	mm.useJetStream = !mm.useJetStream
	useJS := mm.useJetStream
	mm.mu.Unlock()

	// Update the mode label in the current state.
	s := mm.state.Get()
	s.mode = mm.modeLabel()
	s.policy = mm.policyLabel()
	mm.state.SetAndDraw(s)

	mm.app.QueueUpdateDraw(func() {
		if useJS {
			mm.app.ShowInfo("JetStream mode - can replay historical messages")
		} else {
			mm.app.ShowInfo("Core NATS mode - new messages only")
		}
	})
}

func (mm *MessageMonitor) cycleDeliverPolicy() {
	mm.mu.Lock()
	hasSub := mm.sub != nil
	useJS := mm.useJetStream
	mm.mu.Unlock()

	if !useJS {
		mm.app.QueueUpdateDraw(func() {
			mm.app.ShowInfo("Deliver policy only applies to JetStream mode")
		})
		return
	}
	if hasSub {
		mm.app.QueueUpdateDraw(func() {
			mm.app.ShowWarning("Unsubscribe first to change policy (press 'u')")
		})
		return
	}

	mm.mu.Lock()
	switch mm.deliverPolicy {
	case nats.DeliverAll:
		mm.deliverPolicy = nats.DeliverLast
	case nats.DeliverLast:
		mm.deliverPolicy = nats.DeliverNew
	case nats.DeliverNew:
		mm.deliverPolicy = nats.DeliverLastPerSubject
	case nats.DeliverLastPerSubject:
		mm.deliverPolicy = nats.DeliverAll
	}
	mm.mu.Unlock()

	s := mm.state.Get()
	s.policy = mm.policyLabel()
	mm.state.SetAndDraw(s)

	mm.app.QueueUpdateDraw(func() {
		mm.app.ShowInfo(fmt.Sprintf("Deliver: %s", mm.policyLabel()))
	})
}

// ── Helpers (no UI, no locks on binding) ───────────────────────────────────

func (mm *MessageMonitor) renderModeText() {
	mm.mu.Lock()
	useJS := mm.useJetStream
	policy := mm.deliverPolicy
	mm.mu.Unlock()

	accent := theme.TagAccent()
	dim := theme.TagFgDim()

	if useJS {
		var p string
		switch policy {
		case nats.DeliverAll:
			p = "All"
		case nats.DeliverLast:
			p = "Last"
		case nats.DeliverNew:
			p = "New"
		case nats.DeliverLastPerSubject:
			p = "LastPerSubj"
		}
		mm.modeText.SetText(fmt.Sprintf("[%s]JS[-] [%s]%s[-]", accent, dim, p))
	} else {
		mm.modeText.SetText(fmt.Sprintf("[%s]NATS[-]", dim))
	}
}

func (mm *MessageMonitor) modeLabel() string {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.useJetStream {
		return "JS"
	}
	return "NATS"
}

func (mm *MessageMonitor) policyLabel() string {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	switch mm.deliverPolicy {
	case nats.DeliverAll:
		return "All"
	case nats.DeliverLast:
		return "Last"
	case nats.DeliverNew:
		return "New"
	case nats.DeliverLastPerSubject:
		return "LastPerSubj"
	default:
		return "All"
	}
}

func (mm *MessageMonitor) getSelectedMessage() (nats.LiveMessage, bool) {
	s := mm.state.Get()
	row, _ := mm.table.GetSelection()
	displayIdx := row - 1 // account for header
	if displayIdx < 0 || displayIdx >= len(s.filtered) {
		return nats.LiveMessage{}, false
	}
	actualIdx := len(s.filtered) - 1 - displayIdx
	if actualIdx < 0 || actualIdx >= len(s.filtered) {
		return nats.LiveMessage{}, false
	}
	return s.filtered[actualIdx], true
}

func (mm *MessageMonitor) showRepublishModal(msg nats.LiveMessage) {
	modal := components.NewFormBuilder().
		Text("subject", "Subject").
		Value(msg.Subject).
		Placeholder("subject").
		Done().
		TextArea("payload", "Payload").
		Value(string(msg.Data)).
		Done().
		Text("header_key", "Header Key").
		Placeholder("optional").
		Done().
		Text("header_val", "Header Value").
		Placeholder("optional").
		Done().
		OnSubmit(func(values map[string]any) {
			subject := getString(values, "subject")
			if subject == "" {
				mm.app.ShowError("Subject is required")
				return
			}
			payload := getString(values, "payload")
			headers := make(map[string][]string)
			hk := getString(values, "header_key")
			hv := getString(values, "header_val")
			if hk != "" {
				headers[hk] = []string{hv}
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := mm.app.Provider().Publish(ctx, subject, []byte(payload), headers); err != nil {
					mm.app.ShowError(err.Error())
				} else {
					mm.app.ShowSuccess("Published to " + subject)
				}
			}()
		}).
		AsFormModal("Republish Message", 60, 18)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Publish"},
		{Key: "Esc", Description: "Cancel"},
	})

	mm.app.app.Pages().Push(modal)
}

func (mm *MessageMonitor) showPublishModal() {
	s := mm.state.Get()
	prefillSubject := s.subject

	modal := components.NewFormBuilder().
		Text("subject", "Subject").
		Value(prefillSubject).
		Placeholder("orders.new").
		Done().
		TextArea("payload", "Payload").
		Placeholder("message body").
		Done().
		Text("header_key", "Header Key").
		Placeholder("optional").
		Done().
		Text("header_val", "Header Value").
		Placeholder("optional").
		Done().
		Select("mode", "Mode", []string{"Publish", "Request"}).
		Done().
		Text("timeout", "Timeout (Request only)").
		Value("5s").
		Placeholder("5s").
		Done().
		OnSubmit(func(values map[string]any) {
			subject := getString(values, "subject")
			if subject == "" {
				mm.app.ShowError("Subject is required")
				return
			}
			payload := getString(values, "payload")
			headers := make(map[string][]string)
			hk := getString(values, "header_key")
			hv := getString(values, "header_val")
			if hk != "" {
				headers[hk] = []string{hv}
			}

			mode := getString(values, "mode")
			if mode == "Request" {
				timeoutStr := getString(values, "timeout")
				if timeoutStr == "" {
					timeoutStr = "5s"
				}
				timeout, err := time.ParseDuration(timeoutStr)
				if err != nil {
					mm.app.ShowError("Invalid timeout: " + err.Error())
					return
				}
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), timeout+2*time.Second)
					defer cancel()
					resp, err := mm.app.Provider().Request(ctx, subject, []byte(payload), headers, timeout)
					if err != nil {
						mm.app.ShowError("Request failed: " + err.Error())
						return
					}
					mm.app.QueueUpdateDraw(func() {
						mm.showRequestResponse(resp)
					})
				}()
			} else {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := mm.app.Provider().Publish(ctx, subject, []byte(payload), headers); err != nil {
						mm.app.ShowError(err.Error())
					} else {
						mm.app.ShowSuccess("Published to " + subject)
					}
				}()
			}
		}).
		AsFormModal("Publish / Request", 60, 22)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Send"},
		{Key: "Esc", Description: "Cancel"},
	})

	mm.app.app.Pages().Push(modal)
}

func (mm *MessageMonitor) showRequestResponse(resp *nats.RequestResponse) {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Subject:[-] [%s]%s[-]\n", dim, accent, resp.Subject)

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

	tv := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true)
	tv.SetBackgroundColor(theme.Bg())
	tv.SetText(b.String())

	modal := components.NewModal(components.ModalConfig{
		Title:  "Response",
		Width:  70,
		Height: 20,
	}).SetContent(tv)

	modal.SetHints([]components.KeyHint{
		{Key: "Esc", Description: "Close"},
	})

	mm.app.app.Pages().Push(modal)
}

func (mm *MessageMonitor) showJSONFilterInput() {
	mm.app.statusBar.SetCommandPrompt("jq: ")
	mm.app.statusBar.SetCommandPlaceholder(".field.nested")
	mm.app.statusBar.EnterCommandMode()
	if mm.jsonFilter != "" {
		mm.app.statusBar.GetCommandInput().SetText(mm.jsonFilter)
	}
	mm.app.app.SetFocus(mm.app.statusBar.GetCommandInput())

	mm.app.statusBar.SetOnCommandSubmit(func(text string) {
		mm.app.statusBar.ExitCommandMode()
		mm.jsonFilter = text
		if text != "" {
			mm.SetDetailTitle(fmt.Sprintf("Detail (jq: %s)", text))
		} else {
			mm.SetDetailTitle("Message Detail")
		}
		row, _ := mm.table.GetSelection()
		mm.renderPreview(row - 1)
		mm.app.app.SetFocus(mm.MasterDetailView)
	})
	mm.app.statusBar.SetOnCommandCancel(func() {
		mm.app.statusBar.ExitCommandMode()
		mm.app.app.SetFocus(mm.MasterDetailView)
	})
}

func (mm *MessageMonitor) showPipelineInput() {
	mm.app.statusBar.SetCommandPrompt("pipeline: ")
	mm.app.statusBar.SetCommandPlaceholder(".data | select(.status == \"active\") | map(.name)")
	mm.app.statusBar.EnterCommandMode()
	if mm.pipeline != nil {
		mm.app.statusBar.GetCommandInput().SetText(mm.pipeline.String())
	}
	mm.app.app.SetFocus(mm.app.statusBar.GetCommandInput())

	mm.app.statusBar.SetOnCommandSubmit(func(text string) {
		mm.app.statusBar.ExitCommandMode()
		if text == "" {
			mm.pipeline = nil
			mm.SetDetailTitle("Message Detail")
		} else {
			p, err := ParsePipeline(text)
			if err != nil {
				mm.app.ShowError("Pipeline: " + err.Error())
				mm.app.app.SetFocus(mm.MasterDetailView)
				return
			}
			mm.pipeline = p
			mm.SetDetailTitle(fmt.Sprintf("Detail (pipeline: %s)", text))
		}
		row, _ := mm.table.GetSelection()
		mm.renderPreview(row - 1)
		mm.app.app.SetFocus(mm.MasterDetailView)
	})
	mm.app.statusBar.SetOnCommandCancel(func() {
		mm.app.statusBar.ExitCommandMode()
		mm.app.app.SetFocus(mm.MasterDetailView)
	})
}

func filterMessages(msgs []nats.LiveMessage, filter string) []nats.LiveMessage {
	if filter == "" {
		// Return a copy so the caller's slice isn't aliased.
		out := make([]nats.LiveMessage, len(msgs))
		copy(out, msgs)
		return out
	}

	// Support header:Key=Value syntax
	if strings.HasPrefix(filter, "header:") {
		headerFilter := strings.TrimPrefix(filter, "header:")
		parts := strings.SplitN(headerFilter, "=", 2)
		headerKey := strings.TrimSpace(parts[0])
		headerVal := ""
		if len(parts) > 1 {
			headerVal = strings.TrimSpace(parts[1])
		}
		var out []nats.LiveMessage
		for _, msg := range msgs {
			if vals, ok := msg.Headers[headerKey]; ok {
				if headerVal == "" {
					out = append(out, msg)
				} else {
					for _, v := range vals {
						if strings.Contains(strings.ToLower(v), strings.ToLower(headerVal)) {
							out = append(out, msg)
							break
						}
					}
				}
			}
		}
		return out
	}

	lower := strings.ToLower(filter)
	var out []nats.LiveMessage
	for _, msg := range msgs {
		if strings.Contains(strings.ToLower(msg.Subject), lower) ||
			strings.Contains(strings.ToLower(string(msg.Data)), lower) {
			out = append(out, msg)
		}
	}
	return out
}
