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

	"github.com/atterpac/gnat/internal/nats"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const maxMessages = 500

// MessageMonitor shows live messages for a subscribed subject using a master-detail layout.
type MessageMonitor struct {
	*components.MasterDetailView
	app *App

	// UI components
	table        *components.Table
	preview      *tview.TextView
	subjectInput *tview.InputField
	statusText   *tview.TextView
	modeText     *tview.TextView
	topBar       *tview.Flex

	// State
	mu            sync.Mutex
	subscription  nats.Subscription
	subject       string
	messages      []nats.LiveMessage
	filtered      []nats.LiveMessage
	filterText    string
	selectedIdx   int
	paused        bool
	useJetStream  bool
	deliverPolicy nats.DeliverPolicy

	// Initial load batching
	initialLoad    bool          // true during initial message replay
	batchTimer     *time.Timer   // timer to detect end of initial burst
	batchRendered  bool          // true after initial batch has been rendered

	// Flags
	pendingUpdate int32
	stopped       int32

	// Message channel for async processing
	msgChan chan nats.LiveMessage
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
		selectedIdx:   -1,
	}

	// Create the message table
	mm.table = components.NewTable().
		SetHeaders("TIME", "SUBJECT", "SIZE")

	mm.table.SetSelectionChangedFunc(func(row, col int) {
		mm.updatePreview(row - 1) // Adjust for header row
	})

	// Create the preview panel
	mm.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	mm.preview.SetBackgroundColor(theme.Bg())
	theme.Register(mm.preview)

	// Create subject input
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
			go mm.subscribe(subj)
		} else if key == tcell.KeyEscape {
			mm.app.app.SetFocus(mm.MasterDetailView)
		}
	})

	if subject != "" {
		mm.subjectInput.SetText(subject)
		mm.subject = subject
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

	// Wrap table with top bar
	masterContent := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(mm.topBar, 1, 0, false).
		AddItem(mm.table, 0, 1, true)
	masterContent.SetBackgroundColor(theme.Bg())
	theme.Register(masterContent)

	// Create master-detail view
	mm.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Messages").
		SetDetailTitle("Message Detail").
		SetMasterContent(masterContent).
		SetDetailContent(mm.preview).
		SetRatio(0.5).
		ConfigureEmpty("󰍡", "No Messages", "Enter a subject and press Enter to subscribe")

	// Enable search with the app's status bar
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
		mm.applyFilterAndRefresh(query)
	})

	mm.MasterDetailView.SetOnSearchSubmit(func(query string) {
		mm.applyFilterAndRefresh(query)
	})

	mm.MasterDetailView.SetOnSearchCancel(func() {
		// Keep current filter on cancel
	})

	mm.updateModeText()

	return mm
}

func (mm *MessageMonitor) Name() string { return "Monitor" }

func (mm *MessageMonitor) Start() {
	if mm.subject != "" {
		go func() {
			time.Sleep(100 * time.Millisecond)
			if atomic.LoadInt32(&mm.stopped) == 0 {
				mm.subscribe(mm.subject)
			}
		}()
	}
}

func (mm *MessageMonitor) Stop() {
	atomic.StoreInt32(&mm.stopped, 1)
	mm.unsubscribe()
}

func (mm *MessageMonitor) Hints() []components.KeyHint {
	mm.mu.Lock()
	hasSub := mm.subscription != nil
	paused := mm.paused
	useJS := mm.useJetStream
	mm.mu.Unlock()

	hints := []components.KeyHint{
		{Key: "Enter", Description: "View"},
		{Key: "/", Description: "Filter"},
		{Key: "Tab", Description: "Switch pane"},
	}

	if hasSub {
		hints = append(hints, components.KeyHint{Key: "c", Description: "Clear"})
		if paused {
			hints = append(hints, components.KeyHint{Key: "p", Description: "Resume"})
		} else {
			hints = append(hints, components.KeyHint{Key: "p", Description: "Pause"})
		}
		hints = append(hints, components.KeyHint{Key: "u", Description: "Unsubscribe"})
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

func (mm *MessageMonitor) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return mm.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		// Global hotkeys
		if event.Key() == tcell.KeyCtrlM {
			mm.toggleMode()
			return
		}
		if event.Key() == tcell.KeyCtrlD {
			mm.cycleDeliverPolicy()
			return
		}

		// Handle subject input focus
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

		// Handle Escape - clear filter or go back
		if event.Key() == tcell.KeyEscape {
			if mm.GetSearchText() != "" {
				mm.ClearSearch()
				mm.applyFilterAndRefresh("")
				return
			}
			// Let app handle (go back)
			return
		}

		// Handle search
		if mm.HandleSearchKey(event) {
			return
		}

		switch event.Rune() {
		case '/':
			mm.mu.Lock()
			hasSub := mm.subscription != nil
			mm.mu.Unlock()
			if !hasSub {
				setFocus(mm.subjectInput)
				return
			}
			mm.ShowSearch()
			return
		case 'c':
			mm.clearMessages()
			return
		case 'p':
			mm.togglePause()
			return
		case 'u':
			mm.unsubscribe()
			return
		case 'm':
			mm.toggleMode()
			return
		case 'd':
			mm.cycleDeliverPolicy()
			return
		}

		// Delegate unhandled keys to master-detail view
		if handler := mm.MasterDetailView.InputHandler(); handler != nil {
			handler(event, setFocus)
		}
	})
}

func (mm *MessageMonitor) subscribe(subject string) {
	if subject == "" {
		return
	}

	if atomic.LoadInt32(&mm.stopped) == 1 {
		return
	}

	mm.unsubscribe()

	provider := mm.app.Provider()
	if provider == nil {
		return
	}

	mm.mu.Lock()
	mm.subject = subject
	mm.messages = nil
	mm.filtered = nil
	mm.filterText = ""
	mm.selectedIdx = -1
	mm.paused = false
	mm.initialLoad = true
	mm.batchRendered = false
	if mm.batchTimer != nil {
		mm.batchTimer.Stop()
	}
	mm.batchTimer = nil
	// Create new message channel for this subscription
	mm.msgChan = make(chan nats.LiveMessage, 1000)
	useJS := mm.useJetStream
	policy := mm.deliverPolicy
	msgChan := mm.msgChan // Capture for goroutine
	mm.mu.Unlock()

	mm.app.QueueUpdateDraw(func() {
		mm.SetMasterTitle("Messages - Subscribing...")
	})

	// renderBatch renders all buffered messages and switches to live mode
	renderBatch := func() {
		// Check if stopped before acquiring lock
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}

		mm.mu.Lock()
		// Check if already rendered (timer race condition)
		if mm.batchRendered {
			mm.mu.Unlock()
			return
		}
		mm.initialLoad = false
		mm.batchRendered = true
		mm.batchTimer = nil
		mm.mu.Unlock()

		mm.app.QueueUpdateDraw(func() {
			if atomic.LoadInt32(&mm.stopped) == 1 {
				return
			}
			mm.populateTable()
			mm.updateStatus()
		})
	}

	// processMessage handles a single message (called from message processor goroutine)
	processMessage := func(msg nats.LiveMessage) {
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}
		mm.mu.Lock()
		if mm.paused {
			mm.mu.Unlock()
			return
		}
		mm.messages = append(mm.messages, msg)
		if len(mm.messages) > maxMessages {
			mm.messages = mm.messages[len(mm.messages)-maxMessages:]
		}
		mm.applyFilter()

		isInitialLoad := mm.initialLoad
		msgCount := len(mm.messages)

		// During initial load, reset the batch timer on each message
		if isInitialLoad {
			if mm.batchTimer != nil {
				mm.batchTimer.Stop()
			}
			// If we hit maxMessages during initial load, render immediately
			if msgCount >= maxMessages {
				mm.batchTimer = nil
				mm.mu.Unlock()
				renderBatch()
				return
			}
			// Reset timer - render batch after 200ms of no new messages
			mm.batchTimer = time.AfterFunc(200*time.Millisecond, renderBatch)
			mm.mu.Unlock()
			return
		}
		mm.mu.Unlock()

		// Live mode - incremental updates with throttling
		if atomic.LoadInt32(&mm.stopped) == 0 && atomic.CompareAndSwapInt32(&mm.pendingUpdate, 0, 1) {
			mm.app.QueueUpdateDraw(func() {
				atomic.StoreInt32(&mm.pendingUpdate, 0)
				if atomic.LoadInt32(&mm.stopped) == 1 {
					return
				}
				mm.populateTable()
				mm.updateStatus()
			})
		}
	}

	// Start message processor goroutine
	go func() {
		for msg := range msgChan {
			if atomic.LoadInt32(&mm.stopped) == 1 {
				return
			}
			processMessage(msg)
		}
	}()

	// Handler just sends to channel - never blocks
	handler := func(msg nats.LiveMessage) {
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}
		select {
		case msgChan <- msg:
		default:
			// Channel full, drop message (shouldn't happen with 1000 buffer)
		}
	}

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
		mm.app.QueueUpdateDraw(func() {
			mm.SetMasterTitle("Messages")
			mm.ConfigureEmpty("󰅜", "Subscription Failed", err.Error())
			mm.app.ShowError(fmt.Sprintf("Subscribe failed: %v", err))
		})
		return
	}

	mm.mu.Lock()
	mm.subscription = sub
	mm.mu.Unlock()

	toastMsg := fmt.Sprintf("Subscribed to %s (%s)", subject, modeLabel)
	mm.app.QueueUpdateDraw(func() {
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}
		mm.SetMasterTitle(fmt.Sprintf("Messages - %s", subject))
		mm.ConfigureEmpty("󰍡", "Waiting for Messages", fmt.Sprintf("Subscribed to %s", subject))
		mm.statusText.SetText(fmt.Sprintf("[green]Live[-] [%s]0 msgs[-]", theme.TagFgDim()))
	})
	// Show toast in a separate QueueUpdateDraw to avoid deadlock with jig components
	mm.app.QueueUpdateDraw(func() {
		if atomic.LoadInt32(&mm.stopped) == 1 {
			return
		}
		mm.app.ShowSuccess(toastMsg)
	})
}

func (mm *MessageMonitor) unsubscribe() {
	mm.mu.Lock()
	sub := mm.subscription
	subject := mm.subject
	mm.subscription = nil
	if mm.batchTimer != nil {
		mm.batchTimer.Stop()
		mm.batchTimer = nil
	}
	mm.initialLoad = false
	// Close the message channel to stop the processor goroutine
	msgChan := mm.msgChan
	mm.msgChan = nil
	mm.mu.Unlock()

	// Close channel outside lock to avoid blocking
	if msgChan != nil {
		close(msgChan)
	}

	if sub != nil {
		go func() {
			_ = sub.Unsubscribe()
		}()

		if atomic.LoadInt32(&mm.stopped) == 0 {
			mm.app.QueueUpdateDraw(func() {
				mm.SetMasterTitle("Messages")
				mm.ConfigureEmpty("󰍡", "No Messages", "Enter a subject and press Enter to subscribe")
				mm.table.ClearRows()
				mm.preview.SetText("")
				mm.updateStatus()
				mm.app.ShowInfo(fmt.Sprintf("Unsubscribed from %s", subject))
			})
		}
	}
}

func (mm *MessageMonitor) applyFilter() {
	// Must be called with lock held
	if mm.filterText == "" {
		mm.filtered = mm.messages
		return
	}

	lower := strings.ToLower(mm.filterText)
	mm.filtered = nil
	for _, msg := range mm.messages {
		if strings.Contains(strings.ToLower(msg.Subject), lower) ||
			strings.Contains(strings.ToLower(string(msg.Data)), lower) {
			mm.filtered = append(mm.filtered, msg)
		}
	}
}

func (mm *MessageMonitor) applyFilterAndRefresh(query string) {
	mm.mu.Lock()
	mm.filterText = query
	mm.applyFilter()
	mm.mu.Unlock()

	mm.app.QueueUpdateDraw(func() {
		mm.populateTable()
		mm.updateStatus()
	})
}

func (mm *MessageMonitor) populateTable() {
	mm.mu.Lock()
	msgs := make([]nats.LiveMessage, len(mm.filtered))
	copy(msgs, mm.filtered)
	mm.mu.Unlock()

	mm.table.ClearRows()

	if len(msgs) == 0 {
		mm.SetDetailContent(nil) // Show empty state
		return
	}

	// Show newest first
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		ts := msg.Timestamp.Format("15:04:05.000")

		mm.table.AddRow(
			ts,
			msg.Subject,
			formatBytes(uint64(len(msg.Data))),
		)
	}

	if len(msgs) > 0 {
		mm.table.SelectRow(0)
		mm.updatePreview(0)
	}
}

func (mm *MessageMonitor) updatePreview(idx int) {
	mm.mu.Lock()
	// Index is in display order (newest first), convert to slice order
	if idx < 0 || idx >= len(mm.filtered) {
		mm.mu.Unlock()
		mm.preview.SetText("")
		return
	}
	// Reverse index since we display newest first
	actualIdx := len(mm.filtered) - 1 - idx
	if actualIdx < 0 || actualIdx >= len(mm.filtered) {
		mm.mu.Unlock()
		mm.preview.SetText("")
		return
	}
	msg := mm.filtered[actualIdx]
	mm.selectedIdx = actualIdx
	mm.mu.Unlock()

	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder

	// Header info
	fmt.Fprintf(&b, "[%s]Subject:[-]   [%s]%s[-]\n", dim, accent, msg.Subject)
	fmt.Fprintf(&b, "[%s]Time:[-]      %s\n", dim, msg.Timestamp.Format("2006-01-02 15:04:05.000"))
	fmt.Fprintf(&b, "[%s]Size:[-]      %s\n", dim, formatBytes(uint64(len(msg.Data))))

	if msg.Stream != "" {
		fmt.Fprintf(&b, "[%s]Stream:[-]    %s\n", dim, msg.Stream)
	}
	if msg.Sequence > 0 {
		fmt.Fprintf(&b, "[%s]Sequence:[-]  %d\n", dim, msg.Sequence)
	}

	// Headers
	if len(msg.Headers) > 0 {
		fmt.Fprintf(&b, "\n[%s]Headers:[-]\n", dim)
		for k, v := range msg.Headers {
			fmt.Fprintf(&b, "  [%s]%s:[-] %s\n", dim, k, strings.Join(v, ", "))
		}
	}

	// Payload
	fmt.Fprintf(&b, "\n[%s]Payload:[-]\n", dim)
	data := string(msg.Data)

	// Try to pretty print if valid JSON
	if json.Valid(msg.Data) {
		var prettyJSON bytes.Buffer
		if err := json.Indent(&prettyJSON, msg.Data, "", "  "); err == nil {
			data = prettyJSON.String()
		}
	}

	// Escape color tags
	data = strings.ReplaceAll(data, "[", "[[")
	b.WriteString(data)

	mm.preview.SetText(b.String())
	mm.preview.ScrollToBeginning()
	mm.SetDetailContent(mm.preview)
}

func (mm *MessageMonitor) updateStatus() {
	mm.mu.Lock()
	hasSub := mm.subscription != nil
	paused := mm.paused
	totalCount := len(mm.messages)
	filteredCount := len(mm.filtered)
	filterText := mm.filterText
	mm.mu.Unlock()

	dim := theme.TagFgDim()

	var status string
	if !hasSub {
		status = fmt.Sprintf("[%s]Not subscribed[-]", dim)
	} else if paused {
		status = fmt.Sprintf("[yellow]Paused[-] [%s]%d msgs[-]", dim, totalCount)
	} else if filterText != "" {
		status = fmt.Sprintf("[green]Live[-] [%s]%d/%d[-]", dim, filteredCount, totalCount)
	} else {
		status = fmt.Sprintf("[green]Live[-] [%s]%d msgs[-]", dim, totalCount)
	}

	mm.statusText.SetText(status)
}

func (mm *MessageMonitor) updateModeText() {
	mm.mu.Lock()
	useJS := mm.useJetStream
	policy := mm.deliverPolicy
	mm.mu.Unlock()

	accent := theme.TagAccent()
	dim := theme.TagFgDim()

	var modeStr, policyStr string
	if useJS {
		modeStr = fmt.Sprintf("[%s]JS[-]", accent)
		switch policy {
		case nats.DeliverAll:
			policyStr = "All"
		case nats.DeliverLast:
			policyStr = "Last"
		case nats.DeliverNew:
			policyStr = "New"
		case nats.DeliverLastPerSubject:
			policyStr = "LastPerSubj"
		}
		mm.modeText.SetText(fmt.Sprintf("%s [%s]%s[-]", modeStr, dim, policyStr))
	} else {
		modeStr = fmt.Sprintf("[%s]NATS[-]", dim)
		mm.modeText.SetText(modeStr)
	}
}

func (mm *MessageMonitor) togglePause() {
	mm.mu.Lock()
	mm.paused = !mm.paused
	paused := mm.paused
	mm.mu.Unlock()

	mm.app.QueueUpdateDraw(func() {
		mm.updateStatus()
		if paused {
			mm.app.ShowInfo("Paused - new messages will be queued")
		} else {
			mm.app.ShowInfo("Resumed")
		}
	})
}

func (mm *MessageMonitor) clearMessages() {
	mm.mu.Lock()
	mm.messages = nil
	mm.filtered = nil
	mm.selectedIdx = -1
	mm.mu.Unlock()

	mm.app.QueueUpdateDraw(func() {
		mm.table.ClearRows()
		mm.preview.SetText("")
		mm.SetDetailContent(nil)
		mm.updateStatus()
	})
}

func (mm *MessageMonitor) toggleMode() {
	mm.mu.Lock()
	hasSub := mm.subscription != nil
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

	mm.app.QueueUpdateDraw(func() {
		mm.updateModeText()
		if useJS {
			mm.app.ShowInfo("JetStream mode - can replay historical messages")
		} else {
			mm.app.ShowInfo("Core NATS mode - new messages only")
		}
	})
}

func (mm *MessageMonitor) cycleDeliverPolicy() {
	mm.mu.Lock()
	hasSub := mm.subscription != nil
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
	policy := mm.deliverPolicy
	mm.mu.Unlock()

	var policyName string
	switch policy {
	case nats.DeliverAll:
		policyName = "All - replay all historical messages"
	case nats.DeliverLast:
		policyName = "Last - start with last message only"
	case nats.DeliverNew:
		policyName = "New - new messages only (no replay)"
	case nats.DeliverLastPerSubject:
		policyName = "LastPerSubject - last message per subject"
	}

	mm.app.QueueUpdateDraw(func() {
		mm.updateModeText()
		mm.app.ShowInfo(fmt.Sprintf("Deliver: %s", policyName))
	})
}
