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
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const playgroundMaxMessages = 500

type playgroundState struct {
	messages []nats.LiveMessage
	count    int
	subject  string
	active   bool
	mode     string
	pubCount int
}

// Playground provides a split publisher + subscriber view for interactive testing.
type Playground struct {
	*components.Split
	app *App

	// Publisher pane
	pubSubject *tview.InputField
	pubPayload *tview.TextArea
	pubHeaderK *tview.InputField
	pubHeaderV *tview.InputField

	// Subscriber pane
	subTable   *components.Table
	subPreview *tview.TextView
	subDetail  *components.MasterDetailView

	// State
	state *binding.Value[playgroundState]

	mu      sync.Mutex
	sub     nats.Subscription
	msgChan chan nats.LiveMessage
	gen     int64
	useJS   bool

	stopProcess chan struct{}
	stopped     int32

	// Focus cycling
	focusItems []tview.Primitive
	focusIdx   int
}

func NewPlayground(app *App) *Playground {
	pg := &Playground{
		app:         app,
		useJS:       true,
		stopProcess: make(chan struct{}),
	}

	// Publisher inputs
	pg.pubSubject = tview.NewInputField().
		SetLabel("Subject: ").
		SetFieldWidth(0).
		SetPlaceholder("orders.new")
	pg.pubSubject.SetBackgroundColor(theme.Bg())
	pg.pubSubject.SetFieldBackgroundColor(theme.Bg())
	theme.Register(pg.pubSubject)

	pg.pubPayload = tview.NewTextArea().
		SetPlaceholder("message payload...")
	pg.pubPayload.SetBackgroundColor(theme.Bg())
	theme.Register(pg.pubPayload)

	pg.pubHeaderK = tview.NewInputField().
		SetLabel("Header: ").
		SetFieldWidth(0).
		SetPlaceholder("key")
	pg.pubHeaderK.SetBackgroundColor(theme.Bg())
	pg.pubHeaderK.SetFieldBackgroundColor(theme.Bg())
	theme.Register(pg.pubHeaderK)

	pg.pubHeaderV = tview.NewInputField().
		SetLabel("= ").
		SetFieldWidth(0).
		SetPlaceholder("value")
	pg.pubHeaderV.SetBackgroundColor(theme.Bg())
	pg.pubHeaderV.SetFieldBackgroundColor(theme.Bg())
	theme.Register(pg.pubHeaderV)

	headerRow := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(pg.pubHeaderK, 0, 1, false).
		AddItem(pg.pubHeaderV, 0, 1, false)
	headerRow.SetBackgroundColor(theme.Bg())

	pubFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(pg.pubSubject, 1, 0, true).
		AddItem(pg.pubPayload, 0, 1, false).
		AddItem(headerRow, 1, 0, false)
	pubFlex.SetBackgroundColor(theme.Bg())
	theme.Register(pubFlex)
	pubPanel := components.NewPanel().SetTitle("Publisher").SetContent(pubFlex)

	// Subscriber
	pg.subTable = components.NewTable().
		SetHeaders("TIME", "SUBJECT", "SIZE").
		ConfigureEmpty(theme.IconSignal, "No Messages", "Publish to start")

	pg.subPreview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	pg.subPreview.SetBackgroundColor(theme.Bg())
	theme.Register(pg.subPreview)

	pg.subDetail = components.NewMasterDetailView().
		SetMasterTitle("Subscriber").
		SetDetailTitle("Preview").
		SetMasterContent(pg.subTable).
		SetDetailContent(pg.subPreview).
		SetRatio(0.5)

	pg.subTable.SetSelectionChangedFunc(func(row, col int) {
		pg.renderSubPreview(row - 1)
	})

	subPanel := components.NewPanel().SetTitle("Subscriber").SetContent(pg.subDetail)

	pg.Split = components.NewSplit().
		SetDirection(components.SplitHorizontal).
		SetRatio(0.35).
		SetLeft(pubPanel).
		SetRight(subPanel)

	pg.focusItems = []tview.Primitive{
		pg.pubSubject, pg.pubPayload, pg.pubHeaderK, pg.pubHeaderV, pg.subTable,
	}

	pg.state = binding.NewValue(playgroundState{mode: "JS"})
	pg.state.BindToWithDraw(func(s playgroundState) {
		pg.renderSubState(s)
	})

	pg.pubSubject.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			go pg.publish()
		}
	})

	return pg
}

func (pg *Playground) Name() string { return "Playground" }

func (pg *Playground) isTextInputFocused() bool {
	return pg.pubSubject.HasFocus() || pg.pubPayload.HasFocus() ||
		pg.pubHeaderK.HasFocus() || pg.pubHeaderV.HasFocus()
}

func (pg *Playground) Start() {
	atomic.StoreInt32(&pg.stopped, 0)
}

func (pg *Playground) Stop() {
	atomic.StoreInt32(&pg.stopped, 1)
	select {
	case pg.stopProcess <- struct{}{}:
	default:
	}
	pg.doUnsubscribe()
}

func (pg *Playground) Hints() []components.KeyHint {
	pg.mu.Lock()
	hasSub := pg.sub != nil
	pg.mu.Unlock()

	hints := []components.KeyHint{
		{Key: "Ctrl+S", Description: "Publish"},
		{Key: "Tab", Description: "Next field"},
		{Key: "m", Description: "Toggle JS/NATS"},
	}
	if hasSub {
		hints = append(hints, components.KeyHint{Key: "u", Description: "Unsubscribe"})
		hints = append(hints, components.KeyHint{Key: "c", Description: "Clear"})
	}
	hints = append(hints, components.KeyHint{Key: "Esc", Description: "Back"})
	return hints
}

func (pg *Playground) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return pg.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Key() == tcell.KeyCtrlS:
			go pg.publish()
			return
		case event.Key() == tcell.KeyTab:
			pg.focusIdx = (pg.focusIdx + 1) % len(pg.focusItems)
			pg.app.app.SetFocus(pg.focusItems[pg.focusIdx])
			return
		case event.Key() == tcell.KeyBacktab:
			pg.focusIdx--
			if pg.focusIdx < 0 {
				pg.focusIdx = len(pg.focusItems) - 1
			}
			pg.app.app.SetFocus(pg.focusItems[pg.focusIdx])
			return
		case event.Rune() == 'u' && !pg.isTextInputFocused():
			go pg.unsubscribe()
			return
		case event.Rune() == 'c' && !pg.isTextInputFocused():
			go pg.clearMessages()
			return
		case event.Rune() == 'm' && !pg.isTextInputFocused():
			go pg.toggleMode()
			return
		}

		// Delegate to focused primitive
		focused := pg.focusItems[pg.focusIdx]
		if handler := focused.InputHandler(); handler != nil {
			handler(event, setFocus)
		}
	})
}

func (pg *Playground) publish() {
	subject := pg.pubSubject.GetText()
	if subject == "" {
		pg.app.ShowWarning("Subject is required")
		return
	}

	payload := pg.pubPayload.GetText()
	headers := make(map[string][]string)
	hk := pg.pubHeaderK.GetText()
	hv := pg.pubHeaderV.GetText()
	if hk != "" {
		headers[hk] = []string{hv}
	}

	// Auto-subscribe if not already subscribed
	pg.mu.Lock()
	hasSub := pg.sub != nil
	pg.mu.Unlock()
	if !hasSub {
		pg.subscribe(subject)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pg.app.Provider().Publish(ctx, subject, []byte(payload), headers); err != nil {
		pg.app.ShowError(err.Error())
		return
	}

	s := pg.state.Get()
	s.pubCount++
	pg.state.SetAndDraw(s)
	pg.app.QueueUpdateDraw(func() {
		pg.app.ShowSuccess(fmt.Sprintf("Published to %s (#%d)", subject, s.pubCount))
	})
}

func (pg *Playground) subscribe(subject string) {
	if subject == "" || atomic.LoadInt32(&pg.stopped) == 1 {
		return
	}

	pg.doUnsubscribe()

	provider := pg.app.Provider()
	if provider == nil {
		return
	}

	msgChan := make(chan nats.LiveMessage, 1000)

	pg.mu.Lock()
	pg.gen++
	myGen := pg.gen
	pg.msgChan = msgChan
	useJS := pg.useJS
	pg.mu.Unlock()

	go pg.processMessages(myGen, msgChan, subject)

	handler := func(msg nats.LiveMessage) {
		if atomic.LoadInt32(&pg.stopped) == 1 {
			return
		}
		defer func() { recover() }() //nolint:errcheck
		select {
		case msgChan <- msg:
		default:
		}
	}

	var sub nats.Subscription
	var err error
	var modeLabel string

	if useJS {
		sub, err = provider.SubscribeJetStream(context.Background(), subject, nats.DeliverNew, handler)
		modeLabel = "JetStream"
	} else {
		sub, err = provider.Subscribe(context.Background(), subject, handler)
		modeLabel = "Core NATS"
	}

	if err != nil && useJS {
		sub, err = provider.Subscribe(context.Background(), subject, handler)
		if err == nil {
			modeLabel = "Core NATS (fallback)"
		}
	}

	if err != nil {
		pg.app.QueueUpdateDraw(func() {
			pg.app.ShowError(fmt.Sprintf("Subscribe failed: %v", err))
		})
		return
	}

	pg.mu.Lock()
	if pg.gen != myGen {
		pg.mu.Unlock()
		_ = sub.Unsubscribe()
		return
	}
	pg.sub = sub
	pg.mu.Unlock()

	pg.app.QueueUpdateDraw(func() {
		pg.app.ShowSuccess(fmt.Sprintf("Subscribed to %s (%s)", subject, modeLabel))
	})
}

func (pg *Playground) processMessages(myGen int64, msgChan chan nats.LiveMessage, subject string) {
	var messages []nats.LiveMessage
	batchDone := false

	batchTimer := time.NewTimer(500 * time.Millisecond)
	defer batchTimer.Stop()

	renderTicker := time.NewTicker(200 * time.Millisecond)
	defer renderTicker.Stop()

	dirty := false

	pushState := func() {
		if atomic.LoadInt32(&pg.stopped) == 1 {
			return
		}
		pg.mu.Lock()
		if pg.gen != myGen {
			pg.mu.Unlock()
			return
		}
		useJS := pg.useJS
		pg.mu.Unlock()

		mode := "NATS"
		if useJS {
			mode = "JS"
		}

		pg.state.SetAndDraw(playgroundState{
			messages: messages,
			count:    len(messages),
			subject:  subject,
			active:   true,
			mode:     mode,
			pubCount: pg.state.Get().pubCount,
		})
		dirty = false
	}

	for {
		select {
		case <-pg.stopProcess:
			return
		case msg, ok := <-msgChan:
			if !ok {
				return
			}
			messages = append(messages, msg)
			if len(messages) > playgroundMaxMessages {
				messages = messages[len(messages)-playgroundMaxMessages:]
			}
			dirty = true
			if !batchDone {
				batchTimer.Reset(500 * time.Millisecond)
				if len(messages) >= playgroundMaxMessages {
					batchDone = true
					pushState()
				}
			}
		case <-batchTimer.C:
			batchDone = true
			if dirty {
				pushState()
			}
		case <-renderTicker.C:
			if batchDone && dirty {
				pushState()
			}
		}
	}
}

func (pg *Playground) doUnsubscribe() {
	pg.mu.Lock()
	sub := pg.sub
	ch := pg.msgChan
	pg.sub = nil
	pg.msgChan = nil
	pg.gen++
	pg.mu.Unlock()

	select {
	case pg.stopProcess <- struct{}{}:
	default:
	}
	pg.stopProcess = make(chan struct{})

	if sub != nil {
		_ = sub.Unsubscribe()
	}
	if ch != nil {
		close(ch)
	}
}

func (pg *Playground) unsubscribe() {
	pg.doUnsubscribe()
	s := pg.state.Get()
	s.active = false
	pg.state.SetAndDraw(s)
	pg.app.QueueUpdateDraw(func() {
		pg.app.ShowInfo("Unsubscribed")
	})
}

func (pg *Playground) clearMessages() {
	s := pg.state.Get()
	s.messages = nil
	s.count = 0
	pg.state.SetAndDraw(s)
}

func (pg *Playground) toggleMode() {
	pg.mu.Lock()
	hasSub := pg.sub != nil
	pg.mu.Unlock()

	if hasSub {
		pg.app.QueueUpdateDraw(func() {
			pg.app.ShowWarning("Unsubscribe first to change mode (press 'u')")
		})
		return
	}

	pg.mu.Lock()
	pg.useJS = !pg.useJS
	useJS := pg.useJS
	pg.mu.Unlock()

	s := pg.state.Get()
	if useJS {
		s.mode = "JS"
	} else {
		s.mode = "NATS"
	}
	pg.state.SetAndDraw(s)

	pg.app.QueueUpdateDraw(func() {
		if useJS {
			pg.app.ShowInfo("JetStream mode")
		} else {
			pg.app.ShowInfo("Core NATS mode")
		}
	})
}

func (pg *Playground) renderSubState(s playgroundState) {
	title := "Subscriber"
	if s.active {
		title = fmt.Sprintf("Subscriber - %s [%s] %d msgs", s.subject, s.mode, s.count)
	}
	pg.subDetail.SetMasterTitle(title)

	pg.subTable.ClearRows()
	if len(s.messages) == 0 {
		pg.subTable.ConfigureEmpty(theme.IconSignal, "No Messages", "Publish to start")
		return
	}

	for i := len(s.messages) - 1; i >= 0; i-- {
		msg := s.messages[i]
		pg.subTable.AddRow(
			msg.Timestamp.Format("15:04:05.000"),
			msg.Subject,
			formatBytes(uint64(len(msg.Data))),
		)
	}

	pg.subTable.SelectRow(0)
	pg.renderSubPreview(0)
}

func (pg *Playground) renderSubPreview(displayIdx int) {
	s := pg.state.Get()
	if displayIdx < 0 || displayIdx >= len(s.messages) {
		pg.subPreview.SetText("")
		return
	}

	actualIdx := len(s.messages) - 1 - displayIdx
	if actualIdx < 0 || actualIdx >= len(s.messages) {
		pg.subPreview.SetText("")
		return
	}
	msg := s.messages[actualIdx]

	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Subject:[-] [%s]%s[-]\n", dim, accent, msg.Subject)
	fmt.Fprintf(&b, "[%s]Time:[-]    %s\n", dim, msg.Timestamp.Format("15:04:05.000"))
	fmt.Fprintf(&b, "[%s]Size:[-]    %s\n", dim, formatBytes(uint64(len(msg.Data))))

	if len(msg.Headers) > 0 {
		fmt.Fprintf(&b, "\n[%s]Headers:[-]\n", dim)
		for k, v := range msg.Headers {
			fmt.Fprintf(&b, "  [%s]%s:[-] %s\n", dim, k, strings.Join(v, ", "))
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

	pg.subPreview.SetText(b.String())
	pg.subPreview.ScrollToBeginning()
}
