package view

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type consumerLagEntry struct {
	StreamName   string
	ConsumerName string
	NumPending   uint64
	NumAckPending int
	TotalLag     uint64
	LagHistory   []float64
	ProcRate     float64
	LastDelivered uint64
	LastSample   time.Time
	Redelivered  int
}

type consumerLagState struct {
	entries []consumerLagEntry
	err     string
}

// ConsumerLag shows aggregated consumer lag across all streams.
type ConsumerLag struct {
	*components.MasterDetailView
	app *App

	table   *components.Table
	preview *tview.TextView

	state       *binding.Value[consumerLagState]
	stopRefresh chan struct{}
	stopped     int32

	mu       sync.Mutex
	entryMap map[string]*consumerLagEntry // keyed by "stream/consumer"
}

func NewConsumerLag(app *App) *ConsumerLag {
	cl := &ConsumerLag{
		app:         app,
		stopRefresh: make(chan struct{}, 1),
		entryMap:    make(map[string]*consumerLagEntry),
	}

	cl.table = components.NewTable().
		SetHeaders("STREAM", "CONSUMER", "LAG", "ACK_PEND", "RATE", "REDELIV").
		ConfigureEmpty(theme.IconSignal, "Loading...", "Fetching consumer data")

	cl.table.SetSelectionChangedFunc(func(row, col int) {
		cl.renderPreview(row - 1)
	})

	cl.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	cl.preview.SetBackgroundColor(theme.Bg())
	theme.Register(cl.preview)

	cl.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Consumer Lag").
		SetDetailTitle("Details").
		SetMasterContent(cl.table).
		SetDetailContent(cl.preview).
		SetRatio(0.6)

	cl.state = binding.NewValue(consumerLagState{})
	cl.state.BindToWithDraw(func(s consumerLagState) {
		cl.renderState(s)
	})

	return cl
}

func (cl *ConsumerLag) Name() string { return "Consumer Lag" }

func (cl *ConsumerLag) Start() {
	atomic.StoreInt32(&cl.stopped, 0)
	cl.stopRefresh = make(chan struct{}, 1)
	go func() {
		cl.refreshAll()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-cl.stopRefresh:
				return
			case <-ticker.C:
				cl.refreshAll()
			}
		}
	}()
}

func (cl *ConsumerLag) Stop() {
	atomic.StoreInt32(&cl.stopped, 1)
	select {
	case cl.stopRefresh <- struct{}{}:
	default:
	}
}

func (cl *ConsumerLag) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "Consumer detail"},
		{Key: "v", Description: "Stream detail"},
		{Key: "/", Description: "Filter"},
		{Key: "p", Description: "Toggle preview"},
		{Key: "r", Description: "Refresh"},
		{Key: "Esc", Description: "Back"},
	}
}

func (cl *ConsumerLag) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return cl.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch event.Rune() {
		case 'r':
			go cl.refreshAll()
			return
		case 'p':
			cl.ToggleDetail()
			return
		}

		if event.Key() == tcell.KeyEnter {
			if entry, ok := cl.getSelectedEntry(); ok {
				cl.app.NavigateToConsumerDetail(entry.StreamName, entry.ConsumerName)
			}
			return
		}
		if event.Rune() == 'v' {
			if entry, ok := cl.getSelectedEntry(); ok {
				cl.app.NavigateToStreamDetail(entry.StreamName)
			}
			return
		}

		if handler := cl.MasterDetailView.InputHandler(); handler != nil {
			handler(event, setFocus)
		}
	})
}

func (cl *ConsumerLag) refreshAll() {
	if atomic.LoadInt32(&cl.stopped) == 1 {
		return
	}

	provider := cl.app.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	streams, err := provider.ListStreams(ctx)
	if err != nil {
		cl.state.SetAndDraw(consumerLagState{err: err.Error()})
		return
	}

	cl.mu.Lock()
	now := time.Now()
	seen := make(map[string]bool)

	for _, stream := range streams {
		if atomic.LoadInt32(&cl.stopped) == 1 {
			cl.mu.Unlock()
			return
		}

		consumers, err := provider.ListConsumers(ctx, stream.Config.Name)
		if err != nil {
			continue
		}

		for _, consumer := range consumers {
			key := stream.Config.Name + "/" + consumer.Name
			seen[key] = true

			existing, exists := cl.entryMap[key]
			lag := consumer.NumPending + uint64(consumer.NumAckPending)

			if exists {
				// Update rates
				var rate float64
				if !existing.LastSample.IsZero() {
					elapsed := now.Sub(existing.LastSample).Seconds()
					if elapsed > 0 {
						rate = float64(consumer.Delivered.Consumer-existing.LastDelivered) / elapsed
					}
				}
				existing.NumPending = consumer.NumPending
				existing.NumAckPending = consumer.NumAckPending
				existing.TotalLag = lag
				existing.ProcRate = rate
				existing.LastDelivered = consumer.Delivered.Consumer
				existing.LastSample = now
				existing.Redelivered = consumer.NumRedelivered
				existing.LagHistory = append(existing.LagHistory, float64(lag))
				if len(existing.LagHistory) > 60 {
					existing.LagHistory = existing.LagHistory[len(existing.LagHistory)-60:]
				}
			} else {
				cl.entryMap[key] = &consumerLagEntry{
					StreamName:    stream.Config.Name,
					ConsumerName:  consumer.Name,
					NumPending:    consumer.NumPending,
					NumAckPending: consumer.NumAckPending,
					TotalLag:      lag,
					LagHistory:    []float64{float64(lag)},
					LastDelivered: consumer.Delivered.Consumer,
					LastSample:    now,
					Redelivered:   consumer.NumRedelivered,
				}
			}
		}
	}

	// Remove stale entries
	for key := range cl.entryMap {
		if !seen[key] {
			delete(cl.entryMap, key)
		}
	}

	// Build sorted slice
	entries := make([]consumerLagEntry, 0, len(cl.entryMap))
	for _, entry := range cl.entryMap {
		entries = append(entries, *entry)
	}
	cl.mu.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TotalLag > entries[j].TotalLag
	})

	cl.state.SetAndDraw(consumerLagState{entries: entries})
}

func (cl *ConsumerLag) renderState(s consumerLagState) {
	if s.err != "" {
		cl.table.ConfigureEmpty(theme.IconError, "Error", s.err)
		cl.table.ClearRows()
		return
	}

	if len(s.entries) == 0 {
		cl.table.ConfigureEmpty(theme.IconSignal, "No Consumers", "No consumers found across streams")
		cl.table.ClearRows()
		return
	}

	cl.SetMasterTitle(fmt.Sprintf("Consumer Lag (%d)", len(s.entries)))
	cl.table.ClearRows()

	for _, entry := range s.entries {
		lagStr := formatNumber(entry.TotalLag)
		ackStr := fmt.Sprintf("%d", entry.NumAckPending)
		rateStr := fmt.Sprintf("%.1f/s", entry.ProcRate)
		redelivStr := fmt.Sprintf("%d", entry.Redelivered)

		cl.table.AddRow(
			entry.StreamName,
			entry.ConsumerName,
			lagStr,
			ackStr,
			rateStr,
			redelivStr,
		)
	}

	cl.table.SelectRow(0)
	cl.renderPreview(0)
}

func (cl *ConsumerLag) renderPreview(idx int) {
	s := cl.state.Get()
	if idx < 0 || idx >= len(s.entries) {
		cl.preview.SetText("")
		return
	}

	entry := s.entries[idx]
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Stream:[-]      [%s]%s[-]\n", dim, accent, entry.StreamName)
	fmt.Fprintf(&b, "[%s]Consumer:[-]    [%s]%s[-]\n", dim, accent, entry.ConsumerName)
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "[%s]Total Lag:[-]   %s\n", dim, formatNumber(entry.TotalLag))
	fmt.Fprintf(&b, "[%s]Pending:[-]     %s\n", dim, formatNumber(entry.NumPending))
	fmt.Fprintf(&b, "[%s]Ack Pending:[-] %d\n", dim, entry.NumAckPending)
	fmt.Fprintf(&b, "[%s]Redelivered:[-] %d\n", dim, entry.Redelivered)
	fmt.Fprintf(&b, "[%s]Proc Rate:[-]   %.1f msg/s\n", dim, entry.ProcRate)

	if len(entry.LagHistory) > 1 {
		spark := miniSparkline(entry.LagHistory, 30)
		fmt.Fprintf(&b, "\n[%s]Lag History:[-]\n%s\n", dim, spark)
	}

	cl.preview.SetText(b.String())
	cl.preview.ScrollToBeginning()
}

func (cl *ConsumerLag) getSelectedEntry() (consumerLagEntry, bool) {
	s := cl.state.Get()
	row, _ := cl.table.GetSelection()
	idx := row - 1
	if idx < 0 || idx >= len(s.entries) {
		return consumerLagEntry{}, false
	}
	return s.entries[idx], true
}
