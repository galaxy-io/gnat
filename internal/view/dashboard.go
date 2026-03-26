package view

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sort"

	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/galaxy-io/gnat/internal/logger"
	"github.com/galaxy-io/gnat/internal/nats"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

const (
	maxAdvisories  = 100
	maxHistory     = 120 // 120 points × 2s interval = 4 minutes
	pollInterval   = 2 * time.Second
	streamInterval = 10 * time.Second
)

// rateHistory tracks rolling rate data for a single metric.
type rateHistory struct {
	values []float64
}

func (h *rateHistory) add(v float64) {
	h.values = append(h.values, v)
	if len(h.values) > maxHistory {
		h.values = h.values[len(h.values)-maxHistory:]
	}
}

func (h *rateHistory) last() float64 {
	if len(h.values) == 0 {
		return 0
	}
	return h.values[len(h.values)-1]
}

func (h *rateHistory) snapshot() []float64 {
	out := make([]float64, len(h.values))
	copy(out, h.values)
	return out
}

// metricsSnapshot holds a point-in-time snapshot of all dashboard data.
type metricsSnapshot struct {
	// Rates
	msgsInPerSec  float64
	msgsOutPerSec float64
	bytesInPerSec float64
	bytesOutPerSec float64

	// Rate histories (copies for safe binding)
	msgsInHistory  []float64
	msgsOutHistory []float64
	bytesInHistory  []float64
	bytesOutHistory []float64

	// RTT history
	rttHistory []float64

	// JetStream availability
	jsAvailable bool

	// Account info
	memoryUsed  uint64
	memoryLimit int64
	storeUsed   uint64
	storeLimit  int64
	streams     int
	maxStreams  int
	consumers   int
	maxConsumers int
	apiTotal    uint64
	apiErrors   uint64

	// Per-stream breakdown
	streamNames []string
	streamMsgs  []float64

	// Server info (fetched once per refresh, cheap call)
	server nats.ServerInfo
	domain string // JetStream domain from AccountInfo
}

// metricsCollector polls NATS stats and computes rates.
type metricsCollector struct {
	mu sync.Mutex

	prevStats nats.ConnectionStats
	prevTime  time.Time
	hasFirst  bool

	msgsIn  rateHistory
	msgsOut rateHistory
	bytesIn rateHistory
	bytesOut rateHistory
	rtt     rateHistory
}

func (mc *metricsCollector) recordStats(stats nats.ConnectionStats) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	now := time.Now()
	if mc.hasFirst {
		elapsed := now.Sub(mc.prevTime).Seconds()
		if elapsed > 0 {
			mc.msgsIn.add(float64(stats.InMsgs-mc.prevStats.InMsgs) / elapsed)
			mc.msgsOut.add(float64(stats.OutMsgs-mc.prevStats.OutMsgs) / elapsed)
			mc.bytesIn.add(float64(stats.InBytes-mc.prevStats.InBytes) / elapsed)
			mc.bytesOut.add(float64(stats.OutBytes-mc.prevStats.OutBytes) / elapsed)
		}
	}
	mc.prevStats = stats
	mc.prevTime = now
	mc.hasFirst = true
}

func (mc *metricsCollector) snapshot() (msgsIn, msgsOut, bytesIn, bytesOut rateHistory) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.msgsIn, mc.msgsOut, mc.bytesIn, mc.bytesOut
}

// Dashboard is the top-level overview of the NATS JetStream account.
type Dashboard struct {
	*tview.Flex
	app *App

	// Components - Row 1: Metric cards
	cardMsgsIn  *components.MetricCard
	cardMsgsOut *components.MetricCard
	cardBytesIn *components.MetricCard
	cardBytesOut *components.MetricCard

	// Components - Row 1: RTT card
	cardRTT *components.MetricCard

	// Components - Row 2 left: Throughput graph
	throughputGraph *components.LineGraph

	// Components - Row 2 right: Gauges + compact cards
	memoryGauge   *components.Gauge
	storeGauge    *components.Gauge
	cardStreams   *components.MetricCard
	cardConsumers *components.MetricCard
	cardAPI       *components.MetricCard

	// Components - Row 2 right: Server info
	serverText  *tview.TextView
	serverPanel *components.Panel

	// Components - Row 3 left: Bar chart
	streamChart *components.BarChart

	// Components - Row 3 right: Advisories
	advisoryPanel *components.Panel
	advisoryText  *tview.TextView

	// Reactive state
	metrics    *binding.Value[metricsSnapshot]
	advisories *binding.Value[[]nats.Advisory]

	// Advisory buffering — incoming advisories accumulate here and are
	// flushed to the binding on the poll interval, not per-message.
	advMu  sync.Mutex
	advBuf []nats.Advisory

	// Data collection
	collector *metricsCollector

	refreshCancel context.CancelFunc
	stopped       int32
}

// NewDashboard creates the dashboard view.
func NewDashboard(app *App) *Dashboard {
	d := &Dashboard{
		app:         app,
		refreshCancel: func() {},
		collector:   &metricsCollector{},
	}

	d.buildComponents()
	d.buildLayout()
	d.setupBindings()
	return d
}

func (d *Dashboard) buildComponents() {
	// Row 1: Metric cards
	d.cardMsgsIn = components.NewMetricCard().
		SetLabel("Msgs In/s").
		SetValue("0").
		SetShowSpark(true)

	d.cardMsgsOut = components.NewMetricCard().
		SetLabel("Msgs Out/s").
		SetValue("0").
		SetShowSpark(true)

	d.cardBytesIn = components.NewMetricCard().
		SetLabel("Bytes In/s").
		SetValue("0 B").
		SetShowSpark(true)

	d.cardBytesOut = components.NewMetricCard().
		SetLabel("Bytes Out/s").
		SetValue("0 B").
		SetShowSpark(true)

	d.cardRTT = components.NewMetricCard().
		SetLabel("RTT").
		SetValue("-").
		SetShowSpark(true)

	// Row 2 left: Line graph
	d.throughputGraph = components.NewLineGraph().
		SetTitle("Message Throughput").
		SetStyle(components.LineGraphSolid).
		SetShowGrid(true).
		SetShowLegend(true).
		SetAutoScale(true).
		SetYAxis(components.AxisConfig{
			Show:       true,
			LabelCount: 5,
			Format:     "%.0f",
		})

	// Row 2 right: Gauges
	d.memoryGauge = components.NewGauge().
		SetLabel("Memory").
		SetValue(0)

	d.storeGauge = components.NewGauge().
		SetLabel("Store").
		SetValue(0)

	// Compact metric cards for JetStream stats
	d.cardStreams = components.NewMetricCard().
		SetLabel("Streams").
		SetValue("0").
		SetCompact(true).
		SetShowBorder(false)

	d.cardConsumers = components.NewMetricCard().
		SetLabel("Consumers").
		SetValue("0").
		SetCompact(true).
		SetShowBorder(false)

	d.cardAPI = components.NewMetricCard().
		SetLabel("API").
		SetValue("0 / 0 err").
		SetCompact(true).
		SetShowBorder(false)

	// Server info panel
	d.serverText = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)
	d.serverText.SetBackgroundColor(theme.Get().Bg())
	theme.Register(d.serverText)

	d.serverPanel = components.NewPanel().
		SetTitle("Server").
		SetContent(d.serverText)

	// Row 3 left: Bar chart
	d.streamChart = components.NewBarChart().
		SetTitle("Per-Stream Messages").
		SetOrientation(components.BarHorizontal).
		SetShowValues(true).
		SetShowLabels(true).
		SetValueFormat("%.0f")

	// Row 3 right: Advisories
	d.advisoryText = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetScrollable(true)
	d.advisoryText.SetBackgroundColor(theme.Get().Bg())
	theme.Register(d.advisoryText)

	d.advisoryPanel = components.NewPanel().
		SetTitle("Advisories").
		SetContent(d.advisoryText)
}

func (d *Dashboard) buildLayout() {
	// Row 1: 5 metric cards side by side
	row1 := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(d.cardMsgsIn, 0, 1, false).
		AddItem(d.cardMsgsOut, 0, 1, false).
		AddItem(d.cardBytesIn, 0, 1, false).
		AddItem(d.cardBytesOut, 0, 1, false).
		AddItem(d.cardRTT, 0, 1, false)

	// Row 2 right: server info + JetStream gauges/cards stacked
	jsPanel := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.memoryGauge, 5, 0, false).
		AddItem(d.storeGauge, 5, 0, false).
		AddItem(d.cardStreams, 1, 0, false).
		AddItem(d.cardConsumers, 1, 0, false).
		AddItem(d.cardAPI, 1, 0, false)
	jsPanel.SetBackgroundColor(theme.Get().Bg())
	theme.Register(jsPanel)

	jsPanelWrapped := components.NewPanel().
		SetTitle("JetStream").
		SetContent(jsPanel)

	rightPanel := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(d.serverPanel, 0, 1, false).
		AddItem(jsPanelWrapped, 0, 1, false)
	rightPanel.SetBackgroundColor(theme.Get().Bg())
	theme.Register(rightPanel)

	// Row 2: graph (left) + server+JetStream (right)
	row2 := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(d.throughputGraph, 0, 3, false).
		AddItem(nil, 1, 0, false).
		AddItem(rightPanel, 0, 1, false)

	// Row 3: bar chart (left) + advisories (right)
	row3 := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(d.streamChart, 0, 1, false).
		AddItem(d.advisoryPanel, 0, 1, false)

	// Main layout: three rows stacked vertically
	d.Flex = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(row1, 6, 0, false).
		AddItem(row2, 0, 3, false).
		AddItem(row3, 0, 2, false)
	d.Flex.SetBackgroundColor(theme.Get().Bg())
	theme.Register(d.Flex)
}

func (d *Dashboard) setupBindings() {
	d.metrics = binding.NewValue(metricsSnapshot{})
	d.metrics.BindToWithDraw(func(snap metricsSnapshot) {
		d.renderMetrics(snap)
	})

	d.advisories = binding.NewValue([]nats.Advisory{})
	d.advisories.BindToWithDraw(func(advs []nats.Advisory) {
		d.renderAdvisories(advs)
	})
}

func (d *Dashboard) CommandContext() CommandViewContext { return CommandViewContext{} }

func (d *Dashboard) Name() string { return "Dashboard" }

func (d *Dashboard) Start() {
	// Reset lifecycle state so the dashboard works after being re-pushed
	// (e.g. escaping back from a sub-view calls Stop then Start again).
	atomic.StoreInt32(&d.stopped, 0)
	d.refreshCancel()
	ctx, cancel := context.WithCancel(context.Background())
	d.refreshCancel = cancel

	go d.pollLoop(ctx)
}

func (d *Dashboard) Stop() {
	atomic.StoreInt32(&d.stopped, 1)
	d.refreshCancel()
	if provider := d.app.Provider(); provider != nil {
		provider.UnsubscribeAdvisories()
	}
}

func (d *Dashboard) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "s", Description: "Streams"},
		{Key: "k", Description: "KV Stores"},
		{Key: "o", Description: "Object Stores"},
		{Key: "m", Description: "Monitor"},
		{Key: "y", Description: "Yank"},
		{Key: "r", Description: "Refresh"},
		{Key: "q", Description: "Quit"},
	}
}

func (d *Dashboard) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return d.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch event.Rune() {
		case 's':
			d.app.NavigateToStreams()
		case 'k':
			d.app.NavigateToKVStores()
		case 'o':
			d.app.NavigateToObjectStores()
		case 'm':
			d.app.NavigateToMessageMonitor()
		case 'y':
			snap := d.metrics.Get()
			info := map[string]interface{}{
				"server":  snap.server,
				"streams": snap.streams,
				"consumers": snap.consumers,
				"memory_used": snap.memoryUsed,
				"store_used":  snap.storeUsed,
				"api_total":   snap.apiTotal,
				"api_errors":  snap.apiErrors,
				"domain":      snap.domain,
			}
			data, err := json.MarshalIndent(info, "", "  ")
			if err != nil {
				d.app.ShowError(err.Error())
			} else if err := clipboard.Copy(string(data)); err != nil {
				d.app.ShowError("Clipboard: " + err.Error())
			} else {
				d.app.ShowSuccess("Copied server info")
			}
		case 'r':
			go d.refresh()
		}
	})
}

// pollLoop runs the 2-second stats ticker and 10-second streams ticker.
func (d *Dashboard) pollLoop(ctx context.Context) {
	// Wait until the tview event loop is running so QueueUpdateDraw
	// calls are drained immediately instead of piling up in memory.
	select {
	case <-d.app.Ready():
	case <-ctx.Done():
		return
	}

	d.subscribeAdvisories()

	// Kick off the first stats and streams fetch concurrently.
	d.refresh()
	d.flushAdvisories()
	go d.refreshStreams()

	statsTicker := time.NewTicker(pollInterval)
	streamsTicker := time.NewTicker(streamInterval)
	defer statsTicker.Stop()
	defer streamsTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTicker.C:
			d.refresh()
			d.flushAdvisories()
		case <-streamsTicker.C:
			d.refreshStreams()
		}
	}
}

func (d *Dashboard) refresh() {
	provider := d.app.Provider()
	if provider == nil {
		return
	}

	if atomic.LoadInt32(&d.stopped) == 1 {
		return
	}

	// Record connection stats and compute rates
	stats := provider.ConnectionStats()
	d.collector.recordStats(stats)

	// Get server info (cheap — reads cached connection state)
	srvInfo := provider.ServerInfo()

	// Get account info (skip when JetStream is not available)
	jsEnabled := provider.JetStreamEnabled(context.Background())

	var info *jetstream.AccountInfo
	if jsEnabled {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		info, _ = provider.AccountInfo(ctx)
		cancel()
	}

	if atomic.LoadInt32(&d.stopped) == 1 {
		return
	}

	// Record RTT
	d.collector.mu.Lock()
	d.collector.rtt.add(float64(srvInfo.RTT.Microseconds()))
	rttHist := d.collector.rtt.snapshot()
	d.collector.mu.Unlock()

	// Build snapshot
	msgsIn, msgsOut, bytesIn, bytesOut := d.collector.snapshot()

	snap := metricsSnapshot{
		msgsInPerSec:   msgsIn.last(),
		msgsOutPerSec:  msgsOut.last(),
		bytesInPerSec:  bytesIn.last(),
		bytesOutPerSec: bytesOut.last(),
		msgsInHistory:  msgsIn.snapshot(),
		msgsOutHistory: msgsOut.snapshot(),
		bytesInHistory:  bytesIn.snapshot(),
		bytesOutHistory: bytesOut.snapshot(),
		rttHistory:     rttHist,
		server:         srvInfo,
		jsAvailable:    jsEnabled,
	}

	if info != nil {
		snap.memoryUsed = info.Memory
		snap.memoryLimit = info.Limits.MaxMemory
		snap.storeUsed = info.Store
		snap.storeLimit = info.Limits.MaxStore
		snap.streams = info.Streams
		snap.maxStreams = info.Limits.MaxStreams
		snap.consumers = info.Consumers
		snap.maxConsumers = info.Limits.MaxConsumers
		snap.apiTotal = info.API.Total
		snap.apiErrors = info.API.Errors
		snap.domain = info.Domain
	}

	// Preserve stream data from previous snapshot
	prev := d.metrics.Get()
	if len(prev.streamNames) > 0 && len(snap.streamNames) == 0 {
		snap.streamNames = prev.streamNames
		snap.streamMsgs = prev.streamMsgs
	}

	d.metrics.SetAndDraw(snap)
}

func (d *Dashboard) refreshStreams() {
	provider := d.app.Provider()
	if provider == nil {
		return
	}

	if !provider.JetStreamEnabled(context.Background()) {
		return
	}

	if atomic.LoadInt32(&d.stopped) == 1 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Collect stream info then push a single update.
	// Cap at a reasonable limit so the dashboard bar chart stays usable
	// and we don't pull the full listing on accounts with thousands of streams.
	// We use a separate cancellable context so we can abort the SDK paging
	// once we have enough entries.
	const maxDashboardStreams = 500
	iterCtx, iterCancel := context.WithCancel(ctx)
	defer iterCancel()
	var names []string
	var msgs []float64
	_ = provider.ListStreamsIter(iterCtx, func(info *jetstream.StreamInfo) {
		if atomic.LoadInt32(&d.stopped) == 1 {
			iterCancel()
			return
		}
		names = append(names, info.Config.Name)
		msgs = append(msgs, float64(info.State.Msgs))
		if len(names) >= maxDashboardStreams {
			iterCancel()
		}
	})

	d.metrics.UpdateAndDraw(func(snap metricsSnapshot) metricsSnapshot {
		snap.streamNames = names
		snap.streamMsgs = msgs
		return snap
	})
}

func (d *Dashboard) renderMetrics(snap metricsSnapshot) {
	// Row 1: Update metric cards
	d.cardMsgsIn.
		SetValue(formatRate(snap.msgsInPerSec)).
		SetSparkline(snap.msgsInHistory).
		SetTrend(rateTrend(snap.msgsInHistory), "", true)

	d.cardMsgsOut.
		SetValue(formatRate(snap.msgsOutPerSec)).
		SetSparkline(snap.msgsOutHistory).
		SetTrend(rateTrend(snap.msgsOutHistory), "", true)

	d.cardBytesIn.
		SetValue(formatBytesRate(snap.bytesInPerSec)).
		SetSparkline(snap.bytesInHistory).
		SetTrend(rateTrend(snap.bytesInHistory), "", true)

	d.cardBytesOut.
		SetValue(formatBytesRate(snap.bytesOutPerSec)).
		SetSparkline(snap.bytesOutHistory).
		SetTrend(rateTrend(snap.bytesOutHistory), "", true)

	// RTT card
	if len(snap.rttHistory) > 0 {
		lastRTT := snap.rttHistory[len(snap.rttHistory)-1]
		rttLabel := fmt.Sprintf("%.0fus", lastRTT)
		if lastRTT >= 1000 {
			rttLabel = fmt.Sprintf("%.1fms", lastRTT/1000)
		}
		d.cardRTT.
			SetValue(rttLabel).
			SetSparkline(snap.rttHistory).
			SetTrend(rateTrend(snap.rttHistory), "", false)
	}

	// Row 2 left: Update throughput graph
	d.throughputGraph.SetSeries(
		components.DataSeries{
			Label:  "In msgs/s",
			Values: snap.msgsInHistory,
			Color:  theme.Get().Success(),
		},
		components.DataSeries{
			Label:  "Out msgs/s",
			Values: snap.msgsOutHistory,
			Color:  theme.Get().Warning(),
		},
	)

	// Row 2 right: Update JetStream gauges and cards
	if !snap.jsAvailable {
		d.memoryGauge.SetValue(0)
		d.memoryGauge.SetLabel("Memory  -")
		d.storeGauge.SetValue(0)
		d.storeGauge.SetLabel("Store  -")
		d.cardStreams.SetValue("-")
		d.cardConsumers.SetValue("-")
		d.cardAPI.SetValue("-")
	} else {
		// Limits of -1 mean unlimited in NATS; treat as no cap.
		if snap.memoryLimit > 0 {
			d.memoryGauge.SetValue(float64(snap.memoryUsed) / float64(snap.memoryLimit))
			d.memoryGauge.SetLabel(fmt.Sprintf("Memory %s / %s", formatBytes(snap.memoryUsed), formatBytes(uint64(snap.memoryLimit))))
		} else if snap.memoryLimit == -1 {
			d.memoryGauge.SetValue(0)
			d.memoryGauge.SetLabel(fmt.Sprintf("Memory %s / unlimited", formatBytes(snap.memoryUsed)))
		} else {
			d.memoryGauge.SetValue(0)
			d.memoryGauge.SetLabel(fmt.Sprintf("Memory %s", formatBytes(snap.memoryUsed)))
		}
		if snap.storeLimit > 0 {
			d.storeGauge.SetValue(float64(snap.storeUsed) / float64(snap.storeLimit))
			d.storeGauge.SetLabel(fmt.Sprintf("Store %s / %s", formatBytes(snap.storeUsed), formatBytes(uint64(snap.storeLimit))))
		} else if snap.storeLimit == -1 {
			d.storeGauge.SetValue(0)
			d.storeGauge.SetLabel(fmt.Sprintf("Store %s / unlimited", formatBytes(snap.storeUsed)))
		} else {
			d.storeGauge.SetValue(0)
			d.storeGauge.SetLabel(fmt.Sprintf("Store %s", formatBytes(snap.storeUsed)))
		}

		if snap.maxStreams > 0 {
			d.cardStreams.SetValue(fmt.Sprintf("%d / %d", snap.streams, snap.maxStreams))
		} else {
			d.cardStreams.SetValue(fmt.Sprintf("%d", snap.streams))
		}
		if snap.maxConsumers > 0 {
			d.cardConsumers.SetValue(fmt.Sprintf("%d / %d", snap.consumers, snap.maxConsumers))
		} else {
			d.cardConsumers.SetValue(fmt.Sprintf("%d", snap.consumers))
		}
		d.cardAPI.SetValue(fmt.Sprintf("%d / %d err", snap.apiTotal, snap.apiErrors))
	}

	// Server info panel
	d.renderServerInfo(snap)

	// Row 3 left: Update bar chart (sorted by most messages first)
	if !snap.jsAvailable {
		d.streamChart.SetItems(components.BarItem{Label: "JetStream disabled", Value: 0})
	} else if len(snap.streamNames) > 0 {
		items := make([]components.BarItem, len(snap.streamNames))
		for i, name := range snap.streamNames {
			items[i] = components.BarItem{
				Label: name,
				Value: snap.streamMsgs[i],
			}
		}
		sort.Slice(items, func(i, j int) bool {
			return items[i].Value > items[j].Value
		})
		d.streamChart.SetItems(items...)
	}
}

func (d *Dashboard) renderServerInfo(snap metricsSnapshot) {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()
	srv := snap.server

	connStatus := "[green]Connected[-]"
	if !srv.TLS {
		connStatus += fmt.Sprintf(" [%s](plain)[-]", dim)
	} else {
		connStatus += fmt.Sprintf(" [%s](TLS)[-]", dim)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Status:[-]   %s\n", dim, connStatus)

	name := srv.Name
	if name == "" {
		name = srv.ID
	}
	if name != "" {
		fmt.Fprintf(&b, "[%s]Server:[-]   [%s]%s[-]\n", dim, accent, name)
	}
	if srv.Version != "" {
		fmt.Fprintf(&b, "[%s]Version:[-]  [%s]%s[-]\n", dim, accent, srv.Version)
	}
	if srv.Cluster != "" {
		fmt.Fprintf(&b, "[%s]Cluster:[-]  [%s]%s[-]\n", dim, accent, srv.Cluster)
	}
	if snap.domain != "" {
		fmt.Fprintf(&b, "[%s]Domain:[-]   [%s]%s[-]\n", dim, accent, snap.domain)
	}
	fmt.Fprintf(&b, "[%s]RTT:[-]      [%s]%s[-]\n", dim, accent, srv.RTT.Round(time.Microsecond))
	fmt.Fprintf(&b, "[%s]Payload:[-]  [%s]%s[-]\n", dim, accent, formatBytes(uint64(srv.MaxPayload)))
	if srv.ClientID > 0 {
		fmt.Fprintf(&b, "[%s]Client:[-]   [%s]%d[-]\n", dim, accent, srv.ClientID)
	}
	if len(srv.Servers) > 1 {
		fmt.Fprintf(&b, "[%s]Nodes:[-]    [%s]%d[-]\n", dim, accent, len(srv.Servers))
	}
	if srv.Reconnects > 0 {
		fmt.Fprintf(&b, "[%s]Reconns:[-]  [yellow]%d[-]\n", dim, srv.Reconnects)
	}

	d.serverText.SetText(b.String())
}

func (d *Dashboard) subscribeAdvisories() {
	provider := d.app.Provider()
	if provider == nil {
		return
	}
	if !provider.JetStreamEnabled(context.Background()) {
		return
	}

	ctx := context.Background()
	_ = provider.SubscribeAdvisories(ctx, func(adv nats.Advisory) {
		if atomic.LoadInt32(&d.stopped) == 1 {
			return
		}
		d.advMu.Lock()
		d.advBuf = append(d.advBuf, adv)
		if len(d.advBuf) > maxAdvisories {
			d.advBuf = d.advBuf[len(d.advBuf)-maxAdvisories:]
		}
		d.advMu.Unlock()
	})
}

// flushAdvisories drains the advisory buffer into the binding.
func (d *Dashboard) flushAdvisories() {
	d.advMu.Lock()
	pending := d.advBuf
	d.advBuf = nil
	d.advMu.Unlock()

	if len(pending) == 0 {
		return
	}

	d.advisories.UpdateAndDraw(func(advs []nats.Advisory) []nats.Advisory {
		advs = append(advs, pending...)
		if len(advs) > maxAdvisories {
			advs = advs[len(advs)-maxAdvisories:]
		}
		return advs
	})
}

func (d *Dashboard) renderAdvisories(advisories []nats.Advisory) {
	if len(advisories) == 0 {
		dim := theme.TagFgDim()
		d.advisoryText.SetText(fmt.Sprintf("[%s]No advisories received[-]", dim))
		return
	}

	dim := theme.TagFgDim()
	warn := theme.TagWarning()
	var b strings.Builder

	for i := len(advisories) - 1; i >= 0; i-- {
		adv := advisories[i]
		ts := adv.Timestamp.Format("15:04:05")

		target := adv.Stream
		if adv.Consumer != "" {
			target += " > " + adv.Consumer
		}

		fmt.Fprintf(&b, "[%s]%s[-] [%s]%s[-]", dim, ts, warn, adv.Type)
		if target != "" {
			fmt.Fprintf(&b, " [%s]%s[-]", dim, target)
		}
		if adv.Message != "" {
			fmt.Fprintf(&b, "\n         %s", adv.Message)
		}
		b.WriteString("\n")
	}

	d.advisoryText.SetText(b.String())
	d.advisoryText.ScrollToBeginning()
}

// --- Formatting helpers ---

func formatRate(v float64) string {
	if v < 1 {
		return fmt.Sprintf("%.1f", v)
	}
	return formatNumber(uint64(v))
}

func formatBytesRate(v float64) string {
	return formatBytes(uint64(v)) + "/s"
}

func rateTrend(history []float64) components.Trend {
	if len(history) < 2 {
		return components.TrendNeutral
	}
	curr := history[len(history)-1]
	prev := history[len(history)-2]
	if curr > prev*1.05 {
		return components.TrendUp
	}
	if curr < prev*0.95 {
		return components.TrendDown
	}
	return components.TrendNeutral
}

func formatBytes(b uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func formatNumber(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1_000_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1_000_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
}
