package view

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atterpac/gnat/internal/nats"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

const maxAdvisories = 100

// Dashboard is the top-level overview of the NATS JetStream account.
type Dashboard struct {
	*components.Split
	app *App

	connectionPanel *components.Panel
	connectionText  *tview.TextView

	advisoryPanel *components.Panel
	advisoryText  *tview.TextView

	// Reactive state
	connectionData *binding.Value[dashboardData]
	advisories     *binding.Value[[]nats.Advisory]

	stopRefresh chan struct{}
	stopped     int32
}

// NewDashboard creates the dashboard view.
func NewDashboard(app *App) *Dashboard {
	d := &Dashboard{
		app:         app,
		stopRefresh: make(chan struct{}),
	}

	d.connectionText = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)
	d.connectionText.SetBackgroundColor(theme.Get().Bg())
	theme.Register(d.connectionText)

	d.connectionPanel = components.NewPanel().
		SetTitle("Connection").
		SetContent(d.connectionText)

	d.advisoryText = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft).
		SetScrollable(true)
	d.advisoryText.SetBackgroundColor(theme.Get().Bg())
	theme.Register(d.advisoryText)

	d.advisoryPanel = components.NewPanel().
		SetTitle("Advisories").
		SetContent(d.advisoryText)

	// Set up reactive bindings
	d.connectionData = binding.NewValue(dashboardData{})
	d.connectionData.BindToWithDraw(func(data dashboardData) {
		d.renderConnection(data)
	})

	d.advisories = binding.NewValue([]nats.Advisory{})
	d.advisories.BindToWithDraw(func(advs []nats.Advisory) {
		d.renderAdvisories(advs)
	})

	// Use Split for resizable panes (Ctrl+Arrow to resize)
	d.Split = components.NewSplit().
		SetDirection(components.SplitHorizontal).
		SetRatio(0.33).
		SetLeft(d.connectionPanel).
		SetRight(d.advisoryPanel)

	return d
}

func (d *Dashboard) Name() string { return "Dashboard" }

func (d *Dashboard) Start() {
	d.subscribeAdvisories()
	go func() {
		d.refresh()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-d.stopRefresh:
				return
			case <-ticker.C:
				d.refresh()
			}
		}
	}()
}

func (d *Dashboard) Stop() {
	atomic.StoreInt32(&d.stopped, 1)
	select {
	case d.stopRefresh <- struct{}{}:
	default:
	}
}

func (d *Dashboard) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "s", Description: "Streams"},
		{Key: "k", Description: "KV Stores"},
		{Key: "o", Description: "Object Stores"},
		{Key: "m", Description: "Monitor"},
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
		case 'r':
			go d.refresh()
		}
	})
}

func (d *Dashboard) subscribeAdvisories() {
	provider := d.app.Provider()
	if provider == nil {
		return
	}

	ctx := context.Background()
	_ = provider.SubscribeAdvisories(ctx, func(adv nats.Advisory) {
		// Don't process advisories after dashboard is stopped
		if atomic.LoadInt32(&d.stopped) == 1 {
			return
		}
		d.advisories.UpdateAndDraw(func(advs []nats.Advisory) []nats.Advisory {
			advs = append(advs, adv)
			if len(advs) > maxAdvisories {
				advs = advs[len(advs)-maxAdvisories:]
			}
			return advs
		})
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

	// Show newest first
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

// dashboardData holds all data needed for the dashboard refresh.
type dashboardData struct {
	info      *jetstream.AccountInfo
	stats     nats.ConnectionStats
	rtt       time.Duration
	url       string
	connected bool
}

func (d *Dashboard) refresh() {
	provider := d.app.Provider()
	if provider == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		info, err := provider.AccountInfo(ctx)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&d.stopped) == 1 {
			return
		}

		rtt, _ := provider.RTT()
		data := dashboardData{
			info:      info,
			stats:     provider.ConnectionStats(),
			rtt:       rtt,
			url:       provider.ServerURL(),
			connected: provider.IsConnected(),
		}

		if err != nil {
			// Still update with connection stats, but note error
			d.connectionData.SetAndDraw(data)
			d.app.QueueUpdateDraw(func() {
				d.connectionText.SetText(d.connectionText.GetText(false) +
					fmt.Sprintf("\n[red]Account error: %v[-]", err))
			})
		} else {
			d.connectionData.SetAndDraw(data)
		}
	}()
}

func (d *Dashboard) renderConnection(data dashboardData) {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	connStatus := "[green]Connected[-]"
	if !data.connected {
		connStatus = "[red]Disconnected[-]"
	}

	var accountInfo string
	if data.info != nil {
		accountInfo = fmt.Sprintf(
			"\n\n[%s]── Account ──[-]\n"+
				"[%s]Memory:[-]      [%s]%s[-]\n"+
				"[%s]Store:[-]       [%s]%s[-]\n"+
				"[%s]API Total:[-]   %d\n"+
				"[%s]API Errors:[-]  %d\n"+
				"[%s]Streams:[-]     %d / %d\n"+
				"[%s]Consumers:[-]   %d / %d",
			dim,
			dim, accent, formatBytes(data.info.Memory),
			dim, accent, formatBytes(data.info.Store),
			dim, data.info.API.Total,
			dim, data.info.API.Errors,
			dim, data.info.Streams, data.info.Limits.MaxStreams,
			dim, data.info.Consumers, data.info.Limits.MaxConsumers,
		)
	}

	d.connectionText.SetText(fmt.Sprintf(
		"[%s]Status:[-]      %s\n"+
			"[%s]Server:[-]      %s\n"+
			"[%s]RTT:[-]         %s\n"+
			"[%s]InMsgs:[-]      %s\n"+
			"[%s]OutMsgs:[-]     %s\n"+
			"[%s]Reconnects:[-]  %d%s",
		dim, connStatus,
		dim, data.url,
		dim, data.rtt.Round(time.Microsecond),
		dim, formatNumber(data.stats.InMsgs),
		dim, formatNumber(data.stats.OutMsgs),
		dim, data.stats.Reconnects,
		accountInfo,
	))
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
