package view

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

// StreamDetail shows full configuration and state for a single stream.
type StreamDetail struct {
	*components.Split
	app        *App
	streamName string

	configView  *tview.TextView
	stateView   *tview.TextView
	clusterView *tview.TextView

	info        *binding.Value[*jetstream.StreamInfo]
	stopRefresh chan struct{}
	stopped     int32
}

// NewStreamDetail creates a stream detail view.
func NewStreamDetail(app *App, name string) *StreamDetail {
	sd := &StreamDetail{
		app:         app,
		streamName:  name,
		stopRefresh: make(chan struct{}),
	}

	sd.configView = tview.NewTextView().SetDynamicColors(true)
	sd.configView.SetBackgroundColor(theme.Get().Bg())
	theme.Register(sd.configView)

	sd.stateView = tview.NewTextView().SetDynamicColors(true)
	sd.stateView.SetBackgroundColor(theme.Get().Bg())
	theme.Register(sd.stateView)

	sd.clusterView = tview.NewTextView().SetDynamicColors(true)
	sd.clusterView.SetBackgroundColor(theme.Get().Bg())
	theme.Register(sd.clusterView)

	// Set up reactive binding for stream info
	sd.info = binding.NewValue[*jetstream.StreamInfo](nil)
	sd.info.BindToWithDraw(func(info *jetstream.StreamInfo) {
		if info != nil {
			sd.renderConfig(info)
			sd.renderState(info)
			sd.renderCluster(info)
		}
	})

	configPanel := components.NewPanel().SetTitle("Config").SetContent(sd.configView)
	clusterPanel := components.NewPanel().SetTitle("Cluster").SetContent(sd.clusterView)
	statePanel := components.NewPanel().SetTitle("State").SetContent(sd.stateView)

	// Left column with config and cluster panels
	leftCol := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(configPanel, 0, 2, false).
		AddItem(clusterPanel, 0, 1, false)
	leftCol.SetBackgroundColor(theme.Get().Bg())
	theme.Register(leftCol)

	// Use Split for resizable panes (Ctrl+Arrow to resize)
	sd.Split = components.NewSplit().
		SetDirection(components.SplitHorizontal).
		SetRatio(0.5).
		SetLeft(leftCol).
		SetRight(statePanel)

	return sd
}

func (sd *StreamDetail) Name() string { return sd.streamName }

func (sd *StreamDetail) Start() {
	go func() {
		sd.loadInfo()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sd.stopRefresh:
				return
			case <-ticker.C:
				sd.loadInfo()
			}
		}
	}()
}

func (sd *StreamDetail) Stop() {
	atomic.StoreInt32(&sd.stopped, 1)
	select {
	case sd.stopRefresh <- struct{}{}:
	default:
	}
}

func (sd *StreamDetail) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "n", Description: "Consumers"},
		{Key: "w", Description: "Watch messages"},
		{Key: "p", Description: "Purge"},
		{Key: "r", Description: "Refresh"},
		{Key: "Esc", Description: "Back"},
	}
}

func (sd *StreamDetail) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return sd.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch event.Rune() {
		case 'n':
			sd.app.NavigateToConsumers(sd.streamName)
		case 'w':
			// Watch messages - navigate to monitor with stream's first subject
			if info := sd.info.Get(); info != nil && len(info.Config.Subjects) > 0 {
				// Use the first subject pattern (usually a wildcard like "orders.>")
				sd.app.NavigateToMessageMonitorWithSubject(info.Config.Subjects[0])
			}
		case 'r':
			go sd.loadInfo()
		}
	})
}

func (sd *StreamDetail) loadInfo() {
	provider := sd.app.Provider()
	if provider == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		info, err := provider.GetStreamInfo(ctx, sd.streamName)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&sd.stopped) == 1 {
			return
		}

		if err != nil {
			sd.app.QueueUpdateDraw(func() {
				sd.configView.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
			})
			return
		}
		sd.info.SetAndDraw(info)
	}()
}

func (sd *StreamDetail) renderConfig(info *jetstream.StreamInfo) {
	if info == nil {
		return
	}
	cfg := info.Config
	dim := theme.TagFgDim()

	storage := "File"
	if cfg.Storage == jetstream.MemoryStorage {
		storage = "Memory"
	}

	text := fmt.Sprintf(
		"[%s]Name:[-]         %s\n"+
			"[%s]Description:[-]  %s\n"+
			"[%s]Subjects:[-]     %s\n"+
			"[%s]Retention:[-]    %s\n"+
			"[%s]Storage:[-]      %s\n"+
			"[%s]Replicas:[-]     %d\n"+
			"\n"+
			"[%s]MaxMsgs:[-]      %s\n"+
			"[%s]MaxBytes:[-]     %s\n"+
			"[%s]MaxAge:[-]       %s\n"+
			"[%s]MaxMsgSize:[-]   %s\n"+
			"[%s]MaxConsumers:[-] %s\n"+
			"[%s]Discard:[-]      %s\n"+
			"\n"+
			"[%s]AllowDirect:[-]  %v\n"+
			"[%s]DenyDelete:[-]   %v\n"+
			"[%s]DenyPurge:[-]    %v\n"+
			"[%s]Sealed:[-]       %v",
		dim, cfg.Name,
		dim, cfg.Description,
		dim, strings.Join(cfg.Subjects, ", "),
		dim, retentionString(cfg.Retention),
		dim, storage,
		dim, cfg.Replicas,
		dim, limitString(cfg.MaxMsgs),
		dim, byteLimitString(cfg.MaxBytes),
		dim, durationString(cfg.MaxAge),
		dim, byteLimitString32(cfg.MaxMsgSize),
		dim, limitString(int64(cfg.MaxConsumers)),
		dim, discardString(cfg.Discard),
		dim, cfg.AllowDirect,
		dim, cfg.DenyDelete,
		dim, cfg.DenyPurge,
		dim, cfg.Sealed,
	)

	sd.configView.SetText(text)
}

func (sd *StreamDetail) renderState(info *jetstream.StreamInfo) {
	if info == nil {
		return
	}
	s := info.State
	dim := theme.TagFgDim()

	lastTime := "never"
	if !s.LastTime.IsZero() {
		lastTime = s.LastTime.Format(time.RFC3339) + " (" + time.Since(s.LastTime).Round(time.Second).String() + " ago)"
	}
	firstTime := "never"
	if !s.FirstTime.IsZero() {
		firstTime = s.FirstTime.Format(time.RFC3339)
	}

	text := fmt.Sprintf(
		"[%s]Messages:[-]     %s\n"+
			"[%s]Bytes:[-]        %s\n"+
			"[%s]First Seq:[-]    %d\n"+
			"[%s]First Time:[-]   %s\n"+
			"[%s]Last Seq:[-]     %d\n"+
			"[%s]Last Time:[-]    %s\n"+
			"[%s]Consumers:[-]    %d\n"+
			"[%s]Deleted:[-]      %d\n"+
			"[%s]Num Subjects:[-] %d",
		dim, formatNumber(s.Msgs),
		dim, formatBytes(s.Bytes),
		dim, s.FirstSeq,
		dim, firstTime,
		dim, s.LastSeq,
		dim, lastTime,
		dim, s.Consumers,
		dim, s.NumDeleted,
		dim, s.NumSubjects,
	)

	sd.stateView.SetText(text)
}

func (sd *StreamDetail) renderCluster(info *jetstream.StreamInfo) {
	if info == nil || info.Cluster == nil {
		sd.clusterView.SetText("[dim]No cluster info[-]")
		return
	}

	cl := info.Cluster
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Cluster:[-]  %s\n", dim, cl.Name)
	fmt.Fprintf(&b, "[%s]Leader:[-]   [%s]%s[-]\n\n", dim, accent, cl.Leader)

	if len(cl.Replicas) > 0 {
		for _, r := range cl.Replicas {
			status := "[green]current[-]"
			if !r.Current {
				status = fmt.Sprintf("[yellow]lag: %d[-]", r.Lag)
			}
			if r.Offline {
				status = "[red]offline[-]"
			}
			active := ""
			if r.Active > 0 {
				active = fmt.Sprintf(" (%s ago)", r.Active.Round(time.Second))
			}
			fmt.Fprintf(&b, "  %s  %s%s\n", r.Name, status, active)
		}
	}

	sd.clusterView.SetText(b.String())
}

func limitString(v int64) string {
	if v < 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d", v)
}

func byteLimitString(v int64) string {
	if v < 0 {
		return "unlimited"
	}
	return formatBytes(uint64(v))
}

func byteLimitString32(v int32) string {
	if v < 0 {
		return "unlimited"
	}
	return formatBytes(uint64(v))
}

func durationString(d time.Duration) string {
	if d == 0 {
		return "unlimited"
	}
	return d.String()
}

func discardString(d jetstream.DiscardPolicy) string {
	switch d {
	case jetstream.DiscardOld:
		return "Old"
	case jetstream.DiscardNew:
		return "New"
	default:
		return "Unknown"
	}
}
