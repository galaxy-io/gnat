package view

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atterpac/dado/binding"
	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/atterpac/dado/theme"
	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
)

// ConsumerDetail shows full config and metrics for a single consumer.
type ConsumerDetail struct {
	*components.Split
	app          *App
	streamName   string
	consumerName string

	configView  *core.TextView
	metricsView *core.TextView

	info          *binding.Value[*jetstream.ConsumerInfo]
	refreshCancel context.CancelFunc
	stopped       int32

	// For rate calculation
	lastDelivered uint64
	lastAcked     uint64
	lastSample    time.Time

	// Lag history (Feature 7)
	lagHistory []float64

	// Ack staleness tracking (Feature 12)
	lastAckFloor  uint64
	lastAckChange time.Time
}

// NewConsumerDetail creates the consumer detail view.
func NewConsumerDetail(app *App, streamName, consumerName string) *ConsumerDetail {
	cd := &ConsumerDetail{
		app:           app,
		streamName:    streamName,
		consumerName:  consumerName,
		refreshCancel: func() {},
	}

	cd.configView = core.NewTextView().SetDynamicColors(true)
	cd.configView.SetBackgroundColor(theme.Get().Bg())

	cd.metricsView = core.NewTextView().SetDynamicColors(true)
	cd.metricsView.SetBackgroundColor(theme.Get().Bg())

	// Set up reactive binding for consumer info
	cd.info = binding.NewValue[*jetstream.ConsumerInfo](nil)
	cd.info.BindToWithDraw(func(info *jetstream.ConsumerInfo) {
		if info != nil {
			cd.renderConfig(info)
			cd.renderMetrics(info)
		}
	})

	configPanel := components.NewPanel().SetTitle("Config").SetContent(cd.configView)
	metricsPanel := components.NewPanel().SetTitle("Metrics").SetContent(cd.metricsView)

	// Use Split for resizable panes (Ctrl+Arrow to resize)
	cd.Split = components.NewSplit().
		SetDirection(components.SplitHorizontal).
		SetRatio(0.5).
		SetLeft(configPanel).
		SetRight(metricsPanel)

	return cd
}

func (cd *ConsumerDetail) CommandContext() CommandViewContext {
	return CommandViewContext{Stream: cd.streamName, Consumer: cd.consumerName}
}

func (cd *ConsumerDetail) Name() string { return cd.consumerName }

func (cd *ConsumerDetail) Start() {
	atomic.StoreInt32(&cd.stopped, 0)
	cd.refreshCancel()
	ctx, cancel := context.WithCancel(context.Background())
	cd.refreshCancel = cancel
	go func() {
		cd.loadInfo()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cd.loadInfo()
			}
		}
	}()
}

func (cd *ConsumerDetail) Stop() {
	atomic.StoreInt32(&cd.stopped, 1)
	cd.refreshCancel()
}

func (cd *ConsumerDetail) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "e", Description: "Edit"},
		{Key: "y", Description: "Yank"},
		{Key: "x", Description: "Export"},
		{Key: "r", Description: "Refresh"},
		{Key: "Esc", Description: "Back"},
	}
}

func (cd *ConsumerDetail) HandleKey(event *tcell.EventKey) bool {
	switch event.Rune() {
	case 'e':
		if info := cd.info.Get(); info != nil {
			showConsumerEditForm(cd.app, cd.streamName, info, func() {
				go cd.loadInfo()
			})
		}
		return true
	case 'y':
		if info := cd.info.Get(); info != nil {
			data, err := json.MarshalIndent(info, "", "  ")
			if err != nil {
				cd.app.ShowError(err.Error())
			} else if err := clipboard.Copy(string(data)); err != nil {
				cd.app.ShowError("Clipboard: " + err.Error())
			} else {
				cd.app.ShowSuccess("Copied consumer info: " + cd.consumerName)
			}
		}
		return true
	case 'x':
		if info := cd.info.Get(); info != nil {
			data, err := json.MarshalIndent(info.Config, "", "  ")
			if err != nil {
				cd.app.ShowError(err.Error())
			} else if err := clipboard.Copy(string(data)); err != nil {
				cd.app.ShowError("Clipboard: " + err.Error())
			} else {
				cd.app.ShowSuccess("Exported consumer config to clipboard")
			}
		}
		return true
	case 'r':
		cd.loadInfo()
		return true
	}
	return cd.Split.HandleKey(event)
}

func (cd *ConsumerDetail) loadInfo() {
	provider := cd.app.Provider()
	if provider == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		info, err := provider.GetConsumerInfo(ctx, cd.streamName, cd.consumerName)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&cd.stopped) == 1 {
			return
		}

		if err != nil {
			cd.app.QueueUpdateDraw(func() {
				cd.configView.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
			})
			return
		}
		cd.info.SetAndDraw(info)
	}()
}

func (cd *ConsumerDetail) renderConfig(info *jetstream.ConsumerInfo) {
	if info == nil {
		return
	}
	cfg := info.Config
	dim := theme.TagFgDim()

	filter := cfg.FilterSubject
	if filter == "" && len(cfg.FilterSubjects) > 0 {
		filter = strings.Join(cfg.FilterSubjects, ", ")
	}

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Name:[-]              %s\n", dim, cd.consumerName)
	fmt.Fprintf(&b, "[%s]Description:[-]       %s\n", dim, cfg.Description)
	fmt.Fprintf(&b, "[%s]Filter:[-]            %s\n", dim, filter)
	fmt.Fprintf(&b, "[%s]Deliver Policy:[-]    %s\n", dim, deliverPolicyString(cfg.DeliverPolicy))
	fmt.Fprintf(&b, "[%s]Ack Policy:[-]        %s\n", dim, ackPolicyString(cfg.AckPolicy))
	fmt.Fprintf(&b, "[%s]Ack Wait:[-]          %s\n", dim, cfg.AckWait)
	fmt.Fprintf(&b, "[%s]Max Deliver:[-]       %d\n", dim, cfg.MaxDeliver)
	fmt.Fprintf(&b, "[%s]Max Ack Pending:[-]   %d\n", dim, cfg.MaxAckPending)
	fmt.Fprintf(&b, "[%s]Replay:[-]            %s\n", dim, replayPolicyString(cfg.ReplayPolicy))
	fmt.Fprintf(&b, "[%s]Inactive Threshold:[-]%s\n", dim, cfg.InactiveThreshold)
	fmt.Fprintf(&b, "\n[%s]--- Pull Config ---[-]\n", dim)
	fmt.Fprintf(&b, "[%s]Max Waiting:[-]       %d\n", dim, cfg.MaxWaiting)
	fmt.Fprintf(&b, "[%s]Max Batch:[-]         %d\n", dim, cfg.MaxRequestBatch)
	fmt.Fprintf(&b, "[%s]Max Expires:[-]       %s\n", dim, cfg.MaxRequestExpires)

	cd.configView.SetText(b.String())
}

func (cd *ConsumerDetail) renderMetrics(info *jetstream.ConsumerInfo) {
	if info == nil {
		return
	}
	dim := theme.TagFgDim()

	now := time.Now()
	var procRate, ackRate float64
	if !cd.lastSample.IsZero() {
		elapsed := now.Sub(cd.lastSample).Seconds()
		if elapsed > 0 {
			procRate = float64(info.Delivered.Consumer-cd.lastDelivered) / elapsed
			ackRate = float64(info.AckFloor.Consumer-cd.lastAcked) / elapsed
		}
	}
	cd.lastDelivered = info.Delivered.Consumer
	cd.lastAcked = info.AckFloor.Consumer
	cd.lastSample = now

	lag := info.NumPending + uint64(info.NumAckPending)
	errorRate := float64(0)
	if info.Delivered.Consumer > 0 {
		errorRate = float64(info.NumRedelivered) / float64(info.Delivered.Consumer) * 100
	}

	// Track lag history
	cd.lagHistory = append(cd.lagHistory, float64(lag))
	if len(cd.lagHistory) > 60 {
		cd.lagHistory = cd.lagHistory[len(cd.lagHistory)-60:]
	}

	// Track ack staleness
	if info.AckFloor.Consumer != cd.lastAckFloor {
		cd.lastAckFloor = info.AckFloor.Consumer
		cd.lastAckChange = now
	}
	ackStaleness := ""
	if !cd.lastAckChange.IsZero() {
		staleDur := now.Sub(cd.lastAckChange)
		ackStaleness = staleDur.Round(time.Second).String()
		if staleDur > 30*time.Second {
			ackStaleness = fmt.Sprintf("[yellow]%s[-]", ackStaleness)
		}
		if staleDur > 2*time.Minute {
			ackStaleness = fmt.Sprintf("[red]%s[-]", ackStaleness)
		}
	}

	// Lag sparkline (simple ASCII)
	lagSpark := ""
	if len(cd.lagHistory) > 1 {
		lagSpark = " " + miniSparkline(cd.lagHistory, 20)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Delivered Seq:[-]   #%d\n", dim, info.Delivered.Consumer)
	fmt.Fprintf(&b, "[%s]Ack Floor Seq:[-]   #%d\n", dim, info.AckFloor.Consumer)
	fmt.Fprintf(&b, "[%s]Proc Rate:[-]       %.1f msg/s\n", dim, procRate)
	fmt.Fprintf(&b, "[%s]Ack Rate:[-]        %.1f msg/s\n", dim, ackRate)
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "[%s]Pending:[-]         %s\n", dim, formatNumber(info.NumPending))
	fmt.Fprintf(&b, "[%s]Ack Pending:[-]     %d\n", dim, info.NumAckPending)
	fmt.Fprintf(&b, "[%s]Redelivered:[-]     %d\n", dim, info.NumRedelivered)
	fmt.Fprintf(&b, "[%s]Waiting:[-]         %d\n", dim, info.NumWaiting)
	fmt.Fprintf(&b, "[%s]Lag:[-]             %d%s\n", dim, lag, lagSpark)
	fmt.Fprintf(&b, "[%s]Error Rate:[-]      %.3f%%\n", dim, errorRate)
	if ackStaleness != "" {
		fmt.Fprintf(&b, "[%s]Ack Staleness:[-]   %s\n", dim, ackStaleness)
	}

	cd.metricsView.SetText(b.String())
}

func deliverPolicyString(p jetstream.DeliverPolicy) string {
	switch p {
	case jetstream.DeliverAllPolicy:
		return "All"
	case jetstream.DeliverLastPolicy:
		return "Last"
	case jetstream.DeliverNewPolicy:
		return "New"
	case jetstream.DeliverByStartSequencePolicy:
		return "ByStartSequence"
	case jetstream.DeliverByStartTimePolicy:
		return "ByStartTime"
	case jetstream.DeliverLastPerSubjectPolicy:
		return "LastPerSubject"
	default:
		return "Unknown"
	}
}

func replayPolicyString(p jetstream.ReplayPolicy) string {
	switch p {
	case jetstream.ReplayInstantPolicy:
		return "Instant"
	case jetstream.ReplayOriginalPolicy:
		return "Original"
	default:
		return "Unknown"
	}
}
