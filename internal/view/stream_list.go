package view

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

// StreamList displays all JetStream streams in a master-detail layout.
type StreamList struct {
	*components.MasterDetailView
	app *App

	table   *components.Table
	preview *tview.TextView

	binding *binding.TableBinding[*jetstream.StreamInfo]
}

// NewStreamList creates the stream list view.
func NewStreamList(app *App) *StreamList {
	sl := &StreamList{
		app: app,
	}

	sl.table = components.NewTable().
		SetHeaders("NAME", "MSGS", "BYTES", "CONSUMERS", "STORAGE", "REPLICAS")

	sl.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)

	// Set up reactive table binding
	sl.binding = binding.NewTableBinding[*jetstream.StreamInfo](sl.table).
		SetMapper(func(s *jetstream.StreamInfo) []string {
			storage := "File"
			if s.Config.Storage == jetstream.MemoryStorage {
				storage = "Memory"
			}
			return []string{
				s.Config.Name,
				formatNumber(s.State.Msgs),
				formatBytes(s.State.Bytes),
				fmt.Sprintf("%d", s.State.Consumers),
				storage,
				fmt.Sprintf("%d", s.Config.Replicas),
			}
		}).
		SetKeyMapper(func(s *jetstream.StreamInfo) string {
			return s.Config.Name
		}).
		SetFilter(binding.DefaultStringFilter(func(s *jetstream.StreamInfo) []string {
			return []string{s.Config.Name}
		})).
		SetFetcher(func() ([]*jetstream.StreamInfo, error) {
			provider := sl.app.Provider()
			if provider == nil {
				return nil, fmt.Errorf("no provider")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return provider.ListStreams(ctx)
		}).
		SetRefreshInterval(10 * time.Second).
		SetOnSelect(func(s *jetstream.StreamInfo) {
			// Enter opens watch view directly
			if len(s.Config.Subjects) > 0 {
				sl.app.NavigateToMessageMonitorWithSubject(s.Config.Subjects[0])
			}
		}).
		SetOnRefresh(func(data []*jetstream.StreamInfo, err error) {
			if err != nil {
				sl.app.QueueUpdateDraw(func() {
					sl.preview.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
				})
			}
		})

	sl.table.SetSelectionChangedFunc(func(row, col int) {
		sl.updatePreview(row)
	})

	sl.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Streams").
		SetDetailTitle("Preview").
		SetMasterContent(sl.table).
		SetDetailContent(sl.preview).
		SetRatio(0.6).
		ConfigureEmpty("󰋼", "No Streams", "No streams found")

	return sl
}

func (sl *StreamList) Name() string { return "Streams" }

func (sl *StreamList) Start() {
	sl.binding.Start()
}

func (sl *StreamList) Stop() {
	sl.binding.Stop()
}

func (sl *StreamList) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "Watch"},
		{Key: "v", Description: "Detail"},
		{Key: "n", Description: "Consumers"},
		{Key: "/", Description: "Filter"},
		{Key: "c", Description: "Create"},
		{Key: "d", Description: "Delete"},
		{Key: "r", Description: "Refresh"},
	}
}

func (sl *StreamList) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return sl.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Key() == tcell.KeyEnter:
			// Watch messages directly
			if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
				if len(s.Config.Subjects) > 0 {
					sl.app.NavigateToMessageMonitorWithSubject(s.Config.Subjects[0])
				}
			}
		case event.Rune() == 'v':
			// View stream detail
			if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
				sl.app.NavigateToStreamDetail(s.Config.Name)
			}
		case event.Rune() == 'n':
			// View consumers
			if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
				sl.app.NavigateToConsumers(s.Config.Name)
			}
		case event.Rune() == 'r':
			sl.binding.RefreshAsync()
		case event.Rune() == '/':
			sl.ShowSearch()
		default:
			if sl.HandleSearchKey(event) {
				return
			}
			if handler := sl.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (sl *StreamList) updatePreview(row int) {
	s, ok := sl.binding.GetItemValue(row)
	if !ok || s == nil {
		sl.preview.SetText("")
		return
	}
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	subjects := strings.Join(s.Config.Subjects, ", ")
	retention := retentionString(s.Config.Retention)
	storage := "File"
	if s.Config.Storage == jetstream.MemoryStorage {
		storage = "Memory"
	}

	lastTime := "never"
	if !s.State.LastTime.IsZero() {
		lastTime = time.Since(s.State.LastTime).Round(time.Second).String() + " ago"
	}

	cluster := "-"
	leader := "-"
	if s.Cluster != nil {
		cluster = s.Cluster.Name
		leader = s.Cluster.Leader
	}

	text := fmt.Sprintf(
		"[%s]Name:[-]       [%s]%s[-]\n"+
			"[%s]Subjects:[-]   %s\n"+
			"[%s]Retention:[-]  %s\n"+
			"[%s]Storage:[-]    %s\n"+
			"[%s]Replicas:[-]   %d\n"+
			"\n"+
			"[%s]Messages:[-]   %s\n"+
			"[%s]Bytes:[-]      %s\n"+
			"[%s]Consumers:[-]  %d\n"+
			"[%s]First Seq:[-]  %d\n"+
			"[%s]Last Seq:[-]   %d\n"+
			"[%s]Last Time:[-]  %s\n"+
			"\n"+
			"[%s]Cluster:[-]    %s\n"+
			"[%s]Leader:[-]     %s",
		dim, accent, s.Config.Name,
		dim, subjects,
		dim, retention,
		dim, storage,
		dim, s.Config.Replicas,
		dim, formatNumber(s.State.Msgs),
		dim, formatBytes(s.State.Bytes),
		dim, s.State.Consumers,
		dim, s.State.FirstSeq,
		dim, s.State.LastSeq,
		dim, lastTime,
		dim, cluster,
		dim, leader,
	)

	sl.preview.SetText(text)
}

func retentionString(r jetstream.RetentionPolicy) string {
	switch r {
	case jetstream.LimitsPolicy:
		return "Limits"
	case jetstream.InterestPolicy:
		return "Interest"
	case jetstream.WorkQueuePolicy:
		return "WorkQueue"
	default:
		return "Unknown"
	}
}
