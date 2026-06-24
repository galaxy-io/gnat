package view

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/atterpac/dado/binding"
	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/atterpac/dado/theme"
	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamList displays all JetStream streams in a master-detail layout.
type StreamList struct {
	*components.MasterDetailView
	app *App

	table   *components.Table
	preview *core.TextView

	binding *binding.TableBinding[*jetstream.StreamInfo]

	// Growth rate tracking
	prevMsgs  map[string]uint64
	prevBytes map[string]uint64
	prevTime  time.Time
	rates     map[string][2]float64 // [msgs/s, bytes/s]

	// Rolling rate history for sparklines
	msgRateHistory  map[string][]float64
	byteRateHistory map[string][]float64
}

// NewStreamList creates the stream list view.
func NewStreamList(app *App) *StreamList {
	sl := &StreamList{
		app: app,
	}

	sl.table = components.NewTable().
		SetHeaders("NAME", "MSGS", "BYTES", "CONSUMERS", "STORAGE", "REPLICAS").
		ConfigureEmpty(theme.IconDatabase, "No Streams", "")

	sl.preview = core.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(core.AlignLeft)

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
				return
			}
			// Compute growth rates
			now := time.Now()
			if sl.msgRateHistory == nil {
				sl.msgRateHistory = make(map[string][]float64)
				sl.byteRateHistory = make(map[string][]float64)
			}
			if sl.prevMsgs != nil && !sl.prevTime.IsZero() {
				elapsed := now.Sub(sl.prevTime).Seconds()
				if elapsed > 0 {
					sl.rates = make(map[string][2]float64)
					for _, s := range data {
						name := s.Config.Name
						if prevM, ok := sl.prevMsgs[name]; ok {
							msgRate := float64(s.State.Msgs-prevM) / elapsed
							byteRate := float64(s.State.Bytes-sl.prevBytes[name]) / elapsed
							sl.rates[name] = [2]float64{msgRate, byteRate}
							sl.msgRateHistory[name] = append(sl.msgRateHistory[name], msgRate)
							sl.byteRateHistory[name] = append(sl.byteRateHistory[name], byteRate)
							if len(sl.msgRateHistory[name]) > 60 {
								sl.msgRateHistory[name] = sl.msgRateHistory[name][len(sl.msgRateHistory[name])-60:]
							}
							if len(sl.byteRateHistory[name]) > 60 {
								sl.byteRateHistory[name] = sl.byteRateHistory[name][len(sl.byteRateHistory[name])-60:]
							}
						}
					}
				}
			}
			sl.prevMsgs = make(map[string]uint64)
			sl.prevBytes = make(map[string]uint64)
			current := make(map[string]struct{}, len(data))
			for _, s := range data {
				sl.prevMsgs[s.Config.Name] = s.State.Msgs
				sl.prevBytes[s.Config.Name] = s.State.Bytes
				current[s.Config.Name] = struct{}{}
			}
			// Prune history for deleted streams.
			for name := range sl.msgRateHistory {
				if _, ok := current[name]; !ok {
					delete(sl.msgRateHistory, name)
					delete(sl.byteRateHistory, name)
					delete(sl.rates, name)
				}
			}
			sl.prevTime = now
			sl.app.QueueUpdateDraw(func() {
				row, _ := sl.table.GetSelection()
				sl.updatePreview(row)
			})
		})

	sl.table.SetSelectionChangedFunc(func(row, col int) {
		sl.updatePreview(row)
	})

	sl.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Streams").
		SetDetailTitle("Preview").
		SetMasterContent(sl.table).
		SetDetailContent(sl.preview).
		SetRatio(0.6)

	return sl
}

func (sl *StreamList) CommandContext() CommandViewContext {
	if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
		return CommandViewContext{Stream: s.Config.Name}
	}
	return CommandViewContext{}
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
		{Key: "b", Description: "Browse"},
		{Key: "c", Description: "Create"},
		{Key: "e", Description: "Edit"},
		{Key: "d", Description: "Delete"},
		{Key: "Space", Description: "Select"},
		{Key: "D", Description: "Bulk Delete"},
		{Key: "P", Description: "Bulk Purge"},
		{Key: "y", Description: "Yank"},
		{Key: "p", Description: "Preview"},
		{Key: "r", Description: "Refresh"},
	}
}

func (sl *StreamList) HandleKey(event *tcell.EventKey) bool {
	switch {
	case event.Key() == tcell.KeyEnter:
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil && len(s.Config.Subjects) > 0 {
			sl.app.NavigateToMessageMonitorWithSubject(s.Config.Subjects[0])
		}
		return true
	case event.Rune() == 'v':
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
			sl.app.NavigateToStreamDetail(s.Config.Name)
		}
		return true
	case event.Rune() == 'n':
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
			sl.app.NavigateToConsumers(s.Config.Name)
		}
		return true
	case event.Rune() == 'b':
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
			sl.app.NavigateToMessageBrowser(s.Config.Name)
		}
		return true
	case event.Rune() == 'c':
		showStreamCreateForm(sl.app, func() {
			sl.binding.RefreshAsync()
		})
		return true
	case event.Rune() == 'd':
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
			name := s.Config.Name
			ConfirmDelete(sl.app, "stream", name, func() {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := sl.app.Provider().DeleteStream(ctx, name); err != nil {
						sl.app.ShowError(err.Error())
					} else {
						sl.app.ShowSuccess("Deleted stream: " + name)
						sl.binding.RefreshAsync()
					}
				}()
			})
		}
		return true
	case event.Rune() == 'e':
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
			showStreamEditForm(sl.app, s, func() {
				sl.binding.RefreshAsync()
			})
		}
		return true
	case event.Rune() == 'y':
		if s, ok := sl.binding.GetSelectedValue(); ok && s != nil {
			data, err := json.MarshalIndent(s.Config, "", "  ")
			if err != nil {
				sl.app.ShowError(err.Error())
			} else if err := clipboard.Copy(string(data)); err != nil {
				sl.app.ShowError("Clipboard: " + err.Error())
			} else {
				sl.app.ShowSuccess("Copied stream config: " + s.Config.Name)
			}
		}
		return true
	case event.Rune() == 'D':
		sl.bulkDelete()
		return true
	case event.Rune() == 'P' && event.Modifiers() == 0:
		sl.bulkPurge()
		return true
	case event.Rune() == 'p':
		sl.ToggleDetail()
		return true
	case event.Rune() == 'r':
		sl.binding.RefreshAsync()
		return true
	case event.Rune() == '/':
		sl.ShowSearch()
		return true
	}

	if sl.HandleSearchKey(event) {
		return true
	}
	return sl.MasterDetailView.HandleKey(event)
}

func (sl *StreamList) updatePreview(row int) {
	s, ok := sl.binding.GetItemValue(row)
	if !ok || s == nil {
		sl.preview.SetText("")
		return
	}
	dim := theme.TagFgDim()
	accent := theme.TagAccent()
	warn := theme.TagWarning()

	var b strings.Builder

	// ── Identity ──
	fmt.Fprintf(&b, "[%s]Name:[-]        [%s]%s[-]\n", dim, accent, s.Config.Name)
	if s.Config.Description != "" {
		fmt.Fprintf(&b, "[%s]Description:[-] %s\n", dim, s.Config.Description)
	}
	fmt.Fprintf(&b, "[%s]Subjects:[-]    %s\n", dim, strings.Join(s.Config.Subjects, ", "))
	fmt.Fprintf(&b, "[%s]Created:[-]     %s\n", dim, s.Created.Format("2006-01-02 15:04:05"))

	// ── Configuration ──
	fmt.Fprintf(&b, "\n[%s]── Config ──[-]\n", dim)

	storage := "File"
	if s.Config.Storage == jetstream.MemoryStorage {
		storage = "Memory"
	}
	fmt.Fprintf(&b, "[%s]Retention:[-]   %s\n", dim, retentionString(s.Config.Retention))
	fmt.Fprintf(&b, "[%s]Storage:[-]     %s\n", dim, storage)
	fmt.Fprintf(&b, "[%s]Replicas:[-]    %d\n", dim, s.Config.Replicas)
	fmt.Fprintf(&b, "[%s]Discard:[-]     %s\n", dim, discardString(s.Config.Discard))

	if s.Config.MaxMsgs > 0 {
		fmt.Fprintf(&b, "[%s]Max Msgs:[-]    %s\n", dim, formatNumber(uint64(s.Config.MaxMsgs)))
	} else {
		fmt.Fprintf(&b, "[%s]Max Msgs:[-]    unlimited\n", dim)
	}
	if s.Config.MaxBytes > 0 {
		fmt.Fprintf(&b, "[%s]Max Bytes:[-]   %s\n", dim, formatBytes(uint64(s.Config.MaxBytes)))
	} else {
		fmt.Fprintf(&b, "[%s]Max Bytes:[-]   unlimited\n", dim)
	}
	if s.Config.MaxAge > 0 {
		fmt.Fprintf(&b, "[%s]Max Age:[-]     %s\n", dim, s.Config.MaxAge.String())
	} else {
		fmt.Fprintf(&b, "[%s]Max Age:[-]     unlimited\n", dim)
	}
	if s.Config.MaxMsgsPerSubject > 0 {
		fmt.Fprintf(&b, "[%s]Max/Subject:[-] %s\n", dim, formatNumber(uint64(s.Config.MaxMsgsPerSubject)))
	}
	if s.Config.MaxMsgSize > 0 {
		fmt.Fprintf(&b, "[%s]Max Msg Size:[-] %s\n", dim, formatBytes(uint64(s.Config.MaxMsgSize)))
	}
	if s.Config.Duplicates > 0 {
		fmt.Fprintf(&b, "[%s]Dedup Window:[-] %s\n", dim, s.Config.Duplicates.String())
	}

	// ── State ──
	fmt.Fprintf(&b, "\n[%s]── State ──[-]\n", dim)
	fmt.Fprintf(&b, "[%s]Messages:[-]    [%s]%s[-]\n", dim, accent, formatNumber(s.State.Msgs))
	fmt.Fprintf(&b, "[%s]Bytes:[-]       [%s]%s[-]\n", dim, accent, formatBytes(s.State.Bytes))
	fmt.Fprintf(&b, "[%s]Consumers:[-]   %d\n", dim, s.State.Consumers)
	fmt.Fprintf(&b, "[%s]Subjects:[-]    %d\n", dim, s.State.NumSubjects)
	fmt.Fprintf(&b, "[%s]First Seq:[-]   %d\n", dim, s.State.FirstSeq)
	fmt.Fprintf(&b, "[%s]Last Seq:[-]    %d\n", dim, s.State.LastSeq)

	if !s.State.LastTime.IsZero() {
		fmt.Fprintf(&b, "[%s]Last Active:[-] %s ago\n", dim, time.Since(s.State.LastTime).Round(time.Second))
	}
	if sl.rates != nil {
		if rate, ok := sl.rates[s.Config.Name]; ok && (rate[0] > 0.1 || rate[1] > 0.1) {
			fmt.Fprintf(&b, "[%s]Msg Rate:[-]    [%s]%.1f msg/s[-]\n", dim, accent, rate[0])
			fmt.Fprintf(&b, "[%s]Byte Rate:[-]   [%s]%s/s[-]\n", dim, accent, formatBytes(uint64(rate[1])))
		}
	}
	if h := sl.msgRateHistory[s.Config.Name]; len(h) > 1 {
		fmt.Fprintf(&b, "\n[%s]── Rate History ──[-]\n", dim)
		fmt.Fprintf(&b, "[%s]Msgs/s:[-]  [%s]%s[-]\n", dim, accent, miniSparkline(h, 30))
		if bh := sl.byteRateHistory[s.Config.Name]; len(bh) > 1 {
			fmt.Fprintf(&b, "[%s]Bytes/s:[-] [%s]%s[-]\n", dim, accent, miniSparkline(bh, 30))
		}
	}
	if s.State.NumDeleted > 0 {
		fmt.Fprintf(&b, "[%s]Deleted:[-]     [%s]%d[-]\n", dim, warn, s.State.NumDeleted)
	}

	// ── Flags ──
	var flags []string
	if s.Config.Sealed {
		flags = append(flags, "Sealed")
	}
	if s.Config.DenyDelete {
		flags = append(flags, "DenyDelete")
	}
	if s.Config.DenyPurge {
		flags = append(flags, "DenyPurge")
	}
	if s.Config.AllowRollup {
		flags = append(flags, "AllowRollup")
	}
	if s.Config.AllowDirect {
		flags = append(flags, "AllowDirect")
	}
	if s.Config.NoAck {
		flags = append(flags, "NoAck")
	}
	if len(flags) > 0 {
		fmt.Fprintf(&b, "\n[%s]── Flags ──[-]\n", dim)
		fmt.Fprintf(&b, "%s\n", strings.Join(flags, ", "))
	}

	// ── Sources / Mirror ──
	if s.Mirror != nil {
		fmt.Fprintf(&b, "\n[%s]── Mirror ──[-]\n", dim)
		fmt.Fprintf(&b, "[%s]Source:[-]      [%s]%s[-]\n", dim, accent, s.Mirror.Name)
		fmt.Fprintf(&b, "[%s]Lag:[-]         %d\n", dim, s.Mirror.Lag)
	}
	if len(s.Sources) > 0 {
		fmt.Fprintf(&b, "\n[%s]── Sources ──[-]\n", dim)
		for _, src := range s.Sources {
			fmt.Fprintf(&b, "  [%s]%s[-] (lag: %d)\n", accent, src.Name, src.Lag)
		}
	}

	// ── Cluster ──
	if s.Cluster != nil {
		fmt.Fprintf(&b, "\n[%s]── Cluster ──[-]\n", dim)
		fmt.Fprintf(&b, "[%s]Name:[-]        [%s]%s[-]\n", dim, accent, s.Cluster.Name)
		fmt.Fprintf(&b, "[%s]Leader:[-]      [%s]%s[-]\n", dim, accent, s.Cluster.Leader)
		if len(s.Cluster.Replicas) > 0 {
			for _, r := range s.Cluster.Replicas {
				status := "[green]current[-]"
				if r.Offline {
					status = "[red]offline[-]"
				} else if !r.Current {
					status = fmt.Sprintf("[%s]lag: %d[-]", warn, r.Lag)
				}
				fmt.Fprintf(&b, "  [%s]%s[-] %s\n", dim, r.Name, status)
			}
		}
	}

	sl.preview.SetText(b.String())
}

func (sl *StreamList) bulkDelete() {
	keys := sl.table.GetSelectedKeys()
	if len(keys) == 0 {
		sl.app.ShowInfo("No streams selected (use Space to select)")
		return
	}
	label := fmt.Sprintf("%d streams", len(keys))
	ConfirmDelete(sl.app, "bulk", label, func() {
		go func() {
			for _, name := range keys {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := sl.app.Provider().DeleteStream(ctx, name); err != nil {
					sl.app.ShowError(fmt.Sprintf("Delete %s: %s", name, err))
				}
				cancel()
			}
			sl.app.ShowSuccess(fmt.Sprintf("Deleted %d streams", len(keys)))
			sl.table.ClearSelection()
			sl.binding.RefreshAsync()
		}()
	})
}

func (sl *StreamList) bulkPurge() {
	keys := sl.table.GetSelectedKeys()
	if len(keys) == 0 {
		sl.app.ShowInfo("No streams selected (use Space to select)")
		return
	}
	label := fmt.Sprintf("%d streams", len(keys))
	ConfirmDelete(sl.app, "purge", label, func() {
		go func() {
			for _, name := range keys {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := sl.app.Provider().PurgeStream(ctx, name); err != nil {
					sl.app.ShowError(fmt.Sprintf("Purge %s: %s", name, err))
				}
				cancel()
			}
			sl.app.ShowSuccess(fmt.Sprintf("Purged %d streams", len(keys)))
			sl.table.ClearSelection()
			sl.binding.RefreshAsync()
		}()
	})
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
