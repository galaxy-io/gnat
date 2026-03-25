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

	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/galaxy-io/gnat/internal/nats"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const maxWatchEntries = 500 // hard cap on retained watch events

type kvWatchState struct {
	entries []nats.KVWatchEvent
	count   int
}

// KVWatch shows real-time key changes for a KV bucket.
type KVWatch struct {
	*components.MasterDetailView
	app    *App
	bucket string

	table   *components.Table
	preview *tview.TextView

	state *binding.Value[kvWatchState]

	mu      sync.Mutex
	watcher nats.KVWatcher

	stopped       int32
	processCancel context.CancelFunc
}

// NewKVWatch creates the KV watch view.
func NewKVWatch(app *App, bucket string) *KVWatch {
	kw := &KVWatch{
		app:         app,
		bucket:      bucket,
		processCancel: func() {},
	}

	kw.table = components.NewTable().
		SetHeaders("TIME", "KEY", "OP", "REV", "SIZE").
		ConfigureEmpty(theme.IconSignal, "Watching...", "Waiting for changes")

	kw.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	kw.preview.SetBackgroundColor(theme.Bg())
	theme.Register(kw.preview)

	kw.table.SetSelectionChangedFunc(func(row, col int) {
		kw.renderPreview(row - 1)
	})

	kw.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle(fmt.Sprintf("Watch: %s", bucket)).
		SetDetailTitle("Value").
		SetMasterContent(kw.table).
		SetDetailContent(kw.preview).
		SetRatio(0.5)

	kw.state = binding.NewValue(kvWatchState{})
	kw.state.BindToWithDraw(func(s kvWatchState) {
		kw.renderState(s)
	})

	return kw
}

func (kw *KVWatch) CommandContext() CommandViewContext {
	return CommandViewContext{Bucket: kw.bucket}
}

func (kw *KVWatch) Name() string { return fmt.Sprintf("Watch: %s", kw.bucket) }

func (kw *KVWatch) Start() {
	atomic.StoreInt32(&kw.stopped, 0)
	go kw.startWatch()
}

func (kw *KVWatch) Stop() {
	atomic.StoreInt32(&kw.stopped, 1)
	kw.processCancel()
	kw.mu.Lock()
	if kw.watcher != nil {
		_ = kw.watcher.Stop()
		kw.watcher = nil
	}
	kw.mu.Unlock()
}

func (kw *KVWatch) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "y", Description: "Yank value"},
		{Key: "c", Description: "Clear"},
		{Key: "p", Description: "Preview"},
		{Key: "Esc", Description: "Back"},
	}
}

func (kw *KVWatch) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return kw.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch event.Rune() {
		case 'y':
			s := kw.state.Get()
			row, _ := kw.table.GetSelection()
			idx := row - 1
			if idx >= 0 && idx < len(s.entries) {
				actualIdx := len(s.entries) - 1 - idx
				entry := s.entries[actualIdx]
				go kw.yankValue(entry)
			}
		case 'c':
			kw.state.SetAndDraw(kvWatchState{})
		case 'p':
			kw.ToggleDetail()
		default:
			if handler := kw.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (kw *KVWatch) yankValue(evt nats.KVWatchEvent) {
	provider := kw.app.Provider()
	if provider == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := provider.GetKeyValue(ctx, kw.bucket)
	if err != nil {
		kw.app.ShowError("KV: " + err.Error())
		return
	}
	entry, err := kv.GetRevision(ctx, evt.Key, evt.Revision)
	if err != nil {
		kw.app.ShowError("Get revision: " + err.Error())
		return
	}
	if err := clipboard.Copy(string(entry.Value())); err != nil {
		kw.app.ShowError("Clipboard: " + err.Error())
	} else {
		kw.app.ShowSuccess(fmt.Sprintf("Copied value for %s", evt.Key))
	}
}

func (kw *KVWatch) startWatch() {
	provider := kw.app.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	eventCh := make(chan nats.KVWatchEvent, 100)

	watcher, err := provider.WatchKeyValue(ctx, kw.bucket, func(evt nats.KVWatchEvent) {
		if atomic.LoadInt32(&kw.stopped) == 1 {
			return
		}
		select {
		case eventCh <- evt:
		default:
		}
	})
	if err != nil {
		kw.app.QueueUpdateDraw(func() {
			kw.app.ShowError("Watch failed: " + err.Error())
		})
		return
	}

	kw.mu.Lock()
	kw.watcher = watcher
	kw.mu.Unlock()

	kw.app.QueueUpdateDraw(func() {
		kw.app.ShowSuccess(fmt.Sprintf("Watching KV bucket: %s", kw.bucket))
	})

	// Process events
	processCtx, processCancel := context.WithCancel(context.Background())
	kw.processCancel = processCancel
	go kw.processEvents(processCtx, eventCh)
}

func (kw *KVWatch) processEvents(ctx context.Context, eventCh chan nats.KVWatchEvent) {
	var entries []nats.KVWatchEvent
	batchDone := false
	batchTimer := time.NewTimer(500 * time.Millisecond)
	defer batchTimer.Stop()

	renderTicker := time.NewTicker(200 * time.Millisecond)
	defer renderTicker.Stop()

	dirty := false

	pushState := func() {
		if atomic.LoadInt32(&kw.stopped) == 1 {
			return
		}
		kw.state.SetAndDraw(kvWatchState{
			entries: entries,
			count:   len(entries),
		})
		dirty = false
	}

	for {
		select {
		case <-ctx.Done():
			return

		case evt, ok := <-eventCh:
			if !ok {
				return
			}
			entries = append(entries, evt)
			if len(entries) > maxWatchEntries {
				entries = entries[len(entries)-maxWatchEntries:]
			}
			dirty = true

			if !batchDone {
				batchTimer.Reset(500 * time.Millisecond)
				if len(entries) >= maxWatchEntries {
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

func (kw *KVWatch) renderState(s kvWatchState) {
	kw.table.ClearRows()

	if len(s.entries) == 0 {
		kw.table.ConfigureEmpty(theme.IconSignal, "Watching...", "Waiting for changes")
		return
	}

	kw.SetMasterTitle(fmt.Sprintf("Watch: %s (%d events)", kw.bucket, s.count))

	// Show newest first
	for i := len(s.entries) - 1; i >= 0; i-- {
		evt := s.entries[i]
		kw.table.AddRow(
			evt.Timestamp.Format("15:04:05.000"),
			evt.Key,
			evt.Operation,
			fmt.Sprintf("%d", evt.Revision),
			formatBytes(uint64(evt.ValueSize)),
		)
	}

	kw.table.SelectRow(0)
	kw.renderPreview(0)
}

func (kw *KVWatch) renderPreview(displayIdx int) {
	s := kw.state.Get()
	if displayIdx < 0 || displayIdx >= len(s.entries) {
		kw.preview.SetText("")
		return
	}

	actualIdx := len(s.entries) - 1 - displayIdx
	if actualIdx < 0 || actualIdx >= len(s.entries) {
		kw.preview.SetText("")
		return
	}
	evt := s.entries[actualIdx]

	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Key:[-]       [%s]%s[-]\n", dim, accent, evt.Key)
	fmt.Fprintf(&b, "[%s]Operation:[-] %s\n", dim, evt.Operation)
	fmt.Fprintf(&b, "[%s]Revision:[-]  %d\n", dim, evt.Revision)
	fmt.Fprintf(&b, "[%s]Time:[-]      %s\n", dim, evt.Timestamp.Format("2006-01-02 15:04:05.000"))
	fmt.Fprintf(&b, "[%s]Size:[-]      %s\n", dim, formatBytes(uint64(evt.ValueSize)))

	// Fetch value on-demand
	go kw.fetchAndRenderValue(evt, b.String())
}

func (kw *KVWatch) fetchAndRenderValue(evt nats.KVWatchEvent, header string) {
	if evt.Operation != "PUT" {
		kw.app.QueueUpdateDraw(func() {
			kw.preview.SetText(header + fmt.Sprintf("\n[dim](%s — no value)[-]", evt.Operation))
			kw.preview.ScrollToBeginning()
		})
		return
	}

	provider := kw.app.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kv, err := provider.GetKeyValue(ctx, kw.bucket)
	if err != nil {
		kw.app.QueueUpdateDraw(func() {
			kw.preview.SetText(header + fmt.Sprintf("\n[red]Error: %v[-]", err))
		})
		return
	}

	entry, err := kv.GetRevision(ctx, evt.Key, evt.Revision)
	if err != nil {
		kw.app.QueueUpdateDraw(func() {
			kw.preview.SetText(header + fmt.Sprintf("\n[red]Error fetching value: %v[-]", err))
		})
		return
	}

	dim := theme.TagFgDim()
	value := entry.Value()

	var vb strings.Builder
	vb.WriteString(header)
	fmt.Fprintf(&vb, "\n[%s]Value:[-]\n", dim)

	data := string(value)
	if json.Valid(value) {
		var prettyJSON bytes.Buffer
		if err := json.Indent(&prettyJSON, value, "", "  "); err == nil {
			data = prettyJSON.String()
		}
	}
	data = strings.ReplaceAll(data, "[", "[[")
	vb.WriteString(data)

	text := vb.String()
	kw.app.QueueUpdateDraw(func() {
		kw.preview.SetText(text)
		kw.preview.ScrollToBeginning()
	})
}
