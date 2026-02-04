package view

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

// KVDetail is the key browser for a KV bucket.
type KVDetail struct {
	*components.MasterDetailView
	app    *App
	bucket string

	keyTable  *components.Table
	valueView *tview.TextView

	kv      jetstream.KeyValue
	binding *binding.TableBinding[string]

	stopRefresh chan struct{}
	stopped     int32
}

// NewKVDetail creates the KV key browser view.
func NewKVDetail(app *App, bucket string) *KVDetail {
	kd := &KVDetail{
		app:         app,
		bucket:      bucket,
		stopRefresh: make(chan struct{}),
	}

	kd.keyTable = components.NewTable()

	kd.valueView = tview.NewTextView().
		SetDynamicColors(true)

	// Set up reactive table binding for keys
	kd.binding = binding.NewTableBinding[string](kd.keyTable).
		SetMapper(func(key string) []string {
			return []string{key}
		}).
		SetKeyMapper(func(key string) string {
			return key
		}).
		SetOnRefresh(func(data []string, err error) {
			if err != nil {
				kd.app.QueueUpdateDraw(func() {
					kd.valueView.SetText(fmt.Sprintf("[red]Error listing keys: %v[-]", err))
				})
			}
		})

	kd.keyTable.SetSelectionChangedFunc(func(row, col int) {
		kd.loadValue(row)
	})

	kd.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle(fmt.Sprintf("Keys: %s", bucket)).
		SetDetailTitle("Value").
		SetMasterContent(kd.keyTable).
		SetDetailContent(kd.valueView).
		SetRatio(0.4).
		ConfigureEmpty("󰋼", "No Keys", "No keys found in this bucket")

	return kd
}

func (kd *KVDetail) Name() string { return kd.bucket }

func (kd *KVDetail) Start() {
	go kd.initBucket()
}

func (kd *KVDetail) Stop() {
	atomic.StoreInt32(&kd.stopped, 1)
	select {
	case kd.stopRefresh <- struct{}{}:
	default:
	}
}

func (kd *KVDetail) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "View"},
		{Key: "e", Description: "Edit"},
		{Key: "d", Description: "Delete"},
		{Key: "r", Description: "Refresh"},
	}
}

func (kd *KVDetail) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return kd.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'r':
			go kd.loadKeys()
		default:
			if handler := kd.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (kd *KVDetail) initBucket() {
	provider := kd.app.Provider()
	if provider == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		kv, err := provider.GetKeyValue(ctx, kd.bucket)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&kd.stopped) == 1 {
			return
		}

		if err != nil {
			kd.app.QueueUpdateDraw(func() {
				kd.valueView.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
			})
			return
		}

		kd.kv = kv
		kd.loadKeys()
	}()
}

func (kd *KVDetail) loadKeys() {
	if kd.kv == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		keys, err := kd.kv.Keys(ctx)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&kd.stopped) == 1 {
			return
		}

		if err != nil {
			kd.app.QueueUpdateDraw(func() {
				kd.valueView.SetText(fmt.Sprintf("[red]Error listing keys: %v[-]", err))
			})
			return
		}

		kd.binding.SetData(keys)
	}()
}

func (kd *KVDetail) loadValue(row int) {
	key, ok := kd.binding.GetItemValue(row)
	if !ok || kd.kv == nil {
		kd.valueView.SetText("")
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		entry, err := kd.kv.Get(ctx, key)
		if err != nil {
			kd.app.QueueUpdateDraw(func() {
				kd.valueView.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
			})
			return
		}

		kd.app.QueueUpdateDraw(func() {
			kd.renderValue(entry)
		})
	}()
}

func (kd *KVDetail) renderValue(entry jetstream.KeyValueEntry) {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	op := "PUT"
	switch entry.Operation() {
	case jetstream.KeyValueDelete:
		op = "DELETE"
	case jetstream.KeyValuePurge:
		op = "PURGE"
	}

	// Format value — detect JSON
	value := string(entry.Value())
	var prettyJSON json.RawMessage
	if json.Unmarshal(entry.Value(), &prettyJSON) == nil {
		if formatted, err := json.MarshalIndent(prettyJSON, "", "  "); err == nil {
			value = string(formatted)
		}
	}

	text := fmt.Sprintf(
		"[%s]Key:[-]       [%s]%s[-]\n"+
			"[%s]Revision:[-]  %d\n"+
			"[%s]Created:[-]   %s\n"+
			"[%s]Operation:[-] %s\n"+
			"[%s]───────────────────────[-]\n"+
			"%s",
		dim, accent, entry.Key(),
		dim, entry.Revision(),
		dim, entry.Created().Format(time.RFC3339),
		dim, op,
		dim,
		value,
	)

	kd.valueView.SetText(text)
}
