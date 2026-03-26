package view

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/atterpac/jig/validators"
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

	refreshCancel context.CancelFunc
	stopped       int32
}

// NewKVDetail creates the KV key browser view.
func NewKVDetail(app *App, bucket string) *KVDetail {
	kd := &KVDetail{
		app:         app,
		bucket:      bucket,
		refreshCancel: func() {},
	}

	kd.keyTable = components.NewTable().SetHeaders("KEY").
		ConfigureEmpty(theme.IconKey, "No Keys", "")

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
		SetRatio(0.4)

	return kd
}

func (kd *KVDetail) CommandContext() CommandViewContext {
	ctx := CommandViewContext{Bucket: kd.bucket}
	if key, ok := kd.binding.GetSelectedValue(); ok {
		ctx.Key = key
	}
	return ctx
}

func (kd *KVDetail) Name() string { return kd.bucket }

func (kd *KVDetail) Start() {
	atomic.StoreInt32(&kd.stopped, 0)
	kd.refreshCancel()
	_, cancel := context.WithCancel(context.Background())
	kd.refreshCancel = cancel
	go kd.initBucket()
}

func (kd *KVDetail) Stop() {
	atomic.StoreInt32(&kd.stopped, 1)
	kd.refreshCancel()
}

func (kd *KVDetail) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "View"},
		{Key: "y", Description: "Yank"},
		{Key: "h", Description: "History"},
		{Key: "c", Description: "Create key"},
		{Key: "e", Description: "Edit"},
		{Key: "d", Description: "Delete"},
		{Key: "w", Description: "Watch"},
		{Key: "p", Description: "Preview"},
		{Key: "r", Description: "Refresh"},
	}
}

func (kd *KVDetail) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return kd.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'y':
			kd.yankValue()
		case event.Rune() == 'h':
			go kd.showHistory()
		case event.Rune() == 'c':
			kd.createKey()
		case event.Rune() == 'e':
			kd.editKey()
		case event.Rune() == 'w':
			kd.app.NavigateToKVWatch(kd.bucket)
		case event.Rune() == 'd':
			kd.deleteKey()
		case event.Rune() == 'p':
			kd.ToggleDetail()
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

		provider := kd.app.Provider()
		if provider == nil {
			return
		}

		keys, err := provider.ListKeyValueKeys(ctx, kd.bucket)

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
		kd.app.QueueUpdateDraw(func() {
			if len(keys) > 0 {
				kd.loadValue(1)
			}
		})
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

func (kd *KVDetail) showHistory() {
	key, ok := kd.binding.GetSelectedValue()
	if !ok || kd.kv == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	entries, err := kd.kv.History(ctx, key)
	if err != nil {
		kd.app.QueueUpdateDraw(func() {
			kd.app.ShowError("History: " + err.Error())
		})
		return
	}

	if atomic.LoadInt32(&kd.stopped) == 1 {
		return
	}

	kd.app.QueueUpdateDraw(func() {
		table := components.NewTable().
			SetHeaders("REV", "OP", "TIME", "SIZE")

		for _, e := range entries {
			op := "PUT"
			switch e.Operation() {
			case jetstream.KeyValueDelete:
				op = "DEL"
			case jetstream.KeyValuePurge:
				op = "PURGE"
			}
			table.AddRow(
				fmt.Sprintf("%d", e.Revision()),
				op,
				e.Created().Format("15:04:05"),
				formatBytes(uint64(len(e.Value()))),
			)
		}

		// Enable Enter to diff selected revision with previous
		table.SetSelectedFunc(func(row, col int) {
			idx := row - 1 // header offset
			if idx < 0 || idx >= len(entries) {
				return
			}
			if idx+1 >= len(entries) {
				kd.app.ShowInfo("No previous revision to diff against")
				return
			}
			newEntry := entries[idx]
			oldEntry := entries[idx+1]
			kd.showRevisionDiff(oldEntry, newEntry)
		})

		modal := components.NewModal(components.ModalConfig{
			Title:  fmt.Sprintf("History: %s (%d revisions)", key, len(entries)),
			Width:  60,
			Height: 16,
		}).SetContent(table)

		modal.SetHints([]components.KeyHint{
			{Key: "Enter", Description: "Diff"},
			{Key: "Esc", Description: "Close"},
		})

		kd.app.app.Pages().Push(modal)
	})
}

func (kd *KVDetail) yankValue() {
	key, ok := kd.binding.GetSelectedValue()
	if !ok || kd.kv == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		entry, err := kd.kv.Get(ctx, key)
		if err != nil {
			kd.app.ShowError("Failed to load key: " + err.Error())
			return
		}
		if err := clipboard.Copy(string(entry.Value())); err != nil {
			kd.app.ShowError("Clipboard: " + err.Error())
		} else {
			kd.app.ShowSuccess(fmt.Sprintf("Copied %s", formatBytes(uint64(len(entry.Value())))))
		}
	}()
}

func (kd *KVDetail) createKey() {
	if kd.kv == nil {
		kd.app.ShowError("Bucket not initialized")
		return
	}

	modal := components.NewFormBuilder().
		Text("key", "Key").
		Placeholder("my-key").
		Validate(validators.Required()).
		Done().
		TextArea("value", "Value").
		Placeholder("value").
		Done().
		OnSubmit(func(values map[string]any) {
			key := getString(values, "key")
			if key == "" {
				kd.app.ShowError("Key is required")
				return
			}
			value := getString(values, "value")
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := kd.kv.Put(ctx, key, []byte(value)); err != nil {
					kd.app.ShowError(err.Error())
				} else {
					kd.app.ShowSuccess("Created key: " + key)
					go kd.loadKeys()
				}
			}()
		}).
		AsFormModal(fmt.Sprintf("Create Key: %s", kd.bucket), 60, 14)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Create"},
		{Key: "Esc", Description: "Cancel"},
	})

	kd.app.app.Pages().Push(modal)
}

func (kd *KVDetail) editKey() {
	key, ok := kd.binding.GetSelectedValue()
	if !ok || kd.kv == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		entry, err := kd.kv.Get(ctx, key)
		if err != nil {
			kd.app.ShowError("Failed to load key: " + err.Error())
			return
		}

		currentValue := string(entry.Value())

		kd.app.QueueUpdateDraw(func() {
			modal := components.NewFormBuilder().
				TextArea("value", "Value").Value(currentValue).Done().
				OnSubmit(func(values map[string]any) {
					newValue := getString(values, "value")
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if _, err := kd.kv.Put(ctx, key, []byte(newValue)); err != nil {
							kd.app.ShowError(err.Error())
						} else {
							kd.app.ShowSuccess("Updated key: " + key)
							go kd.loadKeys()
						}
					}()
				}).
				AsFormModal(fmt.Sprintf("Edit Key: %s", key), 60, 16)

			modal.SetHints([]components.KeyHint{
				{Key: "Ctrl+S", Description: "Save"},
				{Key: "Esc", Description: "Cancel"},
			})

			kd.app.app.Pages().Push(modal)
		})
	}()
}

func (kd *KVDetail) deleteKey() {
	key, ok := kd.binding.GetSelectedValue()
	if !ok || kd.kv == nil {
		return
	}

	bucket := kd.bucket
	ConfirmDelete(kd.app, "KV key", key, func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := kd.kv.Delete(ctx, key); err != nil {
				kd.app.ShowError(err.Error())
			} else {
				kd.app.ShowSuccess(fmt.Sprintf("Deleted key: %s from %s", key, bucket))
				go kd.loadKeys()
			}
		}()
	})
}

func (kd *KVDetail) showRevisionDiff(oldEntry, newEntry jetstream.KeyValueEntry) {
	oldVal := string(oldEntry.Value())
	newVal := string(newEntry.Value())

	// Pretty-print JSON if possible
	if formatted := formatJSONPretty(oldVal); formatted != oldVal {
		oldVal = formatted
	}
	if formatted := formatJSONPretty(newVal); formatted != newVal {
		newVal = formatted
	}

	diff := components.NewDiffViewer().
		SetDiff(oldVal, newVal).
		SetTitle(fmt.Sprintf("Rev %d → %d", oldEntry.Revision(), newEntry.Revision())).
		SetShowLineNumbers(true)

	modal := components.NewModal(components.ModalConfig{
		Title:    fmt.Sprintf("Diff: Rev %d → %d", oldEntry.Revision(), newEntry.Revision()),
		Width:    80,
		Height:   24,
		Backdrop: true,
	}).SetContent(diff)

	modal.SetHints([]components.KeyHint{
		{Key: "j/k", Description: "Scroll"},
		{Key: "Esc", Description: "Close"},
	})

	kd.app.app.Pages().Push(modal)
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
