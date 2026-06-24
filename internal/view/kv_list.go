package view

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/atterpac/dado/binding"
	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/atterpac/dado/theme"
	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
)

// KVList displays all Key-Value store buckets.
type KVList struct {
	*components.MasterDetailView
	app *App

	table   *components.Table
	preview *core.TextView

	binding *binding.TableBinding[jetstream.KeyValueStatus]
}

// NewKVList creates the KV bucket list view.
func NewKVList(app *App) *KVList {
	kl := &KVList{
		app: app,
	}

	kl.table = components.NewTable().
		SetHeaders("BUCKET", "KEYS", "BYTES", "HISTORY", "TTL", "COMPRESSED").
		ConfigureEmpty(theme.IconKey, "No KV Stores", "")

	kl.preview = core.NewTextView().
		SetDynamicColors(true)

	// Set up reactive table binding
	kl.binding = binding.NewTableBinding[jetstream.KeyValueStatus](kl.table).
		SetMapper(func(s jetstream.KeyValueStatus) []string {
			ttl := "-"
			if s.TTL() > 0 {
				ttl = s.TTL().String()
			}
			compressed := "No"
			if s.IsCompressed() {
				compressed = "Yes"
			}
			return []string{
				s.Bucket(),
				formatNumber(s.Values()),
				formatBytes(s.Bytes()),
				fmt.Sprintf("%d", s.History()),
				ttl,
				compressed,
			}
		}).
		SetKeyMapper(func(s jetstream.KeyValueStatus) string {
			return s.Bucket()
		}).
		SetFetcher(func() ([]jetstream.KeyValueStatus, error) {
			provider := kl.app.Provider()
			if provider == nil {
				return nil, fmt.Errorf("no provider")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return provider.ListKeyValueStores(ctx)
		}).
		SetRefreshInterval(10 * time.Second).
		SetOnSelect(func(s jetstream.KeyValueStatus) {
			kl.app.NavigateToKVDetail(s.Bucket())
		}).
		SetOnRefresh(func(data []jetstream.KeyValueStatus, err error) {
			if err != nil {
				kl.app.QueueUpdateDraw(func() {
					kl.preview.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
				})
				return
			}
			if len(data) > 0 {
				kl.app.QueueUpdateDraw(func() {
					row, _ := kl.table.GetSelection()
					if row < 1 {
						row = 1
					}
					kl.updatePreview(row)
				})
			}
		})

	kl.table.SetSelectionChangedFunc(func(row, col int) {
		kl.updatePreview(row)
	})

	kl.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("KV Stores").
		SetDetailTitle("Preview").
		SetMasterContent(kl.table).
		SetDetailContent(kl.preview).
		SetRatio(0.6)

	return kl
}

func (kl *KVList) CommandContext() CommandViewContext {
	if s, ok := kl.binding.GetSelectedValue(); ok {
		return CommandViewContext{Bucket: s.Bucket()}
	}
	return CommandViewContext{}
}

func (kl *KVList) Name() string { return "KV Stores" }

func (kl *KVList) Start() {
	kl.binding.Start()
}

func (kl *KVList) Stop() {
	kl.binding.Stop()
}

func (kl *KVList) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "Browse Keys"},
		{Key: "c", Description: "Create"},
		{Key: "d", Description: "Delete"},
		{Key: "y", Description: "Yank"},
		{Key: "w", Description: "Watch"},
		{Key: "Space", Description: "Select"},
		{Key: "D", Description: "Bulk Delete"},
		{Key: "p", Description: "Preview"},
		{Key: "r", Description: "Refresh"},
	}
}

func (kl *KVList) HandleKey(event *tcell.EventKey) bool {
	switch event.Rune() {
	case 'c':
		showKVCreateForm(kl.app, func() {
			kl.binding.RefreshAsync()
		})
		return true
	case 'd':
		if s, ok := kl.binding.GetSelectedValue(); ok {
			bucket := s.Bucket()
			ConfirmDelete(kl.app, "KV bucket", bucket, func() {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := kl.app.Provider().DeleteKeyValue(ctx, bucket); err != nil {
						kl.app.ShowError(err.Error())
					} else {
						kl.app.ShowSuccess("Deleted KV bucket: " + bucket)
						kl.binding.RefreshAsync()
					}
				}()
			})
		}
		return true
	case 'y':
		if s, ok := kl.binding.GetSelectedValue(); ok {
			info := map[string]interface{}{
				"bucket":     s.Bucket(),
				"keys":       s.Values(),
				"bytes":      s.Bytes(),
				"history":    s.History(),
				"ttl":        s.TTL().String(),
				"compressed": s.IsCompressed(),
			}
			data, err := json.MarshalIndent(info, "", "  ")
			if err != nil {
				kl.app.ShowError(err.Error())
			} else if err := clipboard.Copy(string(data)); err != nil {
				kl.app.ShowError("Clipboard: " + err.Error())
			} else {
				kl.app.ShowSuccess("Copied KV status: " + s.Bucket())
			}
		}
		return true
	case 'D':
		kl.bulkDelete()
		return true
	case 'w':
		if s, ok := kl.binding.GetSelectedValue(); ok {
			kl.app.NavigateToKVWatch(s.Bucket())
		}
		return true
	case 'p':
		kl.ToggleDetail()
		return true
	case 'r':
		kl.binding.RefreshAsync()
		return true
	}
	return kl.MasterDetailView.HandleKey(event)
}

func (kl *KVList) bulkDelete() {
	keys := kl.table.GetSelectedKeys()
	if len(keys) == 0 {
		kl.app.ShowInfo("No KV buckets selected (use Space to select)")
		return
	}
	label := fmt.Sprintf("%d KV buckets", len(keys))
	ConfirmDelete(kl.app, "bulk", label, func() {
		go func() {
			for _, bucket := range keys {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := kl.app.Provider().DeleteKeyValue(ctx, bucket); err != nil {
					kl.app.ShowError(fmt.Sprintf("Delete %s: %s", bucket, err))
				}
				cancel()
			}
			kl.app.ShowSuccess(fmt.Sprintf("Deleted %d KV buckets", len(keys)))
			kl.table.ClearSelection()
			kl.binding.RefreshAsync()
		}()
	})
}

func (kl *KVList) updatePreview(row int) {
	s, ok := kl.binding.GetItemValue(row)
	if !ok {
		kl.preview.SetText("")
		return
	}
	dim := theme.TagFgDim()

	ttl := "none"
	if s.TTL() > 0 {
		ttl = s.TTL().String()
	}

	text := fmt.Sprintf(
		"[%s]Bucket:[-]      %s\n"+
			"[%s]History:[-]     %d\n"+
			"[%s]TTL:[-]         %s\n"+
			"[%s]Compressed:[-]  %v\n"+
			"\n"+
			"[%s]Keys:[-]        %s\n"+
			"[%s]Bytes:[-]       %s",
		dim, s.Bucket(),
		dim, s.History(),
		dim, ttl,
		dim, s.IsCompressed(),
		dim, formatNumber(s.Values()),
		dim, formatBytes(s.Bytes()),
	)

	kl.preview.SetText(text)
}
