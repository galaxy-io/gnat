package view

import (
	"context"
	"fmt"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

// KVList displays all Key-Value store buckets.
type KVList struct {
	*components.MasterDetailView
	app *App

	table   *components.Table
	preview *tview.TextView

	binding *binding.TableBinding[jetstream.KeyValueStatus]
}

// NewKVList creates the KV bucket list view.
func NewKVList(app *App) *KVList {
	kl := &KVList{
		app: app,
	}

	kl.table = components.NewTable().
		SetHeaders("BUCKET", "KEYS", "BYTES", "HISTORY", "TTL", "COMPRESSED")

	kl.preview = tview.NewTextView().
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
		SetRatio(0.6).
		ConfigureEmpty("󰋼", "No KV Stores", "No key-value stores found")

	return kl
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
		{Key: "r", Description: "Refresh"},
	}
}

func (kl *KVList) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return kl.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'r':
			kl.binding.RefreshAsync()
		default:
			if handler := kl.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
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
