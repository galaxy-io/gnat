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

// ObjectList displays all Object Store buckets.
type ObjectList struct {
	*components.MasterDetailView
	app *App

	table   *components.Table
	preview *tview.TextView

	binding *binding.TableBinding[jetstream.ObjectStoreStatus]
}

// NewObjectList creates the object store list view.
func NewObjectList(app *App) *ObjectList {
	ol := &ObjectList{
		app: app,
	}

	ol.table = components.NewTable().
		SetHeaders("BUCKET", "SIZE", "REPLICAS", "SEALED")

	ol.preview = tview.NewTextView().
		SetDynamicColors(true)

	// Set up reactive table binding
	ol.binding = binding.NewTableBinding[jetstream.ObjectStoreStatus](ol.table).
		SetMapper(func(s jetstream.ObjectStoreStatus) []string {
			sealed := "No"
			if s.Sealed() {
				sealed = "Yes"
			}
			return []string{
				s.Bucket(),
				formatBytes(s.Size()),
				fmt.Sprintf("%d", s.Replicas()),
				sealed,
			}
		}).
		SetKeyMapper(func(s jetstream.ObjectStoreStatus) string {
			return s.Bucket()
		}).
		SetFetcher(func() ([]jetstream.ObjectStoreStatus, error) {
			provider := ol.app.Provider()
			if provider == nil {
				return nil, fmt.Errorf("no provider")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return provider.ListObjectStores(ctx)
		}).
		SetOnSelect(func(s jetstream.ObjectStoreStatus) {
			ol.app.NavigateToObjectDetail(s.Bucket())
		}).
		SetOnRefresh(func(data []jetstream.ObjectStoreStatus, err error) {
			if err != nil {
				ol.app.QueueUpdateDraw(func() {
					ol.preview.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
				})
			}
		})

	ol.table.SetSelectionChangedFunc(func(row, col int) {
		ol.updatePreview(row)
	})

	ol.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Object Stores").
		SetDetailTitle("Preview").
		SetMasterContent(ol.table).
		SetDetailContent(ol.preview).
		SetRatio(0.6).
		ConfigureEmpty("󰋼", "No Object Stores", "No object stores found")

	return ol
}

func (ol *ObjectList) Name() string { return "Object Stores" }

func (ol *ObjectList) Start() {
	ol.binding.Start()
}

func (ol *ObjectList) Stop() {
	ol.binding.Stop()
}

func (ol *ObjectList) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "Browse Objects"},
		{Key: "c", Description: "Create"},
		{Key: "d", Description: "Delete"},
		{Key: "r", Description: "Refresh"},
	}
}

func (ol *ObjectList) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return ol.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'r':
			ol.binding.RefreshAsync()
		default:
			if handler := ol.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (ol *ObjectList) updatePreview(row int) {
	s, ok := ol.binding.GetItemValue(row)
	if !ok {
		ol.preview.SetText("")
		return
	}
	dim := theme.TagFgDim()

	sealed := "No"
	if s.Sealed() {
		sealed = "Yes"
	}

	text := fmt.Sprintf(
		"[%s]Bucket:[-]     %s\n"+
			"[%s]Description:[-]%s\n"+
			"[%s]Size:[-]       %s\n"+
			"[%s]Replicas:[-]   %d\n"+
			"[%s]Sealed:[-]     %s",
		dim, s.Bucket(),
		dim, s.Description(),
		dim, formatBytes(s.Size()),
		dim, s.Replicas(),
		dim, sealed,
	)

	ol.preview.SetText(text)
}
