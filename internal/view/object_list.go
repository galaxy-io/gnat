package view

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/galaxy-io/gnat/internal/clipboard"
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
		SetHeaders("BUCKET", "SIZE", "REPLICAS", "SEALED").
		ConfigureEmpty(theme.IconFolder, "No Object Stores", "")

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
		SetRatio(0.6)

	return ol
}

func (ol *ObjectList) CommandContext() CommandViewContext {
	if s, ok := ol.binding.GetSelectedValue(); ok {
		return CommandViewContext{Bucket: s.Bucket()}
	}
	return CommandViewContext{}
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
		{Key: "y", Description: "Yank"},
		{Key: "Space", Description: "Select"},
		{Key: "D", Description: "Bulk Delete"},
		{Key: "p", Description: "Preview"},
		{Key: "r", Description: "Refresh"},
	}
}

func (ol *ObjectList) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return ol.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'c':
			showObjectStoreCreateForm(ol.app, func() {
				ol.binding.RefreshAsync()
			})
		case event.Rune() == 'd':
			if s, ok := ol.binding.GetSelectedValue(); ok {
				bucket := s.Bucket()
				ConfirmDelete(ol.app, "object store", bucket, func() {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						if err := ol.app.Provider().DeleteObjectStore(ctx, bucket); err != nil {
							ol.app.ShowError(err.Error())
						} else {
							ol.app.ShowSuccess("Deleted object store: " + bucket)
							ol.binding.RefreshAsync()
						}
					}()
				})
			}
		case event.Rune() == 'D':
			ol.bulkDelete()
		case event.Rune() == 'y':
			if s, ok := ol.binding.GetSelectedValue(); ok {
				info := map[string]interface{}{
					"bucket":      s.Bucket(),
					"description": s.Description(),
					"size":        s.Size(),
					"replicas":    s.Replicas(),
					"sealed":      s.Sealed(),
				}
				data, err := json.MarshalIndent(info, "", "  ")
				if err != nil {
					ol.app.ShowError(err.Error())
				} else if err := clipboard.Copy(string(data)); err != nil {
					ol.app.ShowError("Clipboard: " + err.Error())
				} else {
					ol.app.ShowSuccess("Copied object store status: " + s.Bucket())
				}
			}
		case event.Rune() == 'p':
			ol.ToggleDetail()
		case event.Rune() == 'r':
			ol.binding.RefreshAsync()
		default:
			if handler := ol.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (ol *ObjectList) bulkDelete() {
	keys := ol.table.GetSelectedKeys()
	if len(keys) == 0 {
		ol.app.ShowInfo("No object stores selected (use Space to select)")
		return
	}
	label := fmt.Sprintf("%d object stores", len(keys))
	ConfirmDelete(ol.app, "bulk", label, func() {
		go func() {
			for _, bucket := range keys {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := ol.app.Provider().DeleteObjectStore(ctx, bucket); err != nil {
					ol.app.ShowError(fmt.Sprintf("Delete %s: %s", bucket, err))
				}
				cancel()
			}
			ol.app.ShowSuccess(fmt.Sprintf("Deleted %d object stores", len(keys)))
			ol.table.ClearSelection()
			ol.binding.RefreshAsync()
		}()
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
