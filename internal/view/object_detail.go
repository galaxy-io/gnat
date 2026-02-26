package view

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/atterpac/gnat/internal/clipboard"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

// ObjectDetail is the object browser for an Object Store bucket.
type ObjectDetail struct {
	*components.MasterDetailView
	app    *App
	bucket string

	table      *components.Table
	detailView *tview.TextView

	store   jetstream.ObjectStore
	binding *binding.TableBinding[*jetstream.ObjectInfo]

	stopRefresh chan struct{}
	stopped     int32
}

// NewObjectDetail creates the object browser view.
func NewObjectDetail(app *App, bucket string) *ObjectDetail {
	od := &ObjectDetail{
		app:         app,
		bucket:      bucket,
		stopRefresh: make(chan struct{}, 1),
	}

	t := theme.Get()

	od.table = components.NewTable().
		SetHeaders("NAME", "SIZE", "CHUNKS", "MODIFIED", "STATUS").
		ConfigureEmpty(theme.IconFile, "No Objects", "")

	od.detailView = tview.NewTextView().
		SetDynamicColors(true)

	// Set up reactive table binding
	od.binding = binding.NewTableBinding[*jetstream.ObjectInfo](od.table).
		SetMapper(func(obj *jetstream.ObjectInfo) []string {
			status := "active"
			if obj.Deleted {
				status = "deleted"
			}
			modTime := "-"
			if !obj.ModTime.IsZero() {
				modTime = time.Since(obj.ModTime).Round(time.Second).String() + " ago"
			}
			return []string{obj.Name, formatBytes(obj.Size), fmt.Sprintf("%d", obj.Chunks), modTime, status}
		}).
		SetColorMapper(func(obj *jetstream.ObjectInfo) []tcell.Color {
			nameColor := t.Fg()
			if obj.Deleted {
				nameColor = t.FgDim()
			}
			return []tcell.Color{nameColor, t.Fg(), t.FgDim(), t.FgDim(), t.FgDim()}
		}).
		SetKeyMapper(func(obj *jetstream.ObjectInfo) string {
			return obj.Name
		}).
		SetOnRefresh(func(data []*jetstream.ObjectInfo, err error) {
			if err != nil {
				od.app.QueueUpdateDraw(func() {
					od.detailView.SetText(fmt.Sprintf("[red]Error listing objects: %v[-]", err))
				})
			}
		})

	od.table.SetSelectionChangedFunc(func(row, col int) {
		od.updateDetail(row)
	})

	od.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle(fmt.Sprintf("Objects: %s", bucket)).
		SetDetailTitle("Details").
		SetMasterContent(od.table).
		SetDetailContent(od.detailView).
		SetRatio(0.6)

	return od
}

func (od *ObjectDetail) CommandContext() CommandViewContext {
	return CommandViewContext{Bucket: od.bucket}
}

func (od *ObjectDetail) Name() string { return od.bucket }

func (od *ObjectDetail) Start() {
	atomic.StoreInt32(&od.stopped, 0)
	od.stopRefresh = make(chan struct{}, 1)
	go od.initStore()
}

func (od *ObjectDetail) Stop() {
	atomic.StoreInt32(&od.stopped, 1)
	select {
	case od.stopRefresh <- struct{}{}:
	default:
	}
}

func (od *ObjectDetail) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "d", Description: "Delete"},
		{Key: "y", Description: "Yank"},
		{Key: "p", Description: "Preview"},
		{Key: "r", Description: "Refresh"},
		{Key: "Esc", Description: "Back"},
	}
}

func (od *ObjectDetail) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return od.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'd':
			if obj, ok := od.binding.GetSelectedValue(); ok && obj != nil && od.store != nil {
				name := obj.Name
				bucket := od.bucket
				ConfirmDelete(od.app, "object", name, func() {
					go func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						if err := od.store.Delete(ctx, name); err != nil {
							od.app.ShowError(err.Error())
						} else {
							od.app.ShowSuccess(fmt.Sprintf("Deleted object: %s from %s", name, bucket))
							go od.loadObjects()
						}
					}()
				})
			}
		case event.Rune() == 'y':
			if obj, ok := od.binding.GetSelectedValue(); ok && obj != nil {
				data, err := json.MarshalIndent(obj, "", "  ")
				if err != nil {
					od.app.ShowError(err.Error())
				} else if err := clipboard.Copy(string(data)); err != nil {
					od.app.ShowError("Clipboard: " + err.Error())
				} else {
					od.app.ShowSuccess("Copied object metadata: " + obj.Name)
				}
			}
		case event.Rune() == 'p':
			od.ToggleDetail()
		case event.Rune() == 'r':
			go od.loadObjects()
		default:
			if handler := od.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (od *ObjectDetail) initStore() {
	provider := od.app.Provider()
	if provider == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		store, err := provider.GetObjectStore(ctx, od.bucket)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&od.stopped) == 1 {
			return
		}

		if err != nil {
			od.app.QueueUpdateDraw(func() {
				od.detailView.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
			})
			return
		}

		od.store = store
		od.loadObjects()
	}()
}

func (od *ObjectDetail) loadObjects() {
	if od.store == nil {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		infos, err := od.store.List(ctx)

		// Check if view was stopped while fetching
		if atomic.LoadInt32(&od.stopped) == 1 {
			return
		}

		if err != nil {
			od.app.QueueUpdateDraw(func() {
				od.detailView.SetText(fmt.Sprintf("[red]Error listing objects: %v[-]", err))
			})
			return
		}

		od.binding.SetData(infos)
	}()
}

func (od *ObjectDetail) updateDetail(row int) {
	obj, ok := od.binding.GetItemValue(row)
	if !ok || obj == nil {
		od.detailView.SetText("")
		return
	}
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	modTime := "-"
	if !obj.ModTime.IsZero() {
		modTime = obj.ModTime.Format(time.RFC3339) + " (" + time.Since(obj.ModTime).Round(time.Second).String() + " ago)"
	}

	digest := obj.Digest
	if len(digest) > 20 {
		digest = digest[:20] + "..."
	}

	text := fmt.Sprintf(
		"[%s]Name:[-]        [%s]%s[-]\n"+
			"[%s]Description:[-] %s\n"+
			"[%s]Size:[-]        %s (%d bytes)\n"+
			"[%s]Chunks:[-]      %d\n"+
			"[%s]Digest:[-]      %s\n"+
			"[%s]NUID:[-]        %s\n"+
			"[%s]Modified:[-]    %s\n"+
			"[%s]Deleted:[-]     %v",
		dim, accent, obj.Name,
		dim, obj.Description,
		dim, formatBytes(obj.Size), obj.Size,
		dim, obj.Chunks,
		dim, digest,
		dim, obj.NUID,
		dim, modTime,
		dim, obj.Deleted,
	)

	od.detailView.SetText(text)
}
