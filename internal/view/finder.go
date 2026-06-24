package view

import (
	"context"
	"time"

	"github.com/atterpac/dado/components"
	"github.com/gdamore/tcell/v2"
)

// finderWrapper wraps components.Finder to implement nav.Component for page stack.
type finderWrapper struct {
	finder *components.Finder
}

func (w *finderWrapper) Name() string                         { return "Find" }
func (w *finderWrapper) Start()                               {}
func (w *finderWrapper) Stop()                                {}
func (w *finderWrapper) Hints() []components.KeyHint          { return nil }
func (w *finderWrapper) Draw(screen tcell.Screen)             { w.finder.Draw(screen) }
func (w *finderWrapper) Rect() (int, int, int, int)           { return w.finder.Rect() }
func (w *finderWrapper) GetRect() (int, int, int, int)        { return w.finder.Rect() }
func (w *finderWrapper) SetRect(x, y, width, height int)      { w.finder.SetRect(x, y, width, height) }
func (w *finderWrapper) Focus()                               { w.finder.Focus() }
func (w *finderWrapper) Blur()                                { w.finder.Blur() }
func (w *finderWrapper) HasFocus() bool                       { return w.finder.HasFocus() }
func (w *finderWrapper) HandleKey(event *tcell.EventKey) bool { return w.finder.HandleKey(event) }
func (w *finderWrapper) HandlePaste(text string) bool         { return false }

// showGlobalFinder opens the global fuzzy finder (Ctrl+P).
func (a *App) showGlobalFinder() {
	finder := components.NewFinder().
		SetPlaceholder("Search streams, KV stores, object stores...").
		SetPrompt("> ").
		SetShowCategories(true).
		SetShowDescription(true).
		SetMaxVisible(15).
		SetVimMode(true)

	finder.SetCategories([]components.FinderCategory{
		{Name: "Streams", Icon: "󰓦", Priority: 1},
		{Name: "KV Stores", Icon: "󰌆", Priority: 2},
		{Name: "Object Stores", Icon: "󰉕", Priority: 3},
	})

	finder.SetOnSelect(func(item components.FinderItem) {
		a.app.Pages().Pop()
		switch item.Category {
		case "Streams":
			a.NavigateToStreamDetail(item.ID)
		case "KV Stores":
			a.NavigateToKVDetail(item.ID)
		case "Object Stores":
			a.NavigateToObjectDetail(item.ID)
		}
	})

	finder.SetOnCancel(func() {
		a.app.Pages().Pop()
	})

	wrapper := &finderWrapper{finder: finder}
	a.app.Pages().Push(wrapper)
	a.app.SetFocus(finder)

	// Fetch resources in background
	go func() {
		var items []components.FinderItem

		provider := a.Provider()
		if provider == nil {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Fetch streams
		if streams, err := provider.ListStreams(ctx); err == nil {
			for _, s := range streams {
				items = append(items, components.FinderItem{
					ID:          s.Config.Name,
					Label:       s.Config.Name,
					Description: formatNumber(s.State.Msgs) + " msgs, " + formatBytes(s.State.Bytes),
					Category:    "Streams",
					Icon:        "󰓦",
				})
			}
		}

		// Fetch KV stores
		if kvStores, err := provider.ListKeyValueStores(ctx); err == nil {
			for _, kv := range kvStores {
				items = append(items, components.FinderItem{
					ID:          kv.Bucket(),
					Label:       kv.Bucket(),
					Description: formatNumber(kv.Values()) + " keys, " + formatBytes(kv.Bytes()),
					Category:    "KV Stores",
					Icon:        "󰌆",
				})
			}
		}

		// Fetch object stores
		if objStores, err := provider.ListObjectStores(ctx); err == nil {
			for _, os := range objStores {
				items = append(items, components.FinderItem{
					ID:          os.Bucket(),
					Label:       os.Bucket(),
					Description: formatBytes(os.Size()),
					Category:    "Object Stores",
					Icon:        "󰉕",
				})
			}
		}

		a.QueueUpdateDraw(func() {
			finder.SetItems(items)
		})
	}()
}
