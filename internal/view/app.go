package view

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/help"
	"github.com/atterpac/jig/layout"
	"github.com/atterpac/jig/nav"
	"github.com/atterpac/jig/theme"
	"github.com/atterpac/jig/theme/themes"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/atterpac/gnat/internal/config"
	"github.com/atterpac/gnat/internal/nats"
)

// App is the main application controller.
type App struct {
	app       *layout.App
	statusBar *layout.StatusBar
	menu      *layout.Menu
	toasts    *components.ToastManager

	mu            sync.RWMutex
	provider      nats.Provider
	activeProfile *binding.Value[string]
	cfg           *config.Config

	stopMetrics chan struct{}
}

// NewApp creates the application with a NATS provider.
func NewApp(provider nats.Provider, cfg *config.Config, activeProfileName string) *App {
	a := &App{
		provider:      provider,
		cfg:           cfg,
		activeProfile: binding.NewValue(activeProfileName),
		stopMetrics:   make(chan struct{}),
	}
	a.buildApp()
	a.setup()
	a.startMetricsRefresh()
	return a
}

func (a *App) buildApp() {
	a.statusBar = layout.NewStatusBar()
	a.menu = layout.NewMenu()

	a.app = layout.NewApp(layout.AppConfig{
		TopBar:          a.statusBar,
		BottomBar:       a.menu,
		ShowCrumbs:      true,
		TopBarHeight:    3,
		BottomBarHeight: 1,
		OnComponentChange: func(c nav.Component) {
			a.menu.SetHints(c.Hints())
		},
	})

	// Initialize toast manager
	a.toasts = components.NewToastManager(a.app.GetApplication())
	a.toasts.SetPosition(components.ToastBottomRight)
	a.toasts.SetMaxVisible(3)
	a.toasts.SetDefaultDuration(3 * time.Second)

	// Set up toast drawing after main content
	a.app.GetApplication().SetAfterDrawFunc(func(screen tcell.Screen) {
		w, h := screen.Size()
		a.toasts.Draw(screen, w, h)
	})

	// Set up reactive binding for profile status
	a.activeProfile.BindToWithDraw(func(profile string) {
		a.statusBar.ClearSections()
		colorFunc := theme.Get().Accent
		if strings.Contains(profile, "(connecting") {
			colorFunc = theme.Get().Warning
		} else if strings.Contains(profile, "(failed)") {
			colorFunc = theme.Get().Error
		}
		a.statusBar.AddSection(layout.StatusSection{
			Text:      profile,
			ColorFunc: colorFunc,
		})
	})
	if a.provider != nil {
		a.statusBar.SetConnectionStatus(a.provider.IsConnected(), a.provider.ServerURL())
	}
}

func (a *App) setup() {
	a.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		// Skip global handling when command bar is active
		if a.statusBar.IsCommandMode() {
			return event
		}

		isModal := a.app.Pages().CurrentIsModal()

		switch {
		case event.Rune() == 'q' && !isModal:
			if !a.app.Pages().CanPop() {
				a.app.Stop()
				return nil
			}

		case event.Key() == tcell.KeyEscape || event.Key() == tcell.KeyBackspace2:
			if isModal {
				a.app.Pages().DismissModal()
				return nil
			}
			if a.app.Pages().CanPop() {
				a.app.Pages().Pop()
				return nil
			}

		case event.Rune() == '?' && !isModal:
			a.showHelp()
			return nil

		case event.Rune() == 'T' && !isModal:
			a.showThemeSelector()
			return nil

		case event.Rune() == 'P' && !isModal:
			a.showProfileSelector()
			return nil

		case event.Rune() == ':' && !isModal:
			a.showCommandBar()
			return nil
		}

		return event
	})

	// Push initial view
	a.NavigateToDashboard()
}

// Run starts the TUI event loop.
func (a *App) Run() error {
	return a.app.Run()
}

// Provider returns the NATS provider (thread-safe).
func (a *App) Provider() nats.Provider {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.provider
}

// QueueUpdateDraw queues a UI update and redraw (thread-safe).
func (a *App) QueueUpdateDraw(fn func()) {
	a.app.QueueUpdateDraw(fn)
}

// Navigation methods

func (a *App) NavigateToDashboard() {
	view := NewDashboard(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToStreams() {
	view := NewStreamList(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToStreamDetail(name string) {
	view := NewStreamDetail(a, name)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToConsumers(streamName string) {
	view := NewConsumerList(a, streamName)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToConsumerDetail(streamName, consumerName string) {
	view := NewConsumerDetail(a, streamName, consumerName)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToKVStores() {
	view := NewKVList(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToKVDetail(bucket string) {
	view := NewKVDetail(a, bucket)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToObjectStores() {
	view := NewObjectList(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToObjectDetail(bucket string) {
	view := NewObjectDetail(a, bucket)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToMessageMonitor() {
	view := NewMessageMonitor(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToMessageMonitorWithSubject(subject string) {
	view := NewMessageMonitorWithSubject(a, subject)
	a.app.Pages().Push(view)
}

// Toast helpers

// Toast returns the toast manager for custom toast operations.
func (a *App) Toast() *components.ToastManager {
	return a.toasts
}

// ShowSuccess shows a success toast notification.
func (a *App) ShowSuccess(msg string) {
	a.toasts.Success(msg)
}

// ShowError shows an error toast notification.
func (a *App) ShowError(msg string) {
	a.toasts.Error(msg)
}

// ShowInfo shows an info toast notification.
func (a *App) ShowInfo(msg string) {
	a.toasts.Info(msg)
}

// ShowWarning shows a warning toast notification.
func (a *App) ShowWarning(msg string) {
	a.toasts.Warning(msg)
}

// UI helpers

func (a *App) showCommandBar() {
	a.statusBar.SetCommandPrompt(": ")
	a.statusBar.SetCommandPlaceholder("command...")
	a.statusBar.EnterCommandMode()
	a.app.SetFocus(a.statusBar.GetCommandInput())

	a.statusBar.SetOnCommandSubmit(func(text string) {
		a.statusBar.ExitCommandMode()
		a.handleCommand(text)
	})
	a.statusBar.SetOnCommandCancel(func() {
		a.statusBar.ExitCommandMode()
	})
}

func (a *App) handleCommand(text string) {
	switch text {
	case "streams", "s":
		a.NavigateToStreams()
	case "kv", "k":
		a.NavigateToKVStores()
	case "objects", "obj", "o":
		a.NavigateToObjectStores()
	case "dashboard", "dash", "d":
		a.NavigateToDashboard()
	case "monitor", "mon", "m", "sub":
		a.NavigateToMessageMonitor()
	case "profile", "profiles", "p":
		a.showProfileSelector()
	case "quit", "q":
		a.app.Stop()
	}
}

func (a *App) startMetricsRefresh() {
	go func() {
		a.refreshMetrics()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-a.stopMetrics:
				return
			case <-ticker.C:
				a.refreshMetrics()
			}
		}
	}()
}

func (a *App) refreshMetrics() {
	provider := a.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := provider.AccountInfo(ctx)
	if err != nil {
		return
	}

	a.QueueUpdateDraw(func() {
		a.statusBar.SetRightSections([]layout.StatusSection{
			{
				Icon:  "󰍛",
				Text:  formatBytes(info.Memory),
				Color: theme.Get().Accent(),
			},
			{
				Icon:  "󰋊",
				Text:  formatBytes(info.Store),
				Color: theme.Get().Accent(),
			},
			{
				Icon:  "󰇘",
				Text:  fmt.Sprintf("%d", info.API.Total),
				Color: theme.Get().FgDim(),
			},
			{
				Icon:  "󰀨",
				Text:  fmt.Sprintf("%d err", info.API.Errors),
				Color: a.errColor(info.API.Errors),
			},
			{
				Icon:  "󰓦",
				Text:  fmt.Sprintf("%d", info.Streams),
				Color: theme.Get().Accent(),
			},
			{
				Icon:  "󰑐",
				Text:  fmt.Sprintf("%d", info.Consumers),
				Color: theme.Get().Accent(),
			},
		})
	})
}

func (a *App) errColor(count uint64) tcell.Color {
	if count > 0 {
		return theme.Get().Error()
	}
	return theme.Get().FgDim()
}

func (a *App) showHelp() {
	h := help.New().
		SetAppName("gnat").
		SetVersion("by getgalaxy.io").
		AddSection("Global", []help.ActionInfo{
			{Key: "?", Description: "Show this help"},
			{Key: "T", Description: "Select theme"},
			{Key: "P", Description: "Switch profile"},
			{Key: ":", Description: "Command bar"},
			{Key: "Esc", Description: "Go back / Cancel"},
			{Key: "q", Description: "Quit"},
		}).
		AddSection("Navigation", []help.ActionInfo{
			{Key: "j/k", Description: "Move down/up"},
			{Key: "Enter", Description: "Select / View details"},
			{Key: "Tab", Description: "Switch panes"},
			{Key: "Ctrl+←/→", Description: "Resize panes"},
		}).
		AddSection("Dashboard", []help.ActionInfo{
			{Key: "s", Description: "Go to Streams"},
			{Key: "k", Description: "Go to KV Stores"},
			{Key: "o", Description: "Go to Object Stores"},
			{Key: "m", Description: "Go to Message Monitor"},
			{Key: "r", Description: "Refresh data"},
		}).
		AddSection("List Views", []help.ActionInfo{
			{Key: "/", Description: "Filter / Search"},
			{Key: "c", Description: "Create new"},
			{Key: "d", Description: "Delete selected"},
			{Key: "r", Description: "Refresh list"},
		}).
		AddSection("Message Monitor", []help.ActionInfo{
			{Key: "/", Description: "Enter subject / Filter"},
			{Key: "p", Description: "Pause / Resume"},
			{Key: "c", Description: "Clear messages"},
			{Key: "u", Description: "Unsubscribe"},
			{Key: "m", Description: "Toggle JetStream/NATS mode"},
			{Key: "d", Description: "Cycle delivery policy"},
		}).
		AddSection("Commands (: mode)", []help.ActionInfo{
			{Key: "streams", Description: "Go to Streams (alias: s)"},
			{Key: "kv", Description: "Go to KV Stores (alias: k)"},
			{Key: "objects", Description: "Go to Object Stores (alias: o)"},
			{Key: "dashboard", Description: "Go to Dashboard (alias: d)"},
			{Key: "monitor", Description: "Go to Message Monitor (alias: m)"},
			{Key: "quit", Description: "Quit application (alias: q)"},
		})

	modal := h.Modal()
	modal.Show()

	// Wrap help modal in a components.Modal for proper navigation
	wrapper := components.NewModal(components.ModalConfig{
		Title:    "Help",
		Width:    82,
		Height:   32,
		Backdrop: true,
	}).SetContent(modal)

	wrapper.SetHints([]components.KeyHint{
		{Key: "j/k", Description: "Scroll"},
		{Key: "Esc", Description: "Close"},
	})

	a.app.Pages().Push(wrapper)
}

func (a *App) showThemeSelector() {
	themeNames := themes.Names()
	currentTheme := a.cfg.Theme
	if currentTheme == "" {
		currentTheme = themes.DefaultName
	}

	selector := theme.NewThemeSelectorModal(themeNames, currentTheme)

	// Live preview on selection change
	selector.SetOnPreview(func(name string) {
		if t := themes.Get(name); t != nil {
			theme.SetProvider(t)
		}
	})

	// Save theme on select
	selector.SetOnSelect(func(name string) {
		if t := themes.Get(name); t != nil {
			theme.SetProvider(t)
			a.cfg.Theme = name
			_ = a.cfg.Save()
			a.app.Pages().Pop()
			a.ShowSuccess(fmt.Sprintf("Theme set to %s", name))
		} else {
			a.ShowError(fmt.Sprintf("Theme not found: %s", name))
		}
	})

	// Restore original theme on cancel
	selector.SetOnCancel(func() {
		if t := themes.Get(currentTheme); t != nil {
			theme.SetProvider(t)
		}
		a.app.Pages().Pop()
	})

	// Wrap selector in a thin nav.Component wrapper (ThemeSelectorModal already draws its own modal)
	wrapper := &themeSelectorWrapper{selector: selector}
	a.app.Pages().Push(wrapper)
}

// themeSelectorWrapper wraps ThemeSelectorModal to implement nav.Component
type themeSelectorWrapper struct {
	selector *theme.ThemeSelectorModal
}

func (w *themeSelectorWrapper) Name() string                                { return "Theme" }
func (w *themeSelectorWrapper) Start()                                      {}
func (w *themeSelectorWrapper) Stop()                                       {}
func (w *themeSelectorWrapper) Hints() []components.KeyHint                 { return nil }
func (w *themeSelectorWrapper) Draw(screen tcell.Screen)                    { w.selector.Draw(screen) }
func (w *themeSelectorWrapper) GetRect() (int, int, int, int)               { return w.selector.GetRect() }
func (w *themeSelectorWrapper) SetRect(x, y, width, height int)             { w.selector.SetRect(x, y, width, height) }
func (w *themeSelectorWrapper) InputHandler() func(*tcell.EventKey, func(tview.Primitive)) {
	return w.selector.InputHandler()
}
func (w *themeSelectorWrapper) Focus(delegate func(tview.Primitive))        { w.selector.Focus(delegate) }
func (w *themeSelectorWrapper) Blur()                                       { w.selector.Blur() }
func (w *themeSelectorWrapper) HasFocus() bool                              { return w.selector.HasFocus() }
func (w *themeSelectorWrapper) MouseHandler() func(tview.MouseAction, *tcell.EventMouse, func(tview.Primitive)) (bool, tview.Primitive) {
	return w.selector.MouseHandler()
}
func (w *themeSelectorWrapper) PasteHandler() func(string, func(tview.Primitive)) {
	return nil
}

func (a *App) showProfileSelector() {
	modal := NewProfileModal(
		a.cfg,
		func(name string) {
			a.app.Pages().DismissModal()
			a.SwitchProfile(name)
		},
		func() {
			a.app.Pages().DismissModal()
			a.showProfileForm("", config.ConnectionConfig{}, false)
		},
		func() {
			a.app.Pages().DismissModal()
		},
		func(name string) {
			a.app.Pages().DismissModal()
			if cfg, ok := a.cfg.GetProfile(name); ok {
				a.showProfileForm(name, cfg, true)
			}
		},
		func(name string) {
			if err := a.cfg.DeleteProfile(name); err == nil {
				_ = a.cfg.Save()
				a.app.Pages().DismissModal()
				a.showProfileSelector()
				a.ShowSuccess(fmt.Sprintf("Profile '%s' deleted", name))
			} else {
				a.ShowError(fmt.Sprintf("Failed to delete profile: %v", err))
			}
		},
	)
	a.app.Pages().Push(modal)
}

func (a *App) showProfileForm(name string, cfg config.ConnectionConfig, isEdit bool) {
	form := NewProfileForm(
		name,
		cfg,
		isEdit,
		func(saveName string, newCfg config.ConnectionConfig) {
			a.cfg.SaveProfile(saveName, newCfg)
			_ = a.cfg.Save()
			a.app.Pages().DismissModal()
			a.showProfileSelector()
			if isEdit {
				a.ShowSuccess(fmt.Sprintf("Profile '%s' updated", saveName))
			} else {
				a.ShowSuccess(fmt.Sprintf("Profile '%s' created", saveName))
			}
		},
		func() {
			a.app.Pages().DismissModal()
			a.showProfileSelector()
		},
	)
	a.app.Pages().Push(form)
}

// SwitchProfile switches to a different connection profile.
func (a *App) SwitchProfile(name string) {
	profileCfg, ok := a.cfg.GetProfile(name)
	if !ok {
		return
	}

	// Update status to show connecting (binding auto-updates status bar)
	a.activeProfile.SetAndDraw(name + " (connecting...)")
	a.statusBar.SetConnectionStatus(false, "")

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		provider := a.Provider()
		if provider == nil {
			return
		}

		err := provider.Reconnect(ctx, profileCfg)

		if err != nil {
			a.activeProfile.SetAndDraw(name + " (failed)")
			a.app.QueueUpdateDraw(func() {
				a.statusBar.SetConnectionStatus(false, err.Error())
			})
			a.ShowError(fmt.Sprintf("Connection failed: %v", err))
			return
		}

		// Success - binding auto-updates status bar
		a.activeProfile.SetAndDraw(name)

		_ = a.cfg.SetActiveProfile(name)
		_ = a.cfg.Save()

		a.app.QueueUpdateDraw(func() {
			a.statusBar.SetConnectionStatus(provider.IsConnected(), provider.ServerURL())
		})
		a.ShowSuccess(fmt.Sprintf("Connected to %s", name))

		// Refresh metrics immediately
		go a.refreshMetrics()
	}()
}
