package view

import (
	"context"
	"encoding/json"
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

	"github.com/atterpac/gnat/internal/clipboard"
	"github.com/atterpac/gnat/internal/command"
	"github.com/atterpac/gnat/internal/config"
	"github.com/atterpac/gnat/internal/nats"
	"github.com/nats-io/nats.go/jetstream"
)

// CommandContextProvider is implemented by views that provide context for command expansion.
type CommandContextProvider interface {
	CommandContext() CommandViewContext
}

// CommandViewContext holds the view-specific variables for command template expansion.
type CommandViewContext struct {
	Stream, Consumer, Bucket, Subject, Key string
}

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
	a.statusBar.SetTitle("gnat")
	a.statusBar.SetTitleAlign(components.AlignLeft)
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

		case event.Rune() == '!' && !isModal:
			a.NavigateToDebug()
			return nil

		case event.Rune() == ':' && !isModal:
			a.showCommandBar()
			return nil

		case event.Key() == tcell.KeyCtrlP && !isModal:
			a.showGlobalFinder()
			return nil

		case event.Rune() == 'B' && !isModal:
			a.addCurrentBookmark()
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

func (a *App) NavigateToMessageBrowser(streamName string) {
	view := NewMessageBrowser(a, streamName)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToKVWatch(bucket string) {
	view := NewKVWatch(a, bucket)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToMessageMonitor() {
	view := NewMessageMonitor(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToDebug() {
	data := collectDebugData(a)
	view := NewDebugScreen(a, data)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToMessageMonitorWithSubject(subject string) {
	view := NewMessageMonitorWithSubject(a, subject)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToConsumerLag() {
	view := NewConsumerLag(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToRequestReply() {
	view := NewRequestReply(a, "")
	a.app.Pages().Push(view)
}

func (a *App) NavigateToRequestReplyWithSubject(subject string) {
	view := NewRequestReply(a, subject)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToSubjectExplorer() {
	view := NewSubjectExplorer(a)
	a.app.Pages().Push(view)
}

func (a *App) NavigateToPlayground() {
	view := NewPlayground(a)
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

	// Set up tab completion with built-in + user commands
	activeProfile, _ := a.cfg.GetActiveProfile()
	a.statusBar.SetOnComplete(func(input string) []string {
		builtins := []string{
			"streams", "s", "kv", "k", "objects", "obj", "o",
			"dashboard", "dash", "d", "monitor", "mon", "m",
			"lag", "cl", "request", "req", "subjects", "subj", "playground", "play",
			"stream", "consumer", "watch", "purge", "pub", "get",
			"import", "bookmarks", "bm",
			"debug", "info",
			"profile", "quit", "q",
		}
		userCmds := a.cfg.ListCommandNames(activeProfile)
		all := append(builtins, userCmds...)

		if input == "" {
			return all
		}
		var matches []string
		for _, name := range all {
			if strings.HasPrefix(name, input) {
				matches = append(matches, name)
			}
		}
		return matches
	})

	a.statusBar.EnterCommandMode()
	a.app.SetFocus(a.statusBar.GetCommandInput())

	a.statusBar.SetOnCommandSubmit(func(text string) {
		a.statusBar.ExitCommandMode()
		a.handleCommand(text)
		a.refocusCurrent()
	})
	a.statusBar.SetOnCommandCancel(func() {
		a.statusBar.ExitCommandMode()
		a.refocusCurrent()
	})
}

func (a *App) handleCommand(text string) {
	text = strings.TrimSpace(text)
	if text == "" {
		a.refocusCurrent()
		return
	}

	fields := command.SplitArgs(text)
	if len(fields) == 0 {
		a.refocusCurrent()
		return
	}
	cmdName := fields[0]
	args := fields[1:]

	// Check user-defined commands first
	activeProfile, _ := a.cfg.GetActiveProfile()
	commands := a.cfg.GetMergedCommands(activeProfile)
	if cfg, ok := commands[cmdName]; ok {
		a.executeUserCommand(cmdName, cfg, args)
		return
	}

	// Built-in commands
	switch cmdName {
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
	case "lag", "cl":
		a.NavigateToConsumerLag()
	case "request", "req":
		if len(args) > 0 {
			a.NavigateToRequestReplyWithSubject(args[0])
		} else {
			a.NavigateToRequestReply()
		}
	case "subjects", "subj":
		a.NavigateToSubjectExplorer()
	case "playground", "play":
		a.NavigateToPlayground()
	case "profile", "profiles", "p":
		a.handleProfileCommand(strings.Join(args, " "))
	case "stream":
		if len(args) > 0 {
			a.NavigateToStreamDetail(args[0])
		} else {
			a.toasts.Warning("Usage: :stream <name>")
		}
	case "consumer":
		if len(args) >= 2 {
			a.NavigateToConsumerDetail(args[0], args[1])
		} else {
			a.toasts.Warning("Usage: :consumer <stream> <name>")
		}
	case "watch":
		if len(args) > 0 {
			a.NavigateToKVWatch(args[0])
		} else {
			a.toasts.Warning("Usage: :watch <bucket>")
		}
	case "purge":
		if len(args) > 0 {
			streamName := args[0]
			ConfirmDelete(a, "purge stream", streamName, func() {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := a.Provider().PurgeStream(ctx, streamName); err != nil {
						a.ShowError(err.Error())
					} else {
						a.ShowSuccess("Purged stream: " + streamName)
					}
				}()
			})
		} else {
			a.toasts.Warning("Usage: :purge <stream>")
		}
	case "pub":
		if len(args) >= 2 {
			subject := args[0]
			payload := strings.Join(args[1:], " ")
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := a.Provider().Publish(ctx, subject, []byte(payload), nil); err != nil {
					a.ShowError(err.Error())
				} else {
					a.ShowSuccess("Published to " + subject)
				}
			}()
		} else {
			a.toasts.Warning("Usage: :pub <subject> <payload>")
		}
	case "get":
		if len(args) > 0 {
			a.handleGetCommand(args[0])
		} else {
			a.toasts.Warning("Usage: :get kv/<bucket>/<key>")
		}
	case "bookmarks", "bm":
		a.showBookmarks()
	case "import":
		if len(args) > 0 && args[0] == "stream" {
			a.showImportStreamForm()
		} else {
			a.toasts.Warning("Usage: :import stream")
		}
	case "debug", "info":
		a.NavigateToDebug()
	case "quit", "q":
		a.app.Stop()
	default:
		a.toasts.Warning(fmt.Sprintf("Unknown command: %s", cmdName))
		a.refocusCurrent()
	}
}

func (a *App) refocusCurrent() {
	if c := a.app.Pages().Current(); c != nil {
		a.app.SetFocus(c)
	}
}

func (a *App) handleProfileCommand(args string) {
	args = strings.TrimSpace(args)
	if args == "" {
		a.showProfileSelector()
		return
	}

	fields := strings.Fields(args)
	subcmd := fields[0]
	subArgs := fields[1:]

	switch subcmd {
	case "new":
		a.showProfileForm("", config.ConnectionConfig{}, false)
	case "edit":
		name := a.cfg.ActiveProfile
		if len(subArgs) > 0 {
			name = subArgs[0]
		}
		if cfg, ok := a.cfg.GetProfile(name); ok {
			a.showProfileForm(name, cfg, true)
		} else {
			a.toasts.Warning(fmt.Sprintf("Profile not found: %s", name))
		}
	case "delete":
		if len(subArgs) == 0 {
			a.toasts.Warning("Usage: :profile delete <name>")
			return
		}
		name := subArgs[0]
		if err := a.cfg.DeleteProfile(name); err != nil {
			a.toasts.Error(err.Error())
		} else {
			_ = a.cfg.Save()
			a.ShowSuccess(fmt.Sprintf("Profile '%s' deleted", name))
		}
	default:
		// Treat as profile name to switch to
		if a.cfg.ProfileExists(subcmd) {
			a.SwitchProfile(subcmd)
		} else {
			a.toasts.Warning(fmt.Sprintf("Unknown profile: %s", subcmd))
		}
	}
}

func (a *App) handleGetCommand(path string) {
	// Parse kv/<bucket>/<key>
	parts := strings.SplitN(path, "/", 3)
	if len(parts) != 3 || parts[0] != "kv" {
		a.toasts.Warning("Usage: :get kv/<bucket>/<key>")
		return
	}
	bucket, key := parts[1], parts[2]
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		kv, err := a.Provider().GetKeyValue(ctx, bucket)
		if err != nil {
			a.ShowError(err.Error())
			return
		}
		entry, err := kv.Get(ctx, key)
		if err != nil {
			a.ShowError(err.Error())
			return
		}
		if err := clipboard.Copy(string(entry.Value())); err != nil {
			a.ShowError("Clipboard: " + err.Error())
		} else {
			a.ShowSuccess(fmt.Sprintf("Copied %s/%s (%s)", bucket, key, formatBytes(uint64(len(entry.Value())))))
		}
	}()
}

func (a *App) showImportStreamForm() {
	modal := components.NewFormBuilder().
		TextArea("config", "Stream Config (JSON)").
		Placeholder("paste stream config JSON").
		Done().
		OnSubmit(func(values map[string]any) {
			configJSON := getString(values, "config")
			if configJSON == "" {
				a.ShowError("Config is required")
				return
			}
			var cfg jetstream.StreamConfig
			if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
				a.ShowError("Invalid JSON: " + err.Error())
				return
			}
			if cfg.Name == "" {
				a.ShowError("Stream config must include a name")
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := a.Provider().CreateStream(ctx, cfg); err != nil {
					a.ShowError(err.Error())
				} else {
					a.ShowSuccess("Stream imported: " + cfg.Name)
				}
			}()
		}).
		AsFormModal("Import Stream Config", 70, 20)

	modal.SetHints([]components.KeyHint{
		{Key: "Ctrl+S", Description: "Import"},
		{Key: "Esc", Description: "Cancel"},
	})

	a.app.Pages().Push(modal)
}

func (a *App) addCurrentBookmark() {
	stack := a.app.Pages().GetStack()
	for i := len(stack) - 1; i >= 0; i-- {
		if provider, ok := stack[i].(CommandContextProvider); ok {
			vc := provider.CommandContext()
			var bm config.Bookmark
			switch {
			case vc.Consumer != "" && vc.Stream != "":
				bm = config.Bookmark{Type: "consumer", Name: vc.Consumer, Stream: vc.Stream}
			case vc.Stream != "":
				bm = config.Bookmark{Type: "stream", Name: vc.Stream}
			case vc.Bucket != "":
				bm = config.Bookmark{Type: "kv", Name: vc.Bucket}
			default:
				continue
			}
			if a.cfg.AddBookmark(bm) {
				_ = a.cfg.Save()
				a.ShowSuccess(fmt.Sprintf("Bookmarked %s: %s", bm.Type, bm.Name))
			} else {
				// Already exists — toggle it off
				a.cfg.RemoveBookmarkMatch(bm)
				_ = a.cfg.Save()
				a.ShowInfo(fmt.Sprintf("Removed bookmark: %s", bm.Name))
			}
			return
		}
	}
	a.ShowWarning("Nothing to bookmark on this view")
}

func (a *App) showBookmarks() {
	bookmarks := a.cfg.Bookmarks
	if len(bookmarks) == 0 {
		a.ShowInfo("No bookmarks saved. Press B on a resource view to bookmark.")
		return
	}

	finder := components.NewFinder().
		SetPlaceholder("Search bookmarks...").
		SetPrompt("> ").
		SetShowCategories(true).
		SetShowDescription(true).
		SetMaxVisible(15).
		SetVimMode(true)

	var items []components.FinderItem
	for i, bm := range bookmarks {
		label := bm.Name
		desc := bm.Type
		if bm.Stream != "" {
			desc = fmt.Sprintf("%s (stream: %s)", bm.Type, bm.Stream)
		}
		icon := "󰓦"
		switch bm.Type {
		case "kv":
			icon = "󰌆"
		case "object":
			icon = "󰉕"
		case "consumer":
			icon = "󰑐"
		}
		items = append(items, components.FinderItem{
			ID:          fmt.Sprintf("%d", i),
			Label:       label,
			Description: desc,
			Category:    bm.Type,
			Icon:        icon,
			Data:        bm,
		})
	}
	finder.SetItems(items)

	finder.SetOnSelect(func(item components.FinderItem) {
		bm := item.Data.(config.Bookmark)
		a.app.Pages().Pop()
		switch bm.Type {
		case "stream":
			a.NavigateToStreamDetail(bm.Name)
		case "consumer":
			a.NavigateToConsumerDetail(bm.Stream, bm.Name)
		case "kv":
			a.NavigateToKVDetail(bm.Name)
		case "object":
			a.NavigateToObjectDetail(bm.Name)
		}
		a.refocusCurrent()
	})

	finder.SetOnCancel(func() {
		a.app.Pages().Pop()
	})

	wrapper := &finderWrapper{finder: finder}
	a.app.Pages().Push(wrapper)
	a.app.SetFocus(finder)
}

func (a *App) buildCommandContext(args []string) command.Context {
	activeProfileName, profileCfg := a.cfg.GetActiveProfile()
	expanded := profileCfg.ExpandEnv()

	ctx := command.Context{
		Profile:       activeProfileName,
		Args:          args,
		ServerURL:     expanded.URL,
		Domain:        expanded.Domain,
		Credentials:   expanded.Credentials,
		TLSCertPath:   expanded.TLS.Cert,
		TLSKeyPath:    expanded.TLS.Key,
		TLSCAPath:     expanded.TLS.CA,
		TLSServerName: expanded.TLS.ServerName,
		TLSSkipVerify: expanded.TLS.SkipVerify,
	}

	// Fall back to live server URL if config URL is empty
	if ctx.ServerURL == "" {
		provider := a.Provider()
		if provider != nil {
			ctx.ServerURL = provider.ServerURL()
		}
	}

	// Get view context from the current page stack
	stack := a.app.Pages().GetStack()
	for i := len(stack) - 1; i >= 0; i-- {
		if provider, ok := stack[i].(CommandContextProvider); ok {
			vc := provider.CommandContext()
			if vc.Stream != "" || vc.Consumer != "" || vc.Bucket != "" || vc.Subject != "" || vc.Key != "" {
				ctx.Stream = vc.Stream
				ctx.Consumer = vc.Consumer
				ctx.Bucket = vc.Bucket
				ctx.Subject = vc.Subject
				ctx.Key = vc.Key
				break
			}
		}
	}

	return ctx
}

func (a *App) executeUserCommand(name string, cfg config.CommandConfig, args []string) {
	cmdCtx := a.buildCommandContext(args)

	expandedCmd, err := command.ExpandCmd(cfg.Cmd, cmdCtx)
	if err != nil {
		a.toasts.Error(fmt.Sprintf("Command %q: %s", name, err))
		a.refocusCurrent()
		return
	}

	// Inject connection flags for nats CLI commands
	expandedCmd = command.InjectConnectionFlags(expandedCmd, cmdCtx)

	if cfg.Confirm {
		a.showCommandConfirm(name, cfg, expandedCmd)
		return
	}

	a.runCommand(name, expandedCmd, cfg)
}

func (a *App) showCommandConfirm(name string, cfg config.CommandConfig, expandedCmd string) {
	title := fmt.Sprintf("Confirm: %s", name)
	if cfg.Description != "" {
		title = fmt.Sprintf("Confirm: %s", cfg.Description)
	}

	infoText := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)
	infoText.SetBackgroundColor(theme.Bg())
	infoText.SetText(fmt.Sprintf("[%s]Command:[-] [%s]%s[-]\n\n[%s]%s[-]",
		theme.TagFgDim(), theme.TagAccent(), name,
		theme.TagFg(), expandedCmd))

	modal := components.NewModal(components.ModalConfig{
		Title:    title,
		Width:    70,
		Height:   10,
		Backdrop: true,
	})
	modal.SetContent(infoText)
	modal.SetOnSubmit(func() {
		a.app.Pages().Pop()
		a.runCommand(name, expandedCmd, cfg)
	})
	modal.SetHints([]components.KeyHint{
		{Key: "Enter", Description: "Confirm"},
		{Key: "Esc", Description: "Cancel"},
	})

	a.app.Pages().Push(modal)
	a.app.SetFocus(modal)
}

func (a *App) runCommand(name, expandedCmd string, cfg config.CommandConfig) {
	outputType := cfg.Output
	if outputType == "" {
		outputType = config.OutputLog
	}

	switch outputType {
	case config.OutputLog:
		a.runCommandLog(name, expandedCmd, cfg)
	case config.OutputJSON:
		a.runCommandJSON(name, expandedCmd, cfg)
	case config.OutputStreams:
		a.runCommandStreams(name, expandedCmd)
	case config.OutputConsumers:
		a.runCommandConsumers(name, expandedCmd)
	default:
		a.runCommandLog(name, expandedCmd, cfg)
	}
}

func (a *App) runCommandLog(name, expandedCmd string, cfg config.CommandConfig) {
	ctx, cancel := context.WithCancel(context.Background())

	lv := components.NewLogViewer()
	description := cfg.Description
	if description == "" {
		description = name
	}
	view := NewCommandOutputView(a, name, description, lv, cancel)

	a.app.Pages().Push(view)
	a.app.SetFocus(lv)

	go func() {
		lv.AddEntry(components.LogEntry{
			Level:   components.LogLevelInfo,
			Message: "$ " + expandedCmd,
		})

		err := command.RunStreaming(ctx, expandedCmd, func(line string) {
			a.app.QueueUpdateDraw(func() {
				lv.AddEntry(components.LogEntry{
					Level:   components.LogLevelInfo,
					Message: line,
				})
			})
		})

		a.app.QueueUpdateDraw(func() {
			if err != nil && ctx.Err() == nil {
				lv.AddEntry(components.LogEntry{
					Level:   components.LogLevelError,
					Message: fmt.Sprintf("Error: %s", err),
				})
			} else {
				lv.AddEntry(components.LogEntry{
					Level:   components.LogLevelInfo,
					Message: "--- Done ---",
				})
			}
		})
	}()
}

func (a *App) runCommandJSON(name, expandedCmd string, cfg config.CommandConfig) {
	ctx, cancel := context.WithCancel(context.Background())

	cv := components.NewCodeView().SetLanguage(components.LangJSON)
	cv.SetCode("Running...")

	description := cfg.Description
	if description == "" {
		description = name
	}
	view := NewCommandOutputView(a, name, description, cv, cancel)

	a.app.Pages().Push(view)
	a.app.SetFocus(cv)

	go func() {
		output, err := command.Run(ctx, expandedCmd)
		a.app.QueueUpdateDraw(func() {
			if err != nil && ctx.Err() == nil {
				cv.SetCode(fmt.Sprintf("Error: %s\n\n%s", err, output))
			} else {
				formatted := formatJSONPretty(output)
				cv.SetCode(formatted)
			}
		})
	}()
}

func (a *App) runCommandStreams(name, expandedCmd string) {
	a.refocusCurrent()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		output, err := command.Run(ctx, expandedCmd)
		a.app.QueueUpdateDraw(func() {
			if err != nil {
				a.toasts.Error(fmt.Sprintf("Command %q: %s", name, err))
			} else {
				a.toasts.Success(fmt.Sprintf("Command %q completed:\n%s", name, strings.TrimSpace(output)))
			}
			a.refocusCurrent()
		})
	}()
}

func (a *App) runCommandConsumers(name, expandedCmd string) {
	a.refocusCurrent()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		output, err := command.Run(ctx, expandedCmd)
		a.app.QueueUpdateDraw(func() {
			if err != nil {
				a.toasts.Error(fmt.Sprintf("Command %q: %s", name, err))
			} else {
				a.toasts.Success(fmt.Sprintf("Command %q completed:\n%s", name, strings.TrimSpace(output)))
			}
			a.refocusCurrent()
		})
	}()
}

func formatJSONPretty(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}

	var parsed interface{}
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return s
	}

	pretty, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return s
	}
	return string(pretty)
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
			{Key: "!", Description: "Debug info"},
			{Key: "T", Description: "Select theme"},
			{Key: "P", Description: "Switch profile"},
			{Key: "Ctrl+P", Description: "Fuzzy finder"},
			{Key: "B", Description: "Bookmark current"},
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
			{Key: "e", Description: "Edit selected"},
			{Key: "d", Description: "Delete selected"},
			{Key: "y", Description: "Yank to clipboard"},
			{Key: "Space", Description: "Toggle select"},
			{Key: "D", Description: "Bulk delete selected"},
			{Key: "r", Description: "Refresh list"},
		}).
		AddSection("Message Monitor", []help.ActionInfo{
			{Key: "/", Description: "Enter subject / Filter"},
			{Key: "p", Description: "Pause / Resume"},
			{Key: "c", Description: "Clear messages"},
			{Key: "u", Description: "Unsubscribe"},
			{Key: "m", Description: "Toggle JetStream/NATS mode"},
			{Key: "d", Description: "Cycle delivery policy"},
			{Key: "R", Description: "Republish message"},
			{Key: "y", Description: "Yank payload"},
		}).
		AddSection("Message Views", []help.ActionInfo{
			{Key: "f", Description: "JSON path filter (jq-like)"},
			{Key: "Esc", Description: "Clear JSON filter"},
		}).
		AddSection("Commands (: mode)", []help.ActionInfo{
			{Key: "streams", Description: "Go to Streams (alias: s)"},
			{Key: "kv", Description: "Go to KV Stores (alias: k)"},
			{Key: "objects", Description: "Go to Object Stores (alias: o)"},
			{Key: "dashboard", Description: "Go to Dashboard (alias: d)"},
			{Key: "monitor", Description: "Go to Message Monitor (alias: m)"},
			{Key: "lag", Description: "Consumer Lag Dashboard (alias: cl)"},
			{Key: "request", Description: "Request/Reply Tester (alias: req)"},
			{Key: "subjects", Description: "Subject Explorer (alias: subj)"},
			{Key: "playground", Description: "Pub/Sub Playground (alias: play)"},
			{Key: "stream <name>", Description: "Go to stream detail"},
			{Key: "consumer <s> <c>", Description: "Go to consumer detail"},
			{Key: "watch <bucket>", Description: "Watch KV changes"},
			{Key: "bookmarks", Description: "Show bookmarks (alias: bm)"},
			{Key: "pub <subj> <data>", Description: "Publish message"},
			{Key: "purge <stream>", Description: "Purge stream"},
			{Key: "get kv/<b>/<k>", Description: "Get KV value to clipboard"},
			{Key: "import stream", Description: "Import stream from JSON"},
			{Key: "debug", Description: "Debug info (alias: info)"},
			{Key: "profile", Description: "Profile selector (alias: p)"},
			{Key: "quit", Description: "Quit application (alias: q)"},
		})

	// Add custom commands section if any exist
	activeProfile, _ := a.cfg.GetActiveProfile()
	userCmds := a.cfg.GetMergedCommands(activeProfile)
	if len(userCmds) > 0 {
		var cmdInfos []help.ActionInfo
		for _, name := range a.cfg.ListCommandNames(activeProfile) {
			cfg := userCmds[name]
			desc := cfg.Description
			if desc == "" {
				desc = cfg.Cmd
			}
			cmdInfos = append(cmdInfos, help.ActionInfo{Key: name, Description: desc})
		}
		h = h.AddSection("Custom Commands", cmdInfos)
	}

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
