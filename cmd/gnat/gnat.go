package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/atterpac/jig/theme"
	"github.com/atterpac/jig/theme/themes"
	"github.com/atterpac/jig/util"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/galaxy-io/gnat/internal/config"
	"github.com/galaxy-io/gnat/internal/logger"
	gnatnats "github.com/galaxy-io/gnat/internal/nats"
	"github.com/galaxy-io/gnat/internal/view"
)

const splashLogo = `
 ░▒▓██████▓▒░░▒▓███████▓▒░ ░▒▓██████▓▒░▒▓████████▓▒░
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░
░▒▓█▓▒░      ░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░
░▒▓█▓▒▒▓███▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓████████▓▒░ ░▒▓█▓▒░
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░
░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░
 ░▒▓██████▓▒░░▒▓█▓▒░░▒▓█▓▒░▒▓█▓▒░░▒▓█▓▒░ ░▒▓█▓▒░


`

const (
	maxRetries     = 5
	initialBackoff = 1 * time.Second
	maxBackoff     = 10 * time.Second
)

// Set via ldflags at build time.
var (
	version   = "dev"
	commit    = "unknown"
	buildDate = "unknown"
)

func main() {
	var (
		flagProfile string
		flagURL     string
		flagCreds   string
		flagTheme   string
		flagVersion bool
		flagDebug   bool
	)

	flag.StringVar(&flagProfile, "profile", "", "connection profile name")
	flag.StringVar(&flagURL, "url", "", "NATS server URL (overrides profile)")
	flag.StringVar(&flagCreds, "creds", "", "path to credentials file (overrides profile)")
	flag.StringVar(&flagTheme, "theme", "", "color theme (overrides config)")
	flag.BoolVar(&flagVersion, "version", false, "print version and exit")
	flag.BoolVar(&flagDebug, "debug", false, "enable debug logging to file")
	flag.Parse()

	if flagVersion {
		fmt.Printf("gnat %s (%s) built %s\n", version, commit, buildDate)
		os.Exit(0)
	}

	logPath, err := logger.Init(flagDebug)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Close()
	if flagDebug {
		fmt.Fprintf(os.Stderr, "Debug log: %s\n", logPath)
		// Start pprof server for memory profiling
		go func() {
			addr := "localhost:6060"
			fmt.Fprintf(os.Stderr, "pprof: http://%s/debug/pprof/\n", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				fmt.Fprintf(os.Stderr, "pprof server error: %v\n", err)
			}
		}()
	}
	logger.Debugf("version=%s commit=%s built=%s", version, commit, buildDate)
	logger.Debugf("go=%s os=%s arch=%s cpus=%d", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())

	// Load config
	logger.Debugf("loading config")
	cfg, err := config.Load()
	if err != nil {
		logger.Debugf("config load failed, using defaults: %v", err)
		cfg = config.DefaultConfig()
	}
	logger.Debugf("config loaded: theme=%q activeProfile=%q profiles=%d", cfg.Theme, cfg.ActiveProfile, len(cfg.Profiles))

	// Determine theme
	themeName := cfg.Theme
	if flagTheme != "" {
		themeName = flagTheme
	}
	if themeName == "" {
		themeName = themes.DefaultName
	}

	selectedTheme := themes.Get(themeName)
	if selectedTheme == nil {
		selectedTheme = themes.Default()
	}
	theme.SetProvider(selectedTheme)

	// Determine active profile
	activeProfile := cfg.ActiveProfile
	if flagProfile != "" {
		if err := cfg.SetActiveProfile(flagProfile); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		activeProfile = flagProfile
	}

	_, connCfg := cfg.GetActiveProfile()
	connCfg = connCfg.ExpandEnv()

	// CLI overrides
	if flagURL != "" {
		connCfg.URL = flagURL
	}
	if flagCreds != "" {
		connCfg.Credentials = flagCreds
	}

	// Connect with splash UI (separate tview app, matching tempo pattern)
	logger.Debugf("starting connection UI for %s", connCfg.URL)
	provider, err := connectWithUI(connCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer provider.Close()
	logger.Debugf("connected to %s", connCfg.URL)

	// Launch main app with ready provider
	logger.Debugf("creating main app")
	app := view.NewApp(provider, cfg, activeProfile)
	defer app.Close()
	logger.Debugf("starting main event loop")
	if err := app.Run(); err != nil {
		logger.Debugf("app.Run error: %v", err)
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	logger.Debugf("app exited cleanly")
}

// connectWithUI shows a splash screen while connecting to NATS.
// Uses a separate tview.Application that exits once connected.
func connectWithUI(cfg config.ConnectionConfig) (gnatnats.Provider, error) {
	app := tview.NewApplication()

	// Logo with gradient
	logoText := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignLeft)
	logoText.SetBackgroundColor(theme.Bg())

	gradientColors := util.DefaultGradientColors()
	gradientLogo := util.ApplyDiagonalGradient(splashLogo, gradientColors)
	logoText.SetText(gradientLogo)

	// Spacers
	leftSpacer := tview.NewBox().SetBackgroundColor(theme.Bg())
	rightSpacer := tview.NewBox().SetBackgroundColor(theme.Bg())
	topSpacer := tview.NewBox().SetBackgroundColor(theme.Bg())
	midSpacer := tview.NewBox().SetBackgroundColor(theme.Bg())
	bottomSpacer := tview.NewBox().SetBackgroundColor(theme.Bg())

	logoContainer := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(leftSpacer, 0, 1, false).
		AddItem(logoText, 56, 0, false).
		AddItem(rightSpacer, 0, 1, false)
	logoContainer.SetBackgroundColor(theme.Bg())

	// Status display
	statusText := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)
	statusText.SetBackgroundColor(theme.Bg())

	// Sponsor line
	sponsorText := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)
	sponsorText.SetBackgroundColor(theme.Bg())
	sponsorText.SetText(fmt.Sprintf(
		"[%s]Made with %s by getgalaxy.io[-]",
		theme.TagFgDim(), theme.IconHeart,
	))

	// Layout
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topSpacer, 0, 1, false).
		AddItem(logoContainer, 11, 0, false).
		AddItem(statusText, 3, 0, false).
		AddItem(midSpacer, 1, 0, false).
		AddItem(sponsorText, 1, 0, false).
		AddItem(bottomSpacer, 0, 1, false)
	flex.SetBackgroundColor(theme.Bg())

	// Sync
	var provider gnatnats.Provider
	var connErr error
	var mu sync.Mutex
	quit := make(chan struct{})
	done := make(chan struct{})
	appRunning := make(chan struct{})

	setStatusText := func(msg string, isError bool) {
		color := theme.TagAccent()
		if isError {
			color = theme.TagError()
		}
		statusText.SetText(fmt.Sprintf(
			"[%s]%s[-]\n[%s]Press 'q' to quit[-]",
			color, msg,
			theme.TagFgDim(),
		))
	}

	updateStatus := func(msg string, isError bool) {
		app.QueueUpdateDraw(func() {
			setStatusText(msg, isError)
		})
	}

	// Connection goroutine
	go func() {
		defer close(done)

		// Wait for the tview app to be running
		select {
		case <-appRunning:
			logger.Debugf("splash app running")
		case <-quit:
			mu.Lock()
			connErr = fmt.Errorf("cancelled")
			mu.Unlock()
			return
		}

		// Brief splash pause
		select {
		case <-quit:
			mu.Lock()
			connErr = fmt.Errorf("cancelled")
			mu.Unlock()
			return
		case <-time.After(500 * time.Millisecond):
		}

		backoff := initialBackoff
		for attempt := 1; attempt <= maxRetries; attempt++ {
			select {
			case <-quit:
				mu.Lock()
				connErr = fmt.Errorf("cancelled")
				mu.Unlock()
				return
			default:
			}

			logger.Debugf("connection attempt %d/%d to %s", attempt, maxRetries, cfg.URL)
			updateStatus(fmt.Sprintf("Connecting to %s... (attempt %d/%d)", cfg.URL, attempt, maxRetries), false)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			client, err := gnatnats.Connect(ctx, cfg)
			cancel()

			if err == nil {
				logger.Debugf("connection successful")
				mu.Lock()
				provider = client
				mu.Unlock()
				updateStatus("Connected!", false)
				time.Sleep(1 * time.Second)
				app.Stop()
				return
			}

			logger.Debugf("connection attempt %d failed: %v", attempt, err)

			if attempt < maxRetries {
				updateStatus(fmt.Sprintf("Connection failed: %v\nRetrying in %v...", err, backoff), true)
				select {
				case <-quit:
					mu.Lock()
					connErr = fmt.Errorf("cancelled")
					mu.Unlock()
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, maxBackoff)
			} else {
				mu.Lock()
				connErr = fmt.Errorf("failed to connect after %d attempts: %w", maxRetries, err)
				mu.Unlock()
				updateStatus(fmt.Sprintf("Connection failed: %v\n\nMax retries exceeded. Press 'q' to exit.", err), true)
			}
		}

		// Wait for user to quit after max retries
		<-quit
	}()

	// Quit handler
	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == 'q' || event.Key() == tcell.KeyCtrlC {
			select {
			case <-quit:
			default:
				close(quit)
			}
			app.Stop()
			return nil
		}
		return event
	})

	// Set initial status before app runs
	setStatusText("Initializing...", false)

	// Signal when app is running (after first draw) — matches tempo pattern
	var appStartOnce sync.Once
	app.SetAfterDrawFunc(func(screen tcell.Screen) {
		appStartOnce.Do(func() {
			logger.Debugf("splash app: first draw complete, signaling ready")
			close(appRunning)
		})
	})

	// Run splash UI
	app.SetRoot(flex, true)
	logger.Debugf("starting splash event loop")
	if err := app.Run(); err != nil {
		return nil, fmt.Errorf("UI error: %w", err)
	}
	logger.Debugf("splash event loop exited")

	// Wait for connection goroutine
	<-done

	mu.Lock()
	defer mu.Unlock()

	if connErr != nil {
		return nil, connErr
	}

	return provider, nil
}
