package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/atterpac/jig/theme/themes"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/galaxy-io/gnat/internal/config"
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
	)

	flag.StringVar(&flagProfile, "profile", "", "connection profile name")
	flag.StringVar(&flagURL, "url", "", "NATS server URL (overrides profile)")
	flag.StringVar(&flagCreds, "creds", "", "path to credentials file (overrides profile)")
	flag.StringVar(&flagTheme, "theme", "", "color theme (overrides config)")
	flag.BoolVar(&flagVersion, "version", false, "print version and exit")
	flag.Parse()

	if flagVersion {
		fmt.Printf("gnat %s (%s) built %s\n", version, commit, buildDate)
		os.Exit(0)
	}

	// Load config
	cfg, err := config.Load()
	if err != nil {
		cfg = config.DefaultConfig()
	}

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

	// Connect with splash screen
	provider, err := connectWithUI(connCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer provider.Close()

	// Launch main app
	app := view.NewApp(provider, cfg, activeProfile)
	defer app.Close()
	if err := app.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func connectWithUI(cfg config.ConnectionConfig) (gnatnats.Provider, error) {
	app := tview.NewApplication()

	// Create splash using jig's Splash component
	splash := components.NewSplash().
		SetLogo(splashLogo).
		SetLogoWidth(56).
		SetLogoHeight(11).
		SetStatusHeight(6).
		SetGradient(theme.GradientDiagonal).
		SetDismissKeys(nil) // Disable auto-dismiss keys, we handle q/Ctrl+C manually

	// Build splash
	splash.Build()

	// Helper to update splash status with tagline + connection status
	updateStatus := func(connectionStatus string) {
		tagline := fmt.Sprintf("[%s]Made with %s by getgalaxy.io[-]", theme.TagFgDim(), theme.IconHeart)
		splash.SetStatus(connectionStatus + "\n" + tagline)
	}
	updateStatus(fmt.Sprintf("[%s]Initializing...[-]", theme.TagFgDim()))

	type result struct {
		provider gnatnats.Provider
		err      error
	}
	done := make(chan result, 1)
	quit := make(chan struct{})

	app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Rune() == 'q' || event.Key() == tcell.KeyCtrlC {
			close(quit)
			app.Stop()
			return nil
		}
		return event
	})

	go func() {
		time.Sleep(500 * time.Millisecond)

		const maxRetries = 5
		backoff := time.Second

		for attempt := 1; attempt <= maxRetries; attempt++ {
			select {
			case <-quit:
				done <- result{err: fmt.Errorf("cancelled")}
				return
			default:
			}

			app.QueueUpdateDraw(func() {
				updateStatus(fmt.Sprintf("[yellow]Connecting to %s... (attempt %d/%d)[-]", cfg.URL, attempt, maxRetries))
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			client, err := gnatnats.Connect(ctx, cfg)
			cancel()

			if err == nil {
				app.QueueUpdateDraw(func() {
					updateStatus("[green]Connected![-]")
				})
				done <- result{provider: client}
				app.Stop()
				return
			}

			if attempt < maxRetries {
				app.QueueUpdateDraw(func() {
					updateStatus(fmt.Sprintf("[red]Failed: %v[-]\n[dim]Retrying in %s...[-]", err, backoff))
				})
				select {
				case <-quit:
					done <- result{err: fmt.Errorf("cancelled")}
					return
				case <-time.After(backoff):
				}
				backoff = min(backoff*2, 10*time.Second)
			} else {
				app.QueueUpdateDraw(func() {
					updateStatus(fmt.Sprintf("[red]Failed after %d attempts: %v[-]\n[dim]Press 'q' to quit[-]", maxRetries, err))
				})
				<-quit
				done <- result{err: err}
				return
			}
		}
	}()

	app.SetRoot(splash, true)
	if err := app.Run(); err != nil {
		return nil, err
	}

	res := <-done
	return res.provider, res.err
}
