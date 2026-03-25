package view

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/galaxy-io/gnat/internal/clipboard"
	"github.com/galaxy-io/gnat/internal/config"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"golang.org/x/term"
)

// Version is set via ldflags at build time.
var Version = "dev"

const pons = `
               _                       __
              /   \                  /      \
             '      \              /          \
            |       |Oo          o|            |
            \    \  |OOOo......oOO|   /        |
             \    \\OOOOOOOOOOOOOOO\//        /
               \ _o\OOOOOOOOOOOOOOOO//. ___ /
           ______OOOOOOOOOOOOOOOOOOOOOOOo.___
            --- OO'* *OOOOOOOOOO'*   OOOOO--
                OO.   OOOOOOOOO'    .OOOOO o
                \OOOooOOOOOOOOOooooOOOOOO'OOOo
              .OO "OOOOOOOOOOOOOOOOOOOO"OOOOOOOo
          __ OOOOOOOOOOOOOOOOOOOOOO"OOOOOOOOOOOOo
         ___OOOOOOOO_"OOOOOOOOOOO"_OOOOOOOOOOOOOOOO
           OOOOO^OOOO0-(____)/OOOOOOOOOOOOO^OOOOOO
           OOOOO OO000/00||00\000000OOOOOOOO OOOOOO
           OOOOO O0000000000000000 ppppoooooOOOOOO
            OOOOO 0000000000000000 QQQQ "OOOOOOO"
            o"OOOO 000000000000000oooooOOoooooooO'
            OOo"OOOO.00000000000000000000OOOOOOOO'
           OOOOOO QQQQ 0000000000000000000OOOOOOO
          OOOOOO00eeee00000000000000000000OOOOOOOO.
         OOOOOOOO000000000000000000000000OOOOOOOOOO
         OOOOOOOOO00000000000000000000000OOOOOOOOOO
          OOOOOOOOO000000000000000000000OOOOOOOOOOO
           "OOOOOOOO0000000000000000000OOOOOOOOOOO'
             "OOOOOOO00000000000000000OOOOOOOOOO"
  .ooooOOOOOOOo"OOOOOOO000000000000OOOOOOOOOOO"
.OOO"""""""""".oOOOOOOOOOOOOOOOOOOOOOOOOOOOOo
OOO         QQQQO"'                      "QQQQ
OOO
 OOo.
  "OOOOOOOOOOOOoooooooo....
`

// DebugData holds debug information for display.
type DebugData struct {
	Version      string
	Commit       string
	BuildDate    string
	OS           string
	Arch         string
	GoVersion    string
	TerminalCols int
	TerminalRows int
	Term         string
	ColorTerm    string
	TermProgram  string
	ColorSpace   string
	ConfigPath   string
	ThemeName    string
	ProfileName  string
	ServerAddress string
	TLSEnabled   bool
	TLSCertPath  string
	TLSKeyPath   string
	TLSCAPath    string
	Domain       string
	Credentials  string
}

// DebugScreen displays environment and debug information.
type DebugScreen struct {
	*tview.Flex
	panel   *components.Panel
	content *tview.TextView
	pons    *tview.TextView
	inner   *tview.Flex
	data    DebugData
	app     *App
}

// NewDebugScreen creates a new debug screen view.
func NewDebugScreen(app *App, data DebugData) *DebugScreen {
	ds := &DebugScreen{
		Flex:    tview.NewFlex().SetDirection(tview.FlexColumn),
		content: tview.NewTextView(),
		pons:    tview.NewTextView(),
		inner:   tview.NewFlex().SetDirection(tview.FlexColumn),
		data:    data,
		app:     app,
	}
	ds.setup()
	theme.RegisterRefreshable(ds)
	return ds
}

func (ds *DebugScreen) setup() {
	ds.SetBackgroundColor(theme.Bg())

	ds.content.SetDynamicColors(true)
	ds.content.SetBackgroundColor(theme.Bg())
	ds.content.SetTextColor(theme.Fg())
	ds.content.SetWordWrap(true)
	ds.content.SetScrollable(true)

	ds.pons.SetDynamicColors(true)
	ds.pons.SetBackgroundColor(theme.Bg())
	ds.pons.SetTextColor(theme.Fg())
	ds.pons.SetTextAlign(tview.AlignLeft)

	ds.inner.SetBackgroundColor(theme.Bg())
	ds.inner.AddItem(ds.content, 0, 1, true)
	ds.inner.AddItem(ds.pons, 55, 0, false)

	ds.panel = components.NewPanel().SetTitle(fmt.Sprintf("%s Debug Info", theme.IconInfo))
	ds.panel.SetContent(ds.inner)

	ds.AddItem(ds.panel, 0, 1, true)

	ds.renderContent()
	ds.renderRat()
}

func (ds *DebugScreen) renderContent() {
	dim := theme.TagFgDim()
	fg := theme.TagFg()
	accent := theme.TagAccent()
	success := theme.TagSuccess()
	warn := theme.TagWarning()

	var text string

	text += fmt.Sprintf("[%s::b]VERSION[-:-:-]\n", accent)
	text += fmt.Sprintf("  [%s]gnat:[-]   [%s]%s[-]\n", dim, fg, ds.data.Version)
	text += fmt.Sprintf("  [%s]commit:[-]  [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.Commit))
	text += fmt.Sprintf("  [%s]built:[-]   [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.BuildDate))
	text += "\n"

	text += fmt.Sprintf("[%s::b]SYSTEM[-:-:-]\n", accent)
	text += fmt.Sprintf("  [%s]os:[-]      [%s]%s[-]\n", dim, fg, ds.data.OS)
	text += fmt.Sprintf("  [%s]arch:[-]    [%s]%s[-]\n", dim, fg, ds.data.Arch)
	text += fmt.Sprintf("  [%s]go:[-]      [%s]%s[-]\n", dim, fg, ds.data.GoVersion)
	text += "\n"

	text += fmt.Sprintf("[%s::b]TERMINAL[-:-:-]\n", accent)
	text += fmt.Sprintf("  [%s]size:[-]        [%s]%dx%d[-]\n", dim, fg, ds.data.TerminalCols, ds.data.TerminalRows)
	text += fmt.Sprintf("  [%s]term:[-]        [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.Term))
	text += fmt.Sprintf("  [%s]colorterm:[-]   [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.ColorTerm))
	text += fmt.Sprintf("  [%s]term_program:[-][%s] %s[-]\n", dim, fg, valueOrDash(ds.data.TermProgram))
	text += fmt.Sprintf("  [%s]color_space:[-] [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.ColorSpace))
	text += "\n"

	text += fmt.Sprintf("[%s::b]CONFIG[-:-:-]\n", accent)
	text += fmt.Sprintf("  [%s]path:[-]   [%s]%s[-]\n", dim, fg, ds.data.ConfigPath)
	text += fmt.Sprintf("  [%s]theme:[-]  [%s]%s[-]\n", dim, fg, ds.data.ThemeName)
	text += "\n"

	text += fmt.Sprintf("[%s::b]PROFILE[-:-:-]\n", accent)
	text += fmt.Sprintf("  [%s]active:[-]      [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.ProfileName))
	text += fmt.Sprintf("  [%s]address:[-]     [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.ServerAddress))
	text += fmt.Sprintf("  [%s]domain:[-]      [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.Domain))
	text += fmt.Sprintf("  [%s]credentials:[-] [%s]%s[-]\n", dim, fg, valueOrDash(ds.data.Credentials))

	if ds.data.TLSEnabled {
		text += fmt.Sprintf("  [%s]tls:[-]         [%s]enabled[-]\n", dim, success)
		if ds.data.TLSCertPath != "" {
			text += fmt.Sprintf("  [%s]tls_cert:[-]    [%s]%s[-]\n", dim, fg, ds.data.TLSCertPath)
		}
		if ds.data.TLSKeyPath != "" {
			text += fmt.Sprintf("  [%s]tls_key:[-]     [%s]%s[-]\n", dim, fg, ds.data.TLSKeyPath)
		}
		if ds.data.TLSCAPath != "" {
			text += fmt.Sprintf("  [%s]tls_ca:[-]      [%s]%s[-]\n", dim, fg, ds.data.TLSCAPath)
		}
	} else {
		text += fmt.Sprintf("  [%s]tls:[-]         [%s]disabled[-]\n", dim, warn)
	}

	ds.content.SetText(text)
}

func (ds *DebugScreen) renderRat() {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var text string
	text += fmt.Sprintf("[%s]%s[-]\n", dim, pons)
	text += fmt.Sprintf("[%s::b]I'm sorry 󰋔[-:-:-]\n", accent)
	text += fmt.Sprintf("[%s]This information helps me debug and solve issues faster[-]\n\n", dim)
	text += fmt.Sprintf("[%s]y[-] yank report\n", accent)
	text += fmt.Sprintf("[%s]Y[-] yank issue template\n", accent)

	ds.pons.SetText(text)
}

func valueOrDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func redactPath(s string) string {
	if s == "" {
		return ""
	}
	return "(set)"
}

// RefreshTheme updates all component colors after a theme change.
func (ds *DebugScreen) RefreshTheme() {
	bg := theme.Bg()
	ds.SetBackgroundColor(bg)
	ds.inner.SetBackgroundColor(bg)
	ds.content.SetBackgroundColor(bg)
	ds.content.SetTextColor(theme.Fg())
	ds.pons.SetBackgroundColor(bg)
	ds.pons.SetTextColor(theme.Fg())
	ds.renderContent()
	ds.renderRat()
}

// GeneratePlainReport generates a plain text debug report.
func (ds *DebugScreen) GeneratePlainReport() string {
	var sb strings.Builder

	sb.WriteString("=== Gnat Debug Info ===\n\n")

	sb.WriteString("VERSION\n")
	sb.WriteString(fmt.Sprintf("  gnat: %s\n", ds.data.Version))
	sb.WriteString(fmt.Sprintf("  commit: %s\n", valueOrDash(ds.data.Commit)))
	sb.WriteString(fmt.Sprintf("  built: %s\n", valueOrDash(ds.data.BuildDate)))
	sb.WriteString("\n")

	sb.WriteString("SYSTEM\n")
	sb.WriteString(fmt.Sprintf("  os: %s\n", ds.data.OS))
	sb.WriteString(fmt.Sprintf("  arch: %s\n", ds.data.Arch))
	sb.WriteString(fmt.Sprintf("  go: %s\n", ds.data.GoVersion))
	sb.WriteString("\n")

	sb.WriteString("TERMINAL\n")
	sb.WriteString(fmt.Sprintf("  size: %dx%d\n", ds.data.TerminalCols, ds.data.TerminalRows))
	sb.WriteString(fmt.Sprintf("  term: %s\n", valueOrDash(ds.data.Term)))
	sb.WriteString(fmt.Sprintf("  colorterm: %s\n", valueOrDash(ds.data.ColorTerm)))
	sb.WriteString(fmt.Sprintf("  term_program: %s\n", valueOrDash(ds.data.TermProgram)))
	sb.WriteString(fmt.Sprintf("  color_space: %s\n", valueOrDash(ds.data.ColorSpace)))
	sb.WriteString("\n")

	sb.WriteString("CONFIG\n")
	sb.WriteString(fmt.Sprintf("  path: %s\n", ds.data.ConfigPath))
	sb.WriteString(fmt.Sprintf("  theme: %s\n", ds.data.ThemeName))
	sb.WriteString("\n")

	sb.WriteString("PROFILE\n")
	sb.WriteString(fmt.Sprintf("  active: %s\n", valueOrDash(ds.data.ProfileName)))
	sb.WriteString(fmt.Sprintf("  address: %s\n", valueOrDash(ds.data.ServerAddress)))
	sb.WriteString(fmt.Sprintf("  domain: %s\n", valueOrDash(ds.data.Domain)))
	sb.WriteString(fmt.Sprintf("  credentials: %s\n", valueOrDash(ds.data.Credentials)))
	if ds.data.TLSEnabled {
		sb.WriteString("  tls: enabled\n")
	} else {
		sb.WriteString("  tls: disabled\n")
	}

	return sb.String()
}

// GenerateIssueTemplate generates a GitHub issue template with debug info.
func (ds *DebugScreen) GenerateIssueTemplate() string {
	var sb strings.Builder

	sb.WriteString("## Bug Report\n\n")
	sb.WriteString("### Description\n")
	sb.WriteString("<!-- Describe what happened -->\n\n")
	sb.WriteString("### Expected Behavior\n")
	sb.WriteString("<!-- What did you expect to happen? -->\n\n")
	sb.WriteString("### Steps to Reproduce\n")
	sb.WriteString("1. \n2. \n3. \n\n")
	sb.WriteString("### Environment\n")
	sb.WriteString("```\n")
	sb.WriteString(ds.GeneratePlainReport())
	sb.WriteString("```\n")

	return sb.String()
}

// Name returns the view name.
func (ds *DebugScreen) Name() string {
	return "Debug"
}

// Start is called when the view becomes active.
func (ds *DebugScreen) Start() {}

// Stop is called when the view is deactivated.
func (ds *DebugScreen) Stop() {}

// Hints returns keybinding hints for this view.
func (ds *DebugScreen) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "y", Description: "Yank report"},
		{Key: "Y", Description: "Yank issue"},
	}
}

// InputHandler returns the input handler for the debug screen.
func (ds *DebugScreen) InputHandler() func(*tcell.EventKey, func(tview.Primitive)) {
	return ds.WrapInputHandler(func(event *tcell.EventKey, setFocus func(tview.Primitive)) {
		switch event.Rune() {
		case 'y':
			report := ds.GeneratePlainReport()
			if err := clipboard.Copy(report); err != nil {
				ds.app.ShowError("Failed to copy: " + err.Error())
			} else {
				ds.app.ShowSuccess("Report copied to clipboard!")
			}
		case 'Y':
			tmpl := ds.GenerateIssueTemplate()
			if err := clipboard.Copy(tmpl); err != nil {
				ds.app.ShowError("Failed to copy: " + err.Error())
			} else {
				ds.app.ShowSuccess("Issue template copied to clipboard!")
			}
		default:
			if handler := ds.content.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

// collectDebugData gathers debug information from the app at call time.
func collectDebugData(a *App) DebugData {
	data := DebugData{
		Version:   Version,
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		GoVersion: runtime.Version(),
		Term:      os.Getenv("TERM"),
		ColorTerm: os.Getenv("COLORTERM"),
		TermProgram: os.Getenv("TERM_PROGRAM"),
		ColorSpace:  os.Getenv("ITERM_PROFILE"),
		ConfigPath: config.ConfigPath(),
		ThemeName:  a.cfg.Theme,
	}

	// Terminal size
	if w, h, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		data.TerminalCols = w
		data.TerminalRows = h
	}

	// Profile info
	profileName, profileCfg := a.cfg.GetActiveProfile()
	data.ProfileName = profileName
	expanded := profileCfg.ExpandEnv()
	data.ServerAddress = expanded.URL
	data.Domain = expanded.Domain
	data.Credentials = redactPath(expanded.Credentials)
	data.TLSCertPath = redactPath(expanded.TLS.Cert)
	data.TLSKeyPath = redactPath(expanded.TLS.Key)
	data.TLSCAPath = redactPath(expanded.TLS.CA)
	data.TLSEnabled = expanded.TLS.Cert != "" || expanded.TLS.CA != ""

	// Fall back to live server URL
	if data.ServerAddress == "" {
		provider := a.Provider()
		if provider != nil {
			data.ServerAddress = provider.ServerURL()
		}
	}

	return data
}
