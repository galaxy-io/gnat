package view

import (
	"fmt"

	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/atterpac/dado/theme"
	"github.com/galaxy-io/gnat/internal/config"
	"github.com/gdamore/tcell/v2"
)

// ProfileModal displays a list of connection profiles for selection.
type ProfileModal struct {
	*components.Modal
	table    *components.Table
	profiles []string
	active   string
	onSelect func(string)
	onNew    func()
	onEdit   func(string)
	onDelete func(string)
	onClose  func()
}

// NewProfileModal creates a new profile selector modal.
func NewProfileModal(cfg *config.Config, onSelect func(string), onNew, onClose func(), onEdit func(string), onDelete func(string)) *ProfileModal {
	pm := &ProfileModal{
		profiles: cfg.ListProfiles(),
		active:   cfg.ActiveProfile,
		onSelect: onSelect,
		onNew:    onNew,
		onEdit:   onEdit,
		onDelete: onDelete,
		onClose:  onClose,
	}

	pm.table = components.NewTable().
		SetHeaders("", "PROFILE", "SERVER")

	pm.populateTable(cfg)

	content := core.NewFlex().SetDirection(core.Column).
		AddItem(pm.table, 0, 1, true)
	content.SetBackgroundColor(theme.Get().Bg())

	pm.Modal = components.NewModal(components.ModalConfig{
		Title:    "Connection Profiles",
		Width:    60,
		Height:   15,
		Backdrop: false,
	}).SetContent(content)

	pm.Modal.SetHints([]components.KeyHint{
		{Key: "Enter", Description: "Switch"},
		{Key: "n", Description: "New"},
		{Key: "e", Description: "Edit"},
		{Key: "d", Description: "Delete"},
		{Key: "Esc", Description: "Close"},
	})

	return pm
}

func (pm *ProfileModal) Name() string { return "Profiles" }
func (pm *ProfileModal) Start()       {}
func (pm *ProfileModal) Stop()        {}

func (pm *ProfileModal) populateTable(cfg *config.Config) {
	pm.table.ClearRows()
	t := theme.Get()

	for _, name := range pm.profiles {
		profile, _ := cfg.GetProfile(name)

		marker := " "
		if name == pm.active {
			marker = ""
		}

		server := profile.URL
		if len(server) > 30 {
			server = server[:27] + "..."
		}

		pm.table.AddColoredRow(
			[]string{marker, name, server},
			[]tcell.Color{t.Accent(), t.Fg(), t.FgDim()},
		)
	}

	if len(pm.profiles) > 0 {
		pm.table.SelectRow(0)
	}
}

func (pm *ProfileModal) HandleKey(event *tcell.EventKey) bool {
	switch event.Key() {
	case tcell.KeyEnter:
		idx := pm.table.SelectedRow()
		if idx >= 0 && idx < len(pm.profiles) && pm.onSelect != nil {
			pm.onSelect(pm.profiles[idx])
		}
		return true
	case tcell.KeyEscape:
		if pm.onClose != nil {
			pm.onClose()
		}
		return true
	}

	switch event.Rune() {
	case 'n':
		if pm.onNew != nil {
			pm.onNew()
		}
		return true
	case 'e':
		idx := pm.table.SelectedRow()
		if idx >= 0 && idx < len(pm.profiles) && pm.onEdit != nil {
			pm.onEdit(pm.profiles[idx])
		}
		return true
	case 'd':
		idx := pm.table.SelectedRow()
		if idx >= 0 && idx < len(pm.profiles) && pm.onDelete != nil {
			pm.onDelete(pm.profiles[idx])
		}
		return true
	case 'q':
		if pm.onClose != nil {
			pm.onClose()
		}
		return true
	}

	return pm.table.HandleKey(event)
}

// ProfileForm is a modal form for creating/editing a profile.
type ProfileForm struct {
	*components.Modal
	form     *components.Form
	isEdit   bool
	editName string
	onSave   func(string, config.ConnectionConfig)
	onCancel func()
}

// NewProfileForm creates a new profile form modal.
func NewProfileForm(name string, cfg config.ConnectionConfig, isEdit bool, onSave func(string, config.ConnectionConfig), onCancel func()) *ProfileForm {
	pf := &ProfileForm{
		isEdit:   isEdit,
		editName: name,
		onSave:   onSave,
		onCancel: onCancel,
	}

	// Set defaults
	serverURL := cfg.URL
	if serverURL == "" {
		serverURL = "nats://localhost:4222"
	}

	// Build form using FormBuilder
	builder := components.NewFormBuilder()

	if !isEdit {
		builder.Text("name", "Profile Name").Value(name).Placeholder("my-profile").Done()
	}

	builder.Text("url", "Server URL").Value(serverURL).Placeholder("nats://localhost:4222").Done()
	builder.Text("domain", "Domain").Value(cfg.Domain).Placeholder("optional").Done()
	builder.Text("credentials", "Credentials File").Value(cfg.Credentials).Placeholder("/path/to/creds").Done()
	builder.Text("token", "Token").Value(cfg.Token).Placeholder("optional").Done()
	builder.Text("user", "User").Value(cfg.User).Placeholder("optional").Done()
	builder.Password("password", "Password").Value(cfg.Password).Done()
	builder.Text("nkey", "NKey File").Value(cfg.NKey).Placeholder("/path/to/nkey").Done()
	builder.Text("tls_cert", "TLS Cert").Value(cfg.TLS.Cert).Placeholder("/path/to/cert").Done()
	builder.Text("tls_key", "TLS Key").Value(cfg.TLS.Key).Placeholder("/path/to/key").Done()
	builder.Text("tls_ca", "TLS CA").Value(cfg.TLS.CA).Placeholder("/path/to/ca").Done()

	builder.OnSubmit(func(values map[string]any) {
		newCfg := config.ConnectionConfig{
			URL:         getString(values, "url"),
			Domain:      getString(values, "domain"),
			Credentials: getString(values, "credentials"),
			Token:       getString(values, "token"),
			User:        getString(values, "user"),
			Password:    getString(values, "password"),
			NKey:        getString(values, "nkey"),
			TLS: config.TLSConfig{
				Cert: getString(values, "tls_cert"),
				Key:  getString(values, "tls_key"),
				CA:   getString(values, "tls_ca"),
			},
		}
		saveName := getString(values, "name")
		if isEdit {
			saveName = pf.editName
		}
		if pf.onSave != nil {
			pf.onSave(saveName, newCfg)
		}
	})

	builder.OnCancel(func() {
		if pf.onCancel != nil {
			pf.onCancel()
		}
	})

	pf.form = builder.Build()

	title := "New Profile"
	if isEdit {
		title = fmt.Sprintf("Edit Profile: %s", name)
	}

	pf.Modal = components.NewModal(components.ModalConfig{
		Title:    title,
		Width:    70,
		Height:   22,
		Backdrop: false,
	}).SetContent(pf.form)

	pf.Modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next field"},
		{Key: "Enter", Description: "Submit"},
		{Key: "Esc", Description: "Cancel"},
	})

	return pf
}

func (pf *ProfileForm) Name() string { return "Profile" }
func (pf *ProfileForm) Start()       {}
func (pf *ProfileForm) Stop()        {}

// getString safely extracts a string from the values map.
func getString(values map[string]any, key string) string {
	if v, ok := values[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (pf *ProfileForm) HandleKey(event *tcell.EventKey) bool {
	if event.Key() == tcell.KeyEscape {
		if pf.onCancel != nil {
			pf.onCancel()
		}
		return true
	}
	return pf.form.HandleKey(event)
}
