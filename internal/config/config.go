package config

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// CommandOutputType defines how command output should be displayed.
type CommandOutputType string

const (
	OutputLog       CommandOutputType = "log"
	OutputJSON      CommandOutputType = "json"
	OutputStreams   CommandOutputType = "streams"
	OutputConsumers CommandOutputType = "consumers"
)

// CommandConfig defines a user-configured command.
type CommandConfig struct {
	Description string            `yaml:"description,omitempty"`
	Cmd         string            `yaml:"cmd"`
	Output      CommandOutputType `yaml:"output,omitempty"`
	Confirm     bool              `yaml:"confirm,omitempty"`
}

// TLSConfig holds TLS connection settings for NATS.
type TLSConfig struct {
	Cert       string `yaml:"cert,omitempty"`
	Key        string `yaml:"key,omitempty"`
	CA         string `yaml:"ca,omitempty"`
	ServerName string `yaml:"server_name,omitempty"`
	SkipVerify bool   `yaml:"skip_verify,omitempty"`
}

// ConnectionConfig holds NATS connection settings.
type ConnectionConfig struct {
	URL         string                      `yaml:"url"`
	TLS         TLSConfig                   `yaml:"tls,omitempty"`
	Credentials string                      `yaml:"credentials,omitempty"` // Path to .creds file
	Token       string                      `yaml:"token,omitempty"`
	User        string                      `yaml:"user,omitempty"`
	Password    string                      `yaml:"password,omitempty"`
	NKey        string                      `yaml:"nkey,omitempty"` // Path to nkey seed file
	Domain      string                      `yaml:"domain,omitempty"` // JetStream domain
	Commands    map[string]CommandConfig     `yaml:"commands,omitempty"`
}

// ExpandEnv expands environment variables in sensitive fields.
func (c ConnectionConfig) ExpandEnv() ConnectionConfig {
	return ConnectionConfig{
		URL:         c.URL,
		TLS:         c.TLS,
		Credentials: os.ExpandEnv(c.Credentials),
		Token:       os.ExpandEnv(c.Token),
		User:        c.User,
		Password:    os.ExpandEnv(c.Password),
		NKey:        os.ExpandEnv(c.NKey),
		Domain:      c.Domain,
	}
}

// Bookmark represents a saved resource shortcut.
type Bookmark struct {
	Type   string `yaml:"type"`             // "stream", "consumer", "kv", "object"
	Name   string `yaml:"name"`
	Stream string `yaml:"stream,omitempty"` // for consumers
}

// Config represents the application configuration.
type Config struct {
	Theme         string                      `yaml:"theme"`
	ActiveProfile string                      `yaml:"active_profile,omitempty"`
	Profiles      map[string]ConnectionConfig `yaml:"profiles,omitempty"`
	Commands      map[string]CommandConfig     `yaml:"commands,omitempty"`
	Bookmarks     []Bookmark                   `yaml:"bookmarks,omitempty"`
}

// DefaultConfig returns a config with default values.
func DefaultConfig() *Config {
	return &Config{
		Theme:         DefaultTheme,
		ActiveProfile: "default",
		Profiles: map[string]ConnectionConfig{
			"default": {
				URL: "nats://localhost:4222",
			},
		},
	}
}

// Load reads the config file from disk.
func Load() (*Config, error) {
	path := ConfigPath()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DefaultConfig(), nil
		}
		return nil, fmt.Errorf("reading config: %w", err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.ensureDefaults()
	return cfg, nil
}

func (c *Config) ensureDefaults() {
	if c.Profiles == nil || len(c.Profiles) == 0 {
		c.Profiles = map[string]ConnectionConfig{
			"default": {URL: "nats://localhost:4222"},
		}
		c.ActiveProfile = "default"
	}

	if c.ActiveProfile == "" {
		for name := range c.Profiles {
			c.ActiveProfile = name
			break
		}
	} else if _, ok := c.Profiles[c.ActiveProfile]; !ok {
		for name := range c.Profiles {
			c.ActiveProfile = name
			break
		}
	}
}

// Save writes the config to disk.
func (c *Config) Save() error {
	if err := EnsureConfigDir(); err != nil {
		return fmt.Errorf("creating config dir: %w", err)
	}

	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	path := ConfigPath()
	return os.WriteFile(path, data, 0644)
}

// GetProfile returns a profile by name.
func (c *Config) GetProfile(name string) (ConnectionConfig, bool) {
	if c.Profiles == nil {
		return ConnectionConfig{}, false
	}
	profile, ok := c.Profiles[name]
	return profile, ok
}

// GetActiveProfile returns the active profile name and its configuration.
func (c *Config) GetActiveProfile() (string, ConnectionConfig) {
	if c.Profiles == nil || c.ActiveProfile == "" {
		return "default", ConnectionConfig{URL: "nats://localhost:4222"}
	}
	profile, ok := c.Profiles[c.ActiveProfile]
	if !ok {
		for name, cfg := range c.Profiles {
			return name, cfg
		}
		return "default", ConnectionConfig{URL: "nats://localhost:4222"}
	}
	return c.ActiveProfile, profile
}

// SetActiveProfile sets the active profile by name.
func (c *Config) SetActiveProfile(name string) error {
	if c.Profiles == nil {
		return fmt.Errorf("no profiles configured")
	}
	if _, ok := c.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}
	c.ActiveProfile = name
	return nil
}

// SaveProfile saves or updates a profile.
func (c *Config) SaveProfile(name string, cfg ConnectionConfig) {
	if c.Profiles == nil {
		c.Profiles = make(map[string]ConnectionConfig)
	}
	c.Profiles[name] = cfg
}

// DeleteProfile deletes a profile by name.
func (c *Config) DeleteProfile(name string) error {
	if c.Profiles == nil {
		return fmt.Errorf("profile %q not found", name)
	}
	if _, ok := c.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}
	if c.ActiveProfile == name {
		return fmt.Errorf("cannot delete active profile %q", name)
	}
	delete(c.Profiles, name)
	return nil
}

// ListProfiles returns a sorted list of profile names.
func (c *Config) ListProfiles() []string {
	if c.Profiles == nil {
		return nil
	}
	names := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ProfileExists returns true if a profile with the given name exists.
func (c *Config) ProfileExists(name string) bool {
	if c.Profiles == nil {
		return false
	}
	_, ok := c.Profiles[name]
	return ok
}

// GetMergedCommands returns global commands merged with profile-specific commands.
// Profile commands override global commands with the same name.
func (c *Config) GetMergedCommands(profileName string) map[string]CommandConfig {
	merged := make(map[string]CommandConfig)
	for name, cmd := range c.Commands {
		merged[name] = cmd
	}
	if profile, ok := c.Profiles[profileName]; ok {
		for name, cmd := range profile.Commands {
			merged[name] = cmd
		}
	}
	return merged
}

// AddBookmark adds a bookmark if it doesn't already exist.
func (c *Config) AddBookmark(b Bookmark) bool {
	for _, existing := range c.Bookmarks {
		if existing.Type == b.Type && existing.Name == b.Name && existing.Stream == b.Stream {
			return false // already exists
		}
	}
	c.Bookmarks = append(c.Bookmarks, b)
	return true
}

// RemoveBookmark removes a bookmark by index.
func (c *Config) RemoveBookmark(index int) {
	if index < 0 || index >= len(c.Bookmarks) {
		return
	}
	c.Bookmarks = append(c.Bookmarks[:index], c.Bookmarks[index+1:]...)
}

// RemoveBookmarkMatch removes a bookmark that matches the given bookmark's type, name, and stream.
func (c *Config) RemoveBookmarkMatch(b Bookmark) {
	for i, existing := range c.Bookmarks {
		if existing.Type == b.Type && existing.Name == b.Name && existing.Stream == b.Stream {
			c.RemoveBookmark(i)
			return
		}
	}
}

// ListCommandNames returns a sorted list of all command names for a profile.
func (c *Config) ListCommandNames(profileName string) []string {
	merged := c.GetMergedCommands(profileName)
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

