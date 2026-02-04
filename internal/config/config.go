package config

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

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
	URL         string    `yaml:"url"`
	TLS         TLSConfig `yaml:"tls,omitempty"`
	Credentials string    `yaml:"credentials,omitempty"` // Path to .creds file
	Token       string    `yaml:"token,omitempty"`
	User        string    `yaml:"user,omitempty"`
	Password    string    `yaml:"password,omitempty"`
	NKey        string    `yaml:"nkey,omitempty"` // Path to nkey seed file
	Domain      string    `yaml:"domain,omitempty"` // JetStream domain
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

// Config represents the application configuration.
type Config struct {
	Theme         string                      `yaml:"theme"`
	ActiveProfile string                      `yaml:"active_profile,omitempty"`
	Profiles      map[string]ConnectionConfig `yaml:"profiles,omitempty"`
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

