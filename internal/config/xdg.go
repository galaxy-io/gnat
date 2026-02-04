package config

import (
	"os"
	"path/filepath"
)

// ConfigDir returns the configuration directory path following XDG spec.
// Priority: $XDG_CONFIG_HOME/gnat > ~/.config/gnat > ~/.gnat
func ConfigDir() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "gnat")
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return ".gnat"
	}

	configDir := filepath.Join(home, ".config")
	if info, err := os.Stat(configDir); err == nil && info.IsDir() {
		return filepath.Join(configDir, "gnat")
	}

	return filepath.Join(home, ".gnat")
}

// ConfigPath returns the full path to the config file.
func ConfigPath() string {
	return filepath.Join(ConfigDir(), "config.yaml")
}

// EnsureConfigDir creates the config directory if it doesn't exist.
func EnsureConfigDir() error {
	return os.MkdirAll(ConfigDir(), 0755)
}
