package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	mu      sync.Mutex
	logger  *log.Logger
	file    *os.File
	enabled bool
)

func init() {
	logger = log.New(io.Discard, "", 0)
}

// Init initializes the debug logger. When debug is true, logs are written to
// a timestamped file in the user's config directory (e.g. ~/.config/gnat/debug-<timestamp>.log).
// When debug is false, all log calls are no-ops.
func Init(debug bool) (string, error) {
	mu.Lock()
	defer mu.Unlock()

	enabled = debug
	if !debug {
		return "", nil
	}

	dir, err := os.UserConfigDir()
	if err != nil {
		dir = "."
	}
	logDir := filepath.Join(dir, "gnat")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return "", fmt.Errorf("create log dir: %w", err)
	}

	name := fmt.Sprintf("debug-%s.log", time.Now().Format("20060102-150405"))
	path := filepath.Join(logDir, name)

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("create log file: %w", err)
	}
	file = f
	logger = log.New(f, "", log.Ldate|log.Ltime|log.Lmicroseconds)
	logger.Printf("gnat debug log started")

	return path, nil
}

// Close closes the log file if open.
func Close() {
	mu.Lock()
	defer mu.Unlock()
	if file != nil {
		file.Close()
		file = nil
	}
}

// Debugf logs a formatted message when debug mode is enabled.
func Debugf(format string, args ...any) {
	if !enabled {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	logger.Printf(format, args...)
}
