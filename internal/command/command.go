package command

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

// Context holds the variables available for template expansion.
type Context struct {
	Stream   string
	Consumer string
	Bucket   string
	Subject  string
	Key      string

	ServerURL   string
	Domain      string
	Profile     string
	Credentials string
	Args        []string

	TLSCertPath   string
	TLSKeyPath    string
	TLSCAPath     string
	TLSServerName string
	TLSSkipVerify bool
}

var placeholderRe = regexp.MustCompile(`\{([^}]+)\}`)

// shellQuote wraps a value in single quotes for safe shell expansion.
// Single quotes prevent $variable expansion, globbing, and word splitting.
func shellQuote(s string) string {
	// Single-quote the value; escape any embedded single quotes with '\''
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// ExpandCmd replaces {var} placeholders in a command template.
// Returns an error if a required variable is empty.
func ExpandCmd(template string, ctx Context) (string, error) {
	var expandErr error
	result := placeholderRe.ReplaceAllStringFunc(template, func(match string) string {
		key := match[1 : len(match)-1] // strip { }
		var val string
		switch key {
		case "stream":
			val = ctx.Stream
		case "consumer":
			val = ctx.Consumer
		case "bucket":
			val = ctx.Bucket
		case "subject":
			val = ctx.Subject
		case "key":
			val = ctx.Key
		case "server_url":
			val = ctx.ServerURL
		case "domain":
			val = ctx.Domain
		case "profile":
			val = ctx.Profile
		default:
			// Check positional args: {1}, {2}, etc.
			if n, err := strconv.Atoi(key); err == nil && n >= 1 {
				idx := n - 1
				if idx < len(ctx.Args) {
					val = ctx.Args[idx]
				} else {
					expandErr = fmt.Errorf("positional argument {%s} not provided", key)
					return match
				}
			} else {
				expandErr = fmt.Errorf("unknown variable {%s}", key)
				return match
			}
		}
		if val == "" {
			expandErr = fmt.Errorf("variable {%s} is empty", key)
			return match
		}
		return shellQuote(val)
	})
	if expandErr != nil {
		return "", expandErr
	}
	return result, nil
}

// InjectConnectionFlags appends --server, --creds, and --tls-* flags to
// nats CLI commands so they connect to the active profile's server.
// Only modifies commands that start with "nats ".
func InjectConnectionFlags(cmdStr string, ctx Context) string {
	if !strings.HasPrefix(cmdStr, "nats ") {
		return cmdStr
	}

	var flags []string

	if ctx.ServerURL != "" && !strings.Contains(cmdStr, "--server") && !strings.Contains(cmdStr, "-s ") {
		flags = append(flags, fmt.Sprintf("--server %q", ctx.ServerURL))
	}
	if ctx.Credentials != "" && !strings.Contains(cmdStr, "--creds") {
		flags = append(flags, fmt.Sprintf("--creds %q", ctx.Credentials))
	}
	if ctx.TLSCertPath != "" && !strings.Contains(cmdStr, "--tlscert") {
		flags = append(flags, fmt.Sprintf("--tlscert %q", ctx.TLSCertPath))
	}
	if ctx.TLSKeyPath != "" && !strings.Contains(cmdStr, "--tlskey") {
		flags = append(flags, fmt.Sprintf("--tlskey %q", ctx.TLSKeyPath))
	}
	if ctx.TLSCAPath != "" && !strings.Contains(cmdStr, "--tlsca") {
		flags = append(flags, fmt.Sprintf("--tlsca %q", ctx.TLSCAPath))
	}

	if len(flags) == 0 {
		return cmdStr
	}

	return cmdStr + " " + strings.Join(flags, " ")
}

// Run executes a command string via sh -c and returns combined output.
func Run(ctx context.Context, cmdStr string) (string, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// RunStreaming executes a command with line-by-line streaming via scanner on stdout pipe.
// Stderr is merged into stdout.
func RunStreaming(ctx context.Context, cmdStr string, onLine func(string)) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	cmd.Stderr = nil

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating stdout pipe: %w", err)
	}
	cmd.Stderr = cmd.Stdout // merge stderr into stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting command: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		onLine(scanner.Text())
	}

	return cmd.Wait()
}

// SplitArgs splits a string into arguments, respecting double and single quotes.
// e.g. `pub "hello world" foo` → ["pub", "hello world", "foo"]
func SplitArgs(s string) []string {
	var args []string
	var current strings.Builder
	var quote rune
	escaped := false

	for _, r := range s {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && quote == '"' {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
			continue
		}
		switch r {
		case '"', '\'':
			quote = r
		case ' ', '\t':
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args
}
