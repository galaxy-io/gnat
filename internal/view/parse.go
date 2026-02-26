package view

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return -1, nil
	}

	s = strings.ToUpper(s)

	multipliers := map[string]int64{
		"TB": 1024 * 1024 * 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
		"MB": 1024 * 1024,
		"KB": 1024,
		"B":  1,
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, suffix))
			n, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid byte value: %s", s)
			}
			if n < 0 {
				return 0, fmt.Errorf("byte value must be positive")
			}
			return int64(n * float64(mult)), nil
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte value: %s", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("byte value must be positive")
	}
	return n, nil
}

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	// Support "7d" notation
	if strings.HasSuffix(s, "d") {
		numStr := strings.TrimSuffix(s, "d")
		n, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid duration: %s", s)
		}
		return time.Duration(n * float64(24*time.Hour)), nil
	}

	return time.ParseDuration(s)
}

// miniSparkline produces a compact sparkline string from a float64 slice.
func miniSparkline(data []float64, width int) string {
	if len(data) == 0 {
		return ""
	}
	sparks := []rune("▁▂▃▄▅▆▇█")
	max := data[0]
	for _, v := range data {
		if v > max {
			max = v
		}
	}
	if max == 0 {
		max = 1
	}

	// Sample data to fit width
	step := len(data) / width
	if step < 1 {
		step = 1
	}
	var result []rune
	for i := 0; i < len(data) && len(result) < width; i += step {
		idx := int(data[i] / max * float64(len(sparks)-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sparks) {
			idx = len(sparks) - 1
		}
		result = append(result, sparks[idx])
	}
	return string(result)
}

func parseOptionalInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return -1, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %s", s)
	}
	return n, nil
}
