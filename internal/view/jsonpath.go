package view

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// evaluateJSONPath applies a simple path expression to JSON data.
//
// Supported syntax:
//
//	.field          - access object field
//	.field.nested   - nested field access
//	.field[0]       - array index access
//	.[*].name       - iterate all array elements, extract field
//	.field[*]       - iterate all elements of an array field
func evaluateJSONPath(data []byte, path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("empty path")
	}

	var root any
	if err := json.Unmarshal(data, &root); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	segments, err := parseJSONPathSegments(path)
	if err != nil {
		return "", err
	}

	results, err := walkPath(root, segments)
	if err != nil {
		return "", err
	}

	return formatResult(results)
}

type pathSegment interface {
	segType() string
}

type fieldSegment struct {
	name string
}

func (f fieldSegment) segType() string { return "field" }

type indexSegment struct {
	idx int
}

func (i indexSegment) segType() string { return "index" }

type wildcardSegment struct{}

func (w wildcardSegment) segType() string { return "wildcard" }

func parseJSONPathSegments(path string) ([]pathSegment, error) {
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, ".") {
		return nil, fmt.Errorf("path must start with '.'")
	}
	path = path[1:] // trim leading dot

	var segments []pathSegment
	for len(path) > 0 {
		// Handle [*] or [N]
		if path[0] == '[' {
			end := strings.Index(path, "]")
			if end == -1 {
				return nil, fmt.Errorf("unclosed bracket")
			}
			inner := path[1:end]
			path = path[end+1:]

			if inner == "*" {
				segments = append(segments, wildcardSegment{})
			} else {
				idx, err := strconv.Atoi(inner)
				if err != nil {
					return nil, fmt.Errorf("invalid index: %s", inner)
				}
				segments = append(segments, indexSegment{idx: idx})
			}
			// consume trailing dot
			if len(path) > 0 && path[0] == '.' {
				path = path[1:]
			}
			continue
		}

		// Handle field name (up to next . or [)
		end := len(path)
		for i, c := range path {
			if c == '.' || c == '[' {
				end = i
				break
			}
		}
		name := path[:end]
		if name == "*" {
			segments = append(segments, wildcardSegment{})
		} else if name != "" {
			segments = append(segments, fieldSegment{name: name})
		}
		path = path[end:]
		if len(path) > 0 && path[0] == '.' {
			path = path[1:]
		}
	}

	if len(segments) == 0 {
		return nil, fmt.Errorf("empty path expression")
	}
	return segments, nil
}

func walkPath(value any, segments []pathSegment) (any, error) {
	current := value

	for i, seg := range segments {
		remaining := segments[i+1:]

		switch s := seg.(type) {
		case fieldSegment:
			obj, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected object at .%s, got %T", s.name, current)
			}
			val, exists := obj[s.name]
			if !exists {
				return nil, fmt.Errorf("field '%s' not found", s.name)
			}
			current = val

		case indexSegment:
			arr, ok := current.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array at [%d], got %T", s.idx, current)
			}
			if s.idx < 0 || s.idx >= len(arr) {
				return nil, fmt.Errorf("index %d out of range (len %d)", s.idx, len(arr))
			}
			current = arr[s.idx]

		case wildcardSegment:
			arr, ok := current.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array at [*], got %T", current)
			}
			if len(remaining) == 0 {
				return arr, nil
			}
			// Apply remaining path to each element
			var results []any
			for _, elem := range arr {
				result, err := walkPath(elem, remaining)
				if err != nil {
					continue // skip elements that don't match
				}
				results = append(results, result)
			}
			return results, nil
		}
	}

	return current, nil
}

func formatResult(value any) (string, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to format result: %w", err)
	}
	return string(data), nil
}
