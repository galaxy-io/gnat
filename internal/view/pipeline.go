package view

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Pipeline represents a chainable sequence of data transformation stages.
type Pipeline struct {
	stages []PipelineStage
	raw    string
}

// PipelineStage transforms an input value into an output value.
type PipelineStage interface {
	Process(input any) (any, error)
	String() string
}

// ParsePipeline parses a pipeline expression like ".field | select(.x == 1) | map(.name) | sort".
func ParsePipeline(expr string) (*Pipeline, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty pipeline expression")
	}

	parts := splitPipeline(expr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty pipeline expression")
	}

	var stages []PipelineStage
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		stage, err := parseStage(part)
		if err != nil {
			return nil, fmt.Errorf("parsing %q: %w", part, err)
		}
		stages = append(stages, stage)
	}

	if len(stages) == 0 {
		return nil, fmt.Errorf("no valid stages in pipeline")
	}

	return &Pipeline{stages: stages, raw: expr}, nil
}

// Execute runs the pipeline against raw JSON data and returns formatted output.
func (p *Pipeline) Execute(data []byte) (string, error) {
	var root any
	if err := json.Unmarshal(data, &root); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	current := root
	for _, stage := range p.stages {
		result, err := stage.Process(current)
		if err != nil {
			return "", fmt.Errorf("stage %s: %w", stage.String(), err)
		}
		current = result
	}

	out, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return "", fmt.Errorf("formatting result: %w", err)
	}
	return string(out), nil
}

// String returns the original pipeline expression.
func (p *Pipeline) String() string {
	return p.raw
}

// splitPipeline splits on top-level | characters, respecting parentheses.
func splitPipeline(expr string) []string {
	var parts []string
	depth := 0
	start := 0

	for i, c := range expr {
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case '|':
			if depth == 0 {
				parts = append(parts, expr[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, expr[start:])
	return parts
}

// parseStage parses a single pipeline stage expression.
func parseStage(expr string) (PipelineStage, error) {
	// Check for known function-style stages
	if strings.HasPrefix(expr, "select(") && strings.HasSuffix(expr, ")") {
		return parseFilterStage(expr)
	}
	if strings.HasPrefix(expr, "map(") && strings.HasSuffix(expr, ")") {
		return parseMapStage(expr)
	}
	if strings.HasPrefix(expr, "sort_by(") && strings.HasSuffix(expr, ")") {
		return parseSortByStage(expr)
	}
	if strings.HasPrefix(expr, "group_by(") && strings.HasSuffix(expr, ")") {
		return parseGroupByStage(expr)
	}
	if strings.HasPrefix(expr, "first(") && strings.HasSuffix(expr, ")") {
		return parseFirstStage(expr)
	}
	if strings.HasPrefix(expr, "last(") && strings.HasSuffix(expr, ")") {
		return parseLastStage(expr)
	}

	// Keyword stages
	switch expr {
	case "sort":
		return &sortStage{}, nil
	case "unique":
		return &uniqueStage{}, nil
	case "length":
		return &lengthStage{}, nil
	case "flatten":
		return &flattenStage{}, nil
	case "keys":
		return &keysStage{}, nil
	case "values":
		return &valuesStage{}, nil
	case "first":
		return &firstStage{n: 1}, nil
	case "last":
		return &lastStage{n: 1}, nil
	case "reverse":
		return &reverseStage{}, nil
	}

	// JSON path select (starts with .)
	if strings.HasPrefix(expr, ".") {
		return &selectStage{path: expr}, nil
	}

	return nil, fmt.Errorf("unknown stage: %s", expr)
}

// ── Select Stage ───────────────────────────────────────────────────────────

type selectStage struct {
	path string
}

func (s *selectStage) Process(input any) (any, error) {
	// Marshal back to JSON so we can reuse evaluateJSONPath logic,
	// but actually let's walk directly to avoid the round-trip.
	segments, err := parseJSONPathSegments(s.path)
	if err != nil {
		return nil, err
	}
	return walkPath(input, segments)
}

func (s *selectStage) String() string { return s.path }

// ── Filter Stage ───────────────────────────────────────────────────────────

type filterStage struct {
	raw        string
	conditions []filterCondition
	logic      string // "and" or "or" — determined by connectors
}

type filterCondition struct {
	path string
	op   string // ==, !=, >, <, >=, <=, contains
	val  string
}

func parseFilterStage(expr string) (*filterStage, error) {
	// select(.field == "value") or select(.field > 100)
	inner := expr[7 : len(expr)-1] // strip "select(" and ")"

	// Split on " and " or " or " (only top-level)
	var conditions []filterCondition
	logic := "and"

	// Simple split — handle " and " / " or "
	var parts []string
	if strings.Contains(inner, " and ") {
		parts = strings.Split(inner, " and ")
		logic = "and"
	} else if strings.Contains(inner, " or ") {
		parts = strings.Split(inner, " or ")
		logic = "or"
	} else {
		parts = []string{inner}
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		cond, err := parseCondition(part)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}

	return &filterStage{raw: expr, conditions: conditions, logic: logic}, nil
}

func parseCondition(expr string) (filterCondition, error) {
	// Try operators in order of length (longest first to avoid partial matches)
	operators := []string{"!=", ">=", "<=", "==", ">", "<", "contains"}
	for _, op := range operators {
		if idx := strings.Index(expr, " "+op+" "); idx >= 0 {
			path := strings.TrimSpace(expr[:idx])
			val := strings.TrimSpace(expr[idx+len(op)+2:])
			// Strip quotes from string values
			val = strings.Trim(val, "\"'")
			return filterCondition{path: path, op: op, val: val}, nil
		}
	}
	return filterCondition{}, fmt.Errorf("no operator found in condition: %s", expr)
}

func (f *filterStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		// Single object — test against it
		if f.matchesAll(input) {
			return input, nil
		}
		return nil, fmt.Errorf("filter did not match")
	}

	var result []any
	for _, elem := range arr {
		if f.matchesAll(elem) {
			result = append(result, elem)
		}
	}
	return result, nil
}

func (f *filterStage) matchesAll(elem any) bool {
	if f.logic == "or" {
		for _, cond := range f.conditions {
			if matchCondition(elem, cond) {
				return true
			}
		}
		return false
	}
	// "and"
	for _, cond := range f.conditions {
		if !matchCondition(elem, cond) {
			return false
		}
	}
	return true
}

func matchCondition(elem any, cond filterCondition) bool {
	// Extract field value using path
	segments, err := parseJSONPathSegments(cond.path)
	if err != nil {
		return false
	}
	fieldVal, err := walkPath(elem, segments)
	if err != nil {
		return false
	}

	// Compare
	return compareValues(fieldVal, cond.op, cond.val)
}

func compareValues(fieldVal any, op, target string) bool {
	switch v := fieldVal.(type) {
	case string:
		switch op {
		case "==":
			return v == target
		case "!=":
			return v != target
		case ">":
			return v > target
		case "<":
			return v < target
		case ">=":
			return v >= target
		case "<=":
			return v <= target
		case "contains":
			return strings.Contains(v, target)
		}
	case float64:
		targetNum, err := strconv.ParseFloat(target, 64)
		if err != nil {
			return false
		}
		switch op {
		case "==":
			return v == targetNum
		case "!=":
			return v != targetNum
		case ">":
			return v > targetNum
		case "<":
			return v < targetNum
		case ">=":
			return v >= targetNum
		case "<=":
			return v <= targetNum
		}
	case bool:
		targetBool := target == "true"
		switch op {
		case "==":
			return v == targetBool
		case "!=":
			return v != targetBool
		}
	case nil:
		switch op {
		case "==":
			return target == "null" || target == ""
		case "!=":
			return target != "null" && target != ""
		}
	}
	return false
}

func (f *filterStage) String() string { return f.raw }

// ── Map Stage ──────────────────────────────────────────────────────────────

type mapStage struct {
	path string
}

func parseMapStage(expr string) (*mapStage, error) {
	inner := expr[4 : len(expr)-1] // strip "map(" and ")"
	inner = strings.TrimSpace(inner)
	if !strings.HasPrefix(inner, ".") {
		return nil, fmt.Errorf("map argument must be a path starting with '.'")
	}
	return &mapStage{path: inner}, nil
}

func (m *mapStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("map requires array input, got %T", input)
	}

	segments, err := parseJSONPathSegments(m.path)
	if err != nil {
		return nil, err
	}

	var result []any
	for _, elem := range arr {
		val, err := walkPath(elem, segments)
		if err != nil {
			continue // skip elements without the field
		}
		result = append(result, val)
	}
	return result, nil
}

func (m *mapStage) String() string { return fmt.Sprintf("map(%s)", m.path) }

// ── Sort Stage ─────────────────────────────────────────────────────────────

type sortStage struct{}

func (s *sortStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("sort requires array input, got %T", input)
	}

	sorted := make([]any, len(arr))
	copy(sorted, arr)
	sort.SliceStable(sorted, func(i, j int) bool {
		return anyLess(sorted[i], sorted[j])
	})
	return sorted, nil
}

func (s *sortStage) String() string { return "sort" }

// ── Sort By Stage ──────────────────────────────────────────────────────────

type sortByStage struct {
	path string
}

func parseSortByStage(expr string) (*sortByStage, error) {
	inner := expr[8 : len(expr)-1] // strip "sort_by(" and ")"
	inner = strings.TrimSpace(inner)
	if !strings.HasPrefix(inner, ".") {
		return nil, fmt.Errorf("sort_by argument must be a path starting with '.'")
	}
	return &sortByStage{path: inner}, nil
}

func (s *sortByStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("sort_by requires array input, got %T", input)
	}

	segments, err := parseJSONPathSegments(s.path)
	if err != nil {
		return nil, err
	}

	sorted := make([]any, len(arr))
	copy(sorted, arr)
	sort.SliceStable(sorted, func(i, j int) bool {
		vi, _ := walkPath(sorted[i], segments)
		vj, _ := walkPath(sorted[j], segments)
		return anyLess(vi, vj)
	})
	return sorted, nil
}

func (s *sortByStage) String() string { return fmt.Sprintf("sort_by(%s)", s.path) }

// ── Unique Stage ───────────────────────────────────────────────────────────

type uniqueStage struct{}

func (u *uniqueStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("unique requires array input, got %T", input)
	}

	seen := make(map[string]bool)
	var result []any
	for _, elem := range arr {
		key, _ := json.Marshal(elem)
		k := string(key)
		if !seen[k] {
			seen[k] = true
			result = append(result, elem)
		}
	}
	return result, nil
}

func (u *uniqueStage) String() string { return "unique" }

// ── Length Stage ────────────────────────────────────────────────────────────

type lengthStage struct{}

func (l *lengthStage) Process(input any) (any, error) {
	switch v := input.(type) {
	case []any:
		return float64(len(v)), nil
	case map[string]any:
		return float64(len(v)), nil
	case string:
		return float64(len(v)), nil
	default:
		return nil, fmt.Errorf("length not supported for %T", input)
	}
}

func (l *lengthStage) String() string { return "length" }

// ── First / Last Stages ───────────────────────────────────────────────────

type firstStage struct {
	n int
}

func parseFirstStage(expr string) (*firstStage, error) {
	inner := expr[6 : len(expr)-1] // strip "first(" and ")"
	n, err := strconv.Atoi(strings.TrimSpace(inner))
	if err != nil {
		return nil, fmt.Errorf("first argument must be a number: %w", err)
	}
	return &firstStage{n: n}, nil
}

func (f *firstStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("first requires array input, got %T", input)
	}
	if f.n >= len(arr) {
		return arr, nil
	}
	return arr[:f.n], nil
}

func (f *firstStage) String() string { return fmt.Sprintf("first(%d)", f.n) }

type lastStage struct {
	n int
}

func parseLastStage(expr string) (*lastStage, error) {
	inner := expr[5 : len(expr)-1] // strip "last(" and ")"
	n, err := strconv.Atoi(strings.TrimSpace(inner))
	if err != nil {
		return nil, fmt.Errorf("last argument must be a number: %w", err)
	}
	return &lastStage{n: n}, nil
}

func (l *lastStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("last requires array input, got %T", input)
	}
	if l.n >= len(arr) {
		return arr, nil
	}
	return arr[len(arr)-l.n:], nil
}

func (l *lastStage) String() string { return fmt.Sprintf("last(%d)", l.n) }

// ── Flatten Stage ──────────────────────────────────────────────────────────

type flattenStage struct{}

func (f *flattenStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("flatten requires array input, got %T", input)
	}

	var result []any
	for _, elem := range arr {
		if inner, ok := elem.([]any); ok {
			result = append(result, inner...)
		} else {
			result = append(result, elem)
		}
	}
	return result, nil
}

func (f *flattenStage) String() string { return "flatten" }

// ── Keys Stage ─────────────────────────────────────────────────────────────

type keysStage struct{}

func (k *keysStage) Process(input any) (any, error) {
	obj, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("keys requires object input, got %T", input)
	}

	keys := make([]any, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].(string) < keys[j].(string)
	})
	return keys, nil
}

func (k *keysStage) String() string { return "keys" }

// ── Values Stage ───────────────────────────────────────────────────────────

type valuesStage struct{}

func (v *valuesStage) Process(input any) (any, error) {
	obj, ok := input.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("values requires object input, got %T", input)
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	vals := make([]any, 0, len(obj))
	for _, k := range keys {
		vals = append(vals, obj[k])
	}
	return vals, nil
}

func (v *valuesStage) String() string { return "values" }

// ── Group By Stage ─────────────────────────────────────────────────────────

type groupByStage struct {
	path string
}

func parseGroupByStage(expr string) (*groupByStage, error) {
	inner := expr[9 : len(expr)-1] // strip "group_by(" and ")"
	inner = strings.TrimSpace(inner)
	if !strings.HasPrefix(inner, ".") {
		return nil, fmt.Errorf("group_by argument must be a path starting with '.'")
	}
	return &groupByStage{path: inner}, nil
}

func (g *groupByStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("group_by requires array input, got %T", input)
	}

	segments, err := parseJSONPathSegments(g.path)
	if err != nil {
		return nil, err
	}

	groups := make(map[string][]any)
	var order []string
	for _, elem := range arr {
		val, err := walkPath(elem, segments)
		if err != nil {
			continue
		}
		key := fmt.Sprintf("%v", val)
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], elem)
	}

	// Return as ordered array of arrays
	var result []any
	for _, key := range order {
		result = append(result, groups[key])
	}
	return result, nil
}

func (g *groupByStage) String() string { return fmt.Sprintf("group_by(%s)", g.path) }

// ── Reverse Stage ──────────────────────────────────────────────────────────

type reverseStage struct{}

func (r *reverseStage) Process(input any) (any, error) {
	arr, ok := input.([]any)
	if !ok {
		return nil, fmt.Errorf("reverse requires array input, got %T", input)
	}

	result := make([]any, len(arr))
	for i, v := range arr {
		result[len(arr)-1-i] = v
	}
	return result, nil
}

func (r *reverseStage) String() string { return "reverse" }

// ── Helpers ────────────────────────────────────────────────────────────────

func anyLess(a, b any) bool {
	switch va := a.(type) {
	case float64:
		if vb, ok := b.(float64); ok {
			return va < vb
		}
	case string:
		if vb, ok := b.(string); ok {
			return va < vb
		}
	case bool:
		if vb, ok := b.(bool); ok {
			return !va && vb // false < true
		}
	case nil:
		return b != nil
	}
	// Fallback: compare JSON representations
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) < string(jb)
}

// IsPipelineExpr returns true if the expression looks like a pipeline
// (contains | outside of quoted strings or has pipeline-specific keywords).
func IsPipelineExpr(expr string) bool {
	if strings.Contains(expr, "|") {
		return true
	}
	trimmed := strings.TrimSpace(expr)
	pipelineKeywords := []string{
		"select(", "map(", "sort", "unique", "length",
		"first", "last", "flatten", "keys", "values",
		"group_by(", "sort_by(", "reverse",
	}
	for _, kw := range pipelineKeywords {
		if strings.Contains(trimmed, kw) {
			return true
		}
	}
	return false
}

// evaluateJSONPathOrPipeline tries pipeline first if it looks like one,
// otherwise falls back to simple JSON path evaluation.
func evaluateJSONPathOrPipeline(data []byte, expr string) (string, error) {
	if IsPipelineExpr(expr) {
		p, err := ParsePipeline(expr)
		if err != nil {
			return "", err
		}
		return p.Execute(data)
	}
	return evaluateJSONPath(data, expr)
}
