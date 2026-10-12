package view

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMarshalDisplayJSON(t *testing.T) {
	value := map[string]string{"regex": "(?<name>a&b>)\\d+\n\"quoted\""}
	for _, indent := range []string{"", "  "} {
		t.Run("indent="+indent, func(t *testing.T) {
			data, err := marshalDisplayJSON(value, indent)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), "(?<name>a&b>)") {
				t.Fatalf("HTML characters were escaped: %s", data)
			}
			if strings.HasSuffix(string(data), "\n") || (indent == "" && strings.Contains(string(data), "\n")) {
				t.Fatalf("unexpected newline in serialized output: %q", data)
			}
			var decoded map[string]string
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded["regex"] != value["regex"] {
				t.Fatalf("JSON value changed: %q", decoded["regex"])
			}
		})
	}
	if _, err := marshalDisplayJSON(make(chan int), ""); err == nil {
		t.Fatal("expected error for unsupported value")
	}
}

func TestFormatJSONPrettyPreservesContent(t *testing.T) {
	input := `{"z":"(?<name>a&b>)","large":9007199254740993,"escaped":"\u003c","a":1e+09}`
	want := "{\n  \"z\": \"(?<name>a&b>)\",\n  \"large\": 9007199254740993,\n  \"escaped\": \"\\u003c\",\n  \"a\": 1e+09\n}"
	if got := formatJSONPretty(input); got != want {
		t.Fatalf("formatJSONPretty() = %s; want %s", got, want)
	}
	for _, input := range []string{"", "not JSON <>&", `{"invalid":`} {
		if got := formatJSONPretty(input); got != input {
			t.Fatalf("formatJSONPretty(%q) = %q", input, got)
		}
	}
}

func TestJSONQueryResultsPreserveHTMLCharacters(t *testing.T) {
	data := []byte(`{"regex":"(?<name>a&b>)"}`)
	want := `"(?<name>a&b>)"`
	got, err := evaluateJSONPath(data, ".regex")
	if err != nil || got != want {
		t.Fatalf("JSON path result = %q, %v; want %s", got, err, want)
	}
	pipeline, err := ParsePipeline(".regex")
	if err != nil {
		t.Fatal(err)
	}
	got, err = pipeline.Execute(data)
	if err != nil || got != want {
		t.Fatalf("pipeline result = %q, %v; want %s", got, err, want)
	}
}
