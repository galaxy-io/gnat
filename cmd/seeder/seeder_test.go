package main

import (
	"encoding/json"
	"testing"
)

func TestLongKVTestValue(t *testing.T) {
	value := longKVTestValue()
	if !json.Valid([]byte(value)) {
		t.Fatal("longKVTestValue returned invalid JSON")
	}
	if len(value) < 10_000 {
		t.Fatalf("longKVTestValue returned only %d bytes", len(value))
	}
}
