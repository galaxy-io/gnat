package view

import (
	"reflect"
	"testing"
)

func TestOrderKVKeys(t *testing.T) {
	keys := []string{"orders.updated", "accounts.created", "orders.created"}

	got := orderKVKeys(keys, true)
	want := []string{"accounts.created", "orders.created", "orders.updated"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orderKVKeys() = %v", got)
	}
	if keys[0] != "orders.updated" {
		t.Fatalf("orderKVKeys() modified its input: %v", keys)
	}

	got = orderKVKeys(keys, false)
	if !reflect.DeepEqual(got, keys) {
		t.Fatalf("orderKVKeys() = %v", got)
	}
}
