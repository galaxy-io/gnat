package view

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/atterpac/dado/core"
	"github.com/nats-io/nats.go/jetstream"
)

type displayKVEntry struct {
	jetstream.KeyValueEntry
	value string
}

func (e displayKVEntry) Key() string                     { return "test" }
func (e displayKVEntry) Value() []byte                   { return []byte(e.value) }
func (e displayKVEntry) Revision() uint64                { return 1 }
func (e displayKVEntry) Created() time.Time              { return time.Time{} }
func (e displayKVEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }

func TestRenderKVValuePreservesContent(t *testing.T) {
	for _, input := range []string{
		`{"regex":"(?<name>a&b>)","large":9007199254740993}`,
		"  plain <>& text  ",
	} {
		kd := &KVDetail{valueView: core.NewTextView()}
		kd.renderValue(displayKVEntry{value: input})
		got := kd.valueView.GetText()
		if strings.HasPrefix(input, "{") {
			if !strings.Contains(got, `"regex": "(?<name>a&b>)"`) || !strings.Contains(got, "9007199254740993") {
				t.Fatalf("JSON content changed: %s", got)
			}
		} else if !strings.HasSuffix(got, input) {
			t.Fatalf("plain text changed: %q", got)
		}
	}
}

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
