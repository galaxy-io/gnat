package view

import (
	"testing"

	"github.com/atterpac/dado/core"
	"github.com/gdamore/tcell/v2"
)

func TestEscapeDynamicText(t *testing.T) {
	got := escapeDynamicText(`{"items":[]}`)
	if got != `{"items":[[]}` {
		t.Fatalf("escapeDynamicText() = %q", got)
	}
}

func TestHandleTextViewScroll(t *testing.T) {
	view := core.NewTextView().
		SetScrollable(true).
		SetText("one\ntwo\nthree")
	view.SetRect(0, 0, 10, 1)
	view.Focus()

	if !handleTextViewScroll(view, tcell.NewEventKey(tcell.KeyRune, 'j', tcell.ModNone)) {
		t.Fatal("j was not handled")
	}
	row, _ := view.GetScrollOffset()
	if row != 1 {
		t.Fatalf("scroll row = %d", row)
	}

	if !handleTextViewScroll(view, tcell.NewEventKey(tcell.KeyRune, 'k', tcell.ModNone)) {
		t.Fatal("k was not handled")
	}
	row, _ = view.GetScrollOffset()
	if row != 0 {
		t.Fatalf("scroll row = %d", row)
	}
}
