package view

import (
	"strings"

	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/gdamore/tcell/v2"
)

func escapeDynamicText(text string) string {
	return strings.ReplaceAll(text, "[", "[[")
}

func handleTextViewScroll(view *core.TextView, event *tcell.EventKey) bool {
	if !view.HasFocus() {
		return false
	}
	switch event.Rune() {
	case 'j':
		return view.HandleKey(tcell.NewEventKey(tcell.KeyDown, 0, event.Modifiers()))
	case 'k':
		return view.HandleKey(tcell.NewEventKey(tcell.KeyUp, 0, event.Modifiers()))
	}
	return false
}

type scrollableDiffViewer struct {
	*components.DiffViewer
}

func (viewer *scrollableDiffViewer) HandleKey(event *tcell.EventKey) bool {
	switch {
	case event.Key() == tcell.KeyDown || event.Rune() == 'j':
		viewer.MoveDown()
	case event.Key() == tcell.KeyUp || event.Rune() == 'k':
		viewer.MoveUp()
	case event.Key() == tcell.KeyPgDn:
		for range 10 {
			viewer.MoveDown()
		}
	case event.Key() == tcell.KeyPgUp:
		for range 10 {
			viewer.MoveUp()
		}
	default:
		return false
	}
	return true
}
