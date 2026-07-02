package view

import (
	"context"

	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/atterpac/dado/nav"
	"github.com/atterpac/dado/theme"
	"github.com/gdamore/tcell/v2"
)

// CommandOutputView is a nav.Component wrapper for command output display.
// It can hold a LogViewer (streaming) or CodeView (static JSON) as its inner view.
type CommandOutputView struct {
	*core.Flex
	app         *App
	cmdName     string
	description string
	inner       core.Widget
	cancelFn    context.CancelFunc
}

// NewCommandOutputView creates a new command output view.
func NewCommandOutputView(app *App, cmdName, description string, inner core.Widget, cancelFn context.CancelFunc) *CommandOutputView {
	panel := components.NewPanel().SetTitle(description)
	panel.SetContent(inner)

	flex := core.NewFlex().SetDirection(core.Column)
	flex.SetBackgroundColor(theme.Bg())
	flex.AddItem(panel, 0, 1, true)

	return &CommandOutputView{
		Flex:        flex,
		app:         app,
		cmdName:     cmdName,
		description: description,
		inner:       inner,
		cancelFn:    cancelFn,
	}
}

func (c *CommandOutputView) Name() string {
	return "cmd: " + c.description
}

func (c *CommandOutputView) Start() {
	if comp, ok := c.inner.(nav.Component); ok {
		comp.Start()
	}
}

func (c *CommandOutputView) Stop() {
	if c.cancelFn != nil {
		c.cancelFn()
	}
	if comp, ok := c.inner.(nav.Component); ok {
		comp.Stop()
	}
}

func (c *CommandOutputView) Hints() []components.KeyHint {
	hints := []components.KeyHint{
		{Key: "Esc", Description: "Back"},
	}
	if comp, ok := c.inner.(nav.Component); ok {
		hints = append(hints, comp.Hints()...)
	}
	return hints
}

func (c *CommandOutputView) Focus() {
	c.Flex.Focus()
	c.inner.Focus()
}

func (c *CommandOutputView) Draw(screen tcell.Screen) {
	c.SetBackgroundColor(theme.Bg())
	c.Flex.Draw(screen)
}
