package view

import (
	"fmt"

	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/validators"
)

// ConfirmDelete shows a type-to-confirm deletion modal.
// resourceType: "stream", "consumer", "KV key", etc.
// resourceName: the name the user must type to confirm.
// onConfirm: called after successful name match — run the actual deletion in here.
func ConfirmDelete(app *App, resourceType, resourceName string, onConfirm func()) {
	modal := components.NewFormBuilder().
		Text("confirm", fmt.Sprintf("Type \"%s\" to confirm", resourceName)).
		Placeholder(resourceName).
		Validate(validators.Custom(func(v any) error {
			if s, ok := v.(string); ok && s != resourceName {
				return fmt.Errorf("name does not match")
			}
			return nil
		})).
		Done().
		OnSubmit(func(values map[string]any) {
			if getString(values, "confirm") == resourceName {
				onConfirm()
			}
		}).
		AsFormModal(
			fmt.Sprintf("Delete %s: %s", resourceType, resourceName),
			50, 10,
		)

	modal.SetHints([]components.KeyHint{
		{Key: "Enter", Description: "Confirm"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

// Confirm shows a simple yes/no confirmation modal (Enter = confirm, Esc =
// cancel). Use for bulk or low-risk destructive actions where type-to-confirm
// is unnecessary friction.
func Confirm(app *App, title, message string, onConfirm func()) {
	modal := components.NewConfirmModal(title, message)
	modal.SetOnSubmit(func() {
		app.app.Pages().DismissModal()
		onConfirm()
	})
	app.app.Pages().Push(modal)
}
