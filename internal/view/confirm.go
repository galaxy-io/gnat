package view

import (
	"fmt"

	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/validators"
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
