package view

import (
	"bytes"
	"encoding/json"
	"strings"
)

// marshalDisplayJSON serializes values for terminal display and copy/export
// without HTML escaping. An empty indent produces compact JSON.
func marshalDisplayJSON(value any, indent string) ([]byte, error) {
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", indent)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(output.Bytes(), []byte("\n")), nil
}

func formatJSONPretty(s string) string {
	s = strings.TrimSpace(s)
	var output bytes.Buffer
	if err := json.Indent(&output, []byte(s), "", "  "); err != nil {
		return s
	}
	return output.String()
}
