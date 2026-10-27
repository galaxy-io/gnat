package view

import (
	"strings"
	"testing"

	"github.com/atterpac/dado/binding"
	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/core"
	"github.com/galaxy-io/gnat/internal/nats"
	"github.com/gdamore/tcell/v2"
)

func TestJSONPreviewIndentation(t *testing.T) {
	const payload = `{"items":[{"name":"example"}]}`
	const want = "{\n  \"items\": [\n    {\n      \"name\": \"example\"\n    }\n  ]\n}"
	for _, pane := range []string{"kv", "stream browser", "stream monitor"} {
		t.Run(pane, func(t *testing.T) {
			preview := core.NewTextView().SetDynamicColors(true).SetScrollable(true)
			switch pane {
			case "kv":
				kd := &KVDetail{valueView: preview}
				kd.renderValue(displayKVEntry{value: payload})
			case "stream browser":
				mb := &MessageBrowser{preview: preview, messages: []*nats.RawMessage{{Data: []byte(payload)}}}
				mb.renderPreview(1)
			case "stream monitor":
				mm := &MessageMonitor{
					MasterDetailView: components.NewMasterDetailView(),
					preview:          preview,
					state:            binding.NewValue(monitorState{filtered: []nats.LiveMessage{{Data: []byte(payload)}}}),
				}
				mm.renderPreview(0)
			}
			screen := tcell.NewSimulationScreen("")
			if err := screen.Init(); err != nil {
				t.Fatal(err)
			}
			defer screen.Fini()
			screen.SetSize(80, 40)
			preview.SetRect(0, 0, 80, 40)
			preview.Draw(screen)
			var rendered strings.Builder
			for y := 0; y < 40; y++ {
				var line strings.Builder
				for x := 0; x < 80; x++ {
					r, _, _, _ := screen.GetContent(x, y)
					line.WriteRune(r)
				}
				rendered.WriteString(strings.TrimRight(line.String(), " "))
				rendered.WriteByte('\n')
			}
			if !strings.Contains(rendered.String(), want) {
				t.Fatalf("preview does not show indented JSON:\n%s", rendered.String())
			}
		})
	}
}
