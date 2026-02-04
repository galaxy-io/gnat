package view

import (
	"context"
	"fmt"
	"time"

	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rivo/tview"
)

// ConsumerList displays all consumers for a given stream.
type ConsumerList struct {
	*components.MasterDetailView
	app        *App
	streamName string

	table   *components.Table
	preview *tview.TextView

	binding *binding.TableBinding[*jetstream.ConsumerInfo]
}

// NewConsumerList creates the consumer list view.
func NewConsumerList(app *App, streamName string) *ConsumerList {
	cl := &ConsumerList{
		app:        app,
		streamName: streamName,
	}

	cl.table = components.NewTable().
		SetHeaders("NAME", "TYPE", "PENDING", "ACK_PEND", "REDELIVERED", "WAITING")

	cl.preview = tview.NewTextView().
		SetDynamicColors(true)

	t := theme.Get()
	// Set up reactive table binding
	cl.binding = binding.NewTableBinding[*jetstream.ConsumerInfo](cl.table).
		SetMapper(func(c *jetstream.ConsumerInfo) []string {
			name := c.Config.Name
			if name == "" {
				name = c.Config.Durable
			}
			if name == "" {
				name = "(ephemeral)"
			}
			return []string{
				name,
				"Pull",
				formatNumber(c.NumPending),
				fmt.Sprintf("%d", c.NumAckPending),
				fmt.Sprintf("%d", c.NumRedelivered),
				fmt.Sprintf("%d", c.NumWaiting),
			}
		}).
		SetColorMapper(func(c *jetstream.ConsumerInfo) []tcell.Color {
			pendColor := t.Fg()
			if c.NumPending > 1000 {
				pendColor = t.Warning()
			}
			ackColor := t.Fg()
			if c.NumAckPending > 0 && c.Config.MaxAckPending > 0 {
				if float64(c.NumAckPending)/float64(c.Config.MaxAckPending) > 0.8 {
					ackColor = t.Error()
				}
			}
			redelivColor := t.Fg()
			if c.NumRedelivered > 0 {
				redelivColor = t.Warning()
			}
			return []tcell.Color{
				t.Fg(),
				t.FgDim(),
				pendColor,
				ackColor,
				redelivColor,
				t.FgDim(),
			}
		}).
		SetKeyMapper(func(c *jetstream.ConsumerInfo) string {
			name := c.Config.Name
			if name == "" {
				name = c.Config.Durable
			}
			return name
		}).
		SetFetcher(func() ([]*jetstream.ConsumerInfo, error) {
			provider := cl.app.Provider()
			if provider == nil {
				return nil, fmt.Errorf("no provider")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return provider.ListConsumers(ctx, cl.streamName)
		}).
		SetRefreshInterval(5 * time.Second).
		SetOnSelect(func(c *jetstream.ConsumerInfo) {
			name := c.Config.Name
			if name == "" {
				name = c.Config.Durable
			}
			cl.app.NavigateToConsumerDetail(cl.streamName, name)
		}).
		SetOnRefresh(func(data []*jetstream.ConsumerInfo, err error) {
			if err != nil {
				cl.app.QueueUpdateDraw(func() {
					cl.preview.SetText(fmt.Sprintf("[red]Error: %v[-]", err))
				})
			}
		})

	cl.table.SetSelectionChangedFunc(func(row, col int) {
		cl.updatePreview(row)
	})

	cl.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle(fmt.Sprintf("Consumers: %s", streamName)).
		SetDetailTitle("Preview").
		SetMasterContent(cl.table).
		SetDetailContent(cl.preview).
		SetRatio(0.6).
		ConfigureEmpty("󰋼", "No Consumers", "No consumers found")

	return cl
}

func (cl *ConsumerList) Name() string { return "Consumers" }

func (cl *ConsumerList) Start() {
	cl.binding.Start()
}

func (cl *ConsumerList) Stop() {
	cl.binding.Stop()
}

func (cl *ConsumerList) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "Detail"},
		{Key: "c", Description: "Create"},
		{Key: "d", Description: "Delete"},
		{Key: "r", Description: "Refresh"},
	}
}

func (cl *ConsumerList) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return cl.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch {
		case event.Rune() == 'r':
			cl.binding.RefreshAsync()
		default:
			if handler := cl.MasterDetailView.InputHandler(); handler != nil {
				handler(event, setFocus)
			}
		}
	})
}

func (cl *ConsumerList) updatePreview(row int) {
	c, ok := cl.binding.GetItemValue(row)
	if !ok || c == nil {
		cl.preview.SetText("")
		return
	}
	dim := theme.TagFgDim()

	name := c.Config.Name
	if name == "" {
		name = c.Config.Durable
	}

	ctype := "Pull"

	filter := c.Config.FilterSubject
	if filter == "" && len(c.Config.FilterSubjects) > 0 {
		filter = fmt.Sprintf("%v", c.Config.FilterSubjects)
	}

	text := fmt.Sprintf(
		"[%s]Name:[-]         %s\n"+
			"[%s]Type:[-]         %s\n"+
			"[%s]Ack Policy:[-]   %s\n"+
			"[%s]Filter:[-]       %s\n"+
			"[%s]Max Deliver:[-]  %d\n"+
			"[%s]Max Ack Pend:[-] %d\n"+
			"\n"+
			"[%s]Pending:[-]      %s\n"+
			"[%s]Ack Pending:[-]  %d\n"+
			"[%s]Redelivered:[-]  %d\n"+
			"[%s]Waiting:[-]      %d\n"+
			"[%s]Delivered:[-]    #%d\n"+
			"[%s]Ack Floor:[-]    #%d",
		dim, name,
		dim, ctype,
		dim, ackPolicyString(c.Config.AckPolicy),
		dim, filter,
		dim, c.Config.MaxDeliver,
		dim, c.Config.MaxAckPending,
		dim, formatNumber(c.NumPending),
		dim, c.NumAckPending,
		dim, c.NumRedelivered,
		dim, c.NumWaiting,
		dim, c.Delivered.Consumer,
		dim, c.AckFloor.Consumer,
	)

	cl.preview.SetText(text)
}

func ackPolicyString(p jetstream.AckPolicy) string {
	switch p {
	case jetstream.AckExplicitPolicy:
		return "Explicit"
	case jetstream.AckNonePolicy:
		return "None"
	case jetstream.AckAllPolicy:
		return "All"
	default:
		return "Unknown"
	}
}
