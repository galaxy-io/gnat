package view

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/atterpac/gnat/internal/nats"
	"github.com/atterpac/jig/binding"
	"github.com/atterpac/jig/components"
	"github.com/atterpac/jig/theme"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

type subjectNodeData struct {
	FullPath   string
	NavSubject string // subject for navigation (may include wildcards, empty = use FullPath)
	Count      uint64
	Streams    []string
}

type subjectExplorerState struct {
	root *components.TreeNode
	err  string
}

// SubjectExplorer shows a hierarchical tree of all subjects across streams.
type SubjectExplorer struct {
	*components.MasterDetailView
	app *App

	tree    *components.Tree
	preview *tview.TextView

	state       *binding.Value[subjectExplorerState]
	stopRefresh chan struct{}
	stopped     int32

	// Recent messages preview
	previewCancel  context.CancelFunc
	previewData    *subjectNodeData
}

func NewSubjectExplorer(app *App) *SubjectExplorer {
	se := &SubjectExplorer{
		app:         app,
		stopRefresh: make(chan struct{}, 1),
	}

	se.tree = components.NewTree().
		SetShowLines(true).
		SetShowIcons(true).
		SetIndentSize(2)

	se.tree.SetOnHighlight(func(node *components.TreeNode) {
		se.renderPreview(node)
	})

	se.tree.SetOnSelect(func(node *components.TreeNode) {
		if data, ok := node.Data.(*subjectNodeData); ok {
			subject := data.FullPath
			if data.NavSubject != "" {
				subject = data.NavSubject
			} else if !node.IsLeaf() {
				subject = data.FullPath + ".>"
			}
			app.NavigateToMessageMonitorWithSubject(subject)
		}
	})

	se.preview = tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWrap(true)
	se.preview.SetBackgroundColor(theme.Bg())
	theme.Register(se.preview)

	se.MasterDetailView = components.NewMasterDetailView().
		SetMasterTitle("Subjects").
		SetDetailTitle("Details").
		SetMasterContent(se.tree).
		SetDetailContent(se.preview).
		SetRatio(0.5)

	se.state = binding.NewValue(subjectExplorerState{})
	se.state.BindToWithDraw(func(s subjectExplorerState) {
		se.renderState(s)
	})

	return se
}

func (se *SubjectExplorer) Name() string { return "Subject Explorer" }

func (se *SubjectExplorer) Start() {
	atomic.StoreInt32(&se.stopped, 0)
	se.stopRefresh = make(chan struct{}, 1)
	go func() {
		se.refresh()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-se.stopRefresh:
				return
			case <-ticker.C:
				se.refresh()
			}
		}
	}()
}

func (se *SubjectExplorer) Stop() {
	atomic.StoreInt32(&se.stopped, 1)
	select {
	case se.stopRefresh <- struct{}{}:
	default:
	}
}

func (se *SubjectExplorer) Hints() []components.KeyHint {
	return []components.KeyHint{
		{Key: "Enter", Description: "Monitor subject"},
		{Key: "v", Description: "Stream detail"},
		{Key: "o/O", Description: "Expand / Expand all"},
		{Key: "C", Description: "Collapse all"},
		{Key: "/", Description: "Filter"},
		{Key: "r", Description: "Refresh"},
		{Key: "p", Description: "Toggle preview"},
		{Key: "Esc", Description: "Back"},
	}
}

func (se *SubjectExplorer) InputHandler() func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
	return se.WrapInputHandler(func(event *tcell.EventKey, setFocus func(p tview.Primitive)) {
		switch event.Rune() {
		case 'v':
			if node := se.tree.GetSelected(); node != nil {
				if data, ok := node.Data.(*subjectNodeData); ok && len(data.Streams) > 0 {
					se.app.NavigateToStreamDetail(data.Streams[0])
				}
			}
			return
		case 'r':
			go se.refresh()
			return
		case 'p':
			se.ToggleDetail()
			return
		case '/':
			se.showFilter()
			return
		}

		// Delegate to tree for j/k/h/l/o/O/C/Enter etc.
		if handler := se.tree.InputHandler(); handler != nil {
			handler(event, setFocus)
		}
	})
}

func (se *SubjectExplorer) showFilter() {
	se.app.statusBar.SetCommandPrompt("Filter: ")
	se.app.statusBar.SetCommandPlaceholder("subject pattern...")
	se.app.statusBar.EnterCommandMode()
	se.app.app.SetFocus(se.app.statusBar.GetCommandInput())

	se.app.statusBar.SetOnCommandSubmit(func(text string) {
		se.app.statusBar.ExitCommandMode()
		se.tree.Filter(text)
		se.app.app.SetFocus(se.MasterDetailView)
	})
	se.app.statusBar.SetOnCommandCancel(func() {
		se.app.statusBar.ExitCommandMode()
		se.tree.Filter("")
		se.app.app.SetFocus(se.MasterDetailView)
	})
}

type subjectInfo struct {
	count      uint64
	streams    []string
	navSubject string // subject to use for navigation (may include wildcards)
}

// stripWildcard removes trailing NATS wildcards (> or *) from a subject pattern.
// "orders.>" becomes "orders", "events.*" becomes "events", ">" becomes "".
func stripWildcard(pattern string) string {
	pattern = strings.TrimSuffix(pattern, ".>")
	pattern = strings.TrimSuffix(pattern, ".*")
	if pattern == ">" || pattern == "*" {
		return ""
	}
	return pattern
}

func (se *SubjectExplorer) refresh() {
	if atomic.LoadInt32(&se.stopped) == 1 {
		return
	}

	provider := se.app.Provider()
	if provider == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	streams, err := provider.ListStreams(ctx)
	if err != nil {
		se.state.SetAndDraw(subjectExplorerState{err: err.Error()})
		return
	}

	allSubjects := make(map[string]*subjectInfo)

	for _, stream := range streams {
		if atomic.LoadInt32(&se.stopped) == 1 {
			return
		}
		subjects, err := provider.StreamSubjects(ctx, stream.Config.Name)
		if err == nil && len(subjects) > 0 {
			for subj, count := range subjects {
				if info, exists := allSubjects[subj]; exists {
					info.count += count
					info.streams = append(info.streams, stream.Config.Name)
				} else {
					allSubjects[subj] = &subjectInfo{count: count, streams: []string{stream.Config.Name}}
				}
			}
			continue
		}
		// Fallback: use config subject patterns with wildcards stripped for display
		for _, pattern := range stream.Config.Subjects {
			displaySubj := stripWildcard(pattern)
			if displaySubj == "" {
				continue
			}
			if info, exists := allSubjects[displaySubj]; exists {
				info.count += stream.State.Msgs
				info.streams = append(info.streams, stream.Config.Name)
			} else {
				allSubjects[displaySubj] = &subjectInfo{
					count:      stream.State.Msgs,
					streams:    []string{stream.Config.Name},
					navSubject: pattern,
				}
			}
		}
	}

	root := buildSubjectTree(allSubjects)
	se.state.SetAndDraw(subjectExplorerState{root: root})
}

// trieNode is used internally for building the subject tree.
type trieNode struct {
	children   map[string]*trieNode
	count      uint64
	streams    map[string]bool
	navSubject string // original subject for navigation (may include wildcards)
}

func buildSubjectTree(subjects map[string]*subjectInfo) *components.TreeNode {
	// Build trie
	root := &trieNode{children: make(map[string]*trieNode)}

	for subj, info := range subjects {
		parts := strings.Split(subj, ".")
		current := root
		for _, part := range parts {
			if current.children[part] == nil {
				current.children[part] = &trieNode{
					children: make(map[string]*trieNode),
					streams:  make(map[string]bool),
				}
			}
			current = current.children[part]
		}
		current.count += info.count
		if info.navSubject != "" {
			current.navSubject = info.navSubject
		}
		for _, s := range info.streams {
			current.streams[s] = true
		}
	}

	// Aggregate counts bottom-up
	aggregateCounts(root)

	// Convert trie to TreeNode
	treeRoot := &components.TreeNode{
		ID:       ">",
		Label:    fmt.Sprintf("> (%s)", formatNumber(root.count)),
		Icon:     theme.IconFolder,
		Expanded: true,
		Data: &subjectNodeData{
			FullPath: ">",
			Count:    root.count,
			Streams:  collectStreams(root),
		},
	}

	addTrieChildren(treeRoot, root, "")
	return treeRoot
}

func aggregateCounts(node *trieNode) uint64 {
	total := node.count
	for _, child := range node.children {
		total += aggregateCounts(child)
		for s := range child.streams {
			if node.streams == nil {
				node.streams = make(map[string]bool)
			}
			node.streams[s] = true
		}
	}
	node.count = total
	return total
}

func collectStreams(node *trieNode) []string {
	var streams []string
	for s := range node.streams {
		streams = append(streams, s)
	}
	sort.Strings(streams)
	return streams
}

func addTrieChildren(parent *components.TreeNode, trieParent *trieNode, prefix string) {
	// Sort children alphabetically
	names := make([]string, 0, len(trieParent.children))
	for name := range trieParent.children {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		child := trieParent.children[name]
		fullPath := name
		if prefix != "" {
			fullPath = prefix + "." + name
		}

		icon := theme.IconFile
		if len(child.children) > 0 {
			icon = theme.IconFolder
		}

		label := fmt.Sprintf("%s (%s)", name, formatNumber(child.count))
		treeNode := &components.TreeNode{
			ID:    fullPath,
			Label: label,
			Icon:  icon,
			Data: &subjectNodeData{
				FullPath:   fullPath,
				NavSubject: child.navSubject,
				Count:      child.count,
				Streams:    collectStreams(child),
			},
		}

		parent.AddChild(treeNode)

		if len(child.children) > 0 {
			addTrieChildren(treeNode, child, fullPath)
		}
	}
}

func (se *SubjectExplorer) renderState(s subjectExplorerState) {
	if s.err != "" {
		se.preview.SetText(fmt.Sprintf("[red]Error: %s[-]", s.err))
		return
	}

	if s.root == nil {
		return
	}

	se.tree.SetRoot(s.root)
	se.tree.ExpandTo(2)
	se.SetMasterTitle("Subjects")
}

func (se *SubjectExplorer) renderPreview(node *components.TreeNode) {
	if node == nil {
		se.preview.SetText("")
		return
	}

	data, ok := node.Data.(*subjectNodeData)
	if !ok {
		se.preview.SetText(node.Label)
		return
	}

	// Cancel any in-flight preview fetch
	if se.previewCancel != nil {
		se.previewCancel()
	}
	se.previewData = data

	// Render metadata immediately
	se.renderPreviewText(data, nil)

	// Only fetch messages for leaf subjects with at least one stream
	if !node.IsLeaf() || len(data.Streams) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	se.previewCancel = cancel

	subject := data.FullPath
	streams := make([]string, len(data.Streams))
	copy(streams, data.Streams)

	go func() {
		defer cancel()
		provider := se.app.Provider()
		if provider == nil {
			return
		}
		var msgs []*nats.RawMessage
		for _, stream := range streams {
			if ctx.Err() != nil {
				return
			}
			fetched, err := provider.GetRecentMessagesForSubject(ctx, stream, subject, 5)
			if err != nil {
				continue
			}
			msgs = append(msgs, fetched...)
		}
		se.app.QueueUpdateDraw(func() {
			if se.previewData == data {
				se.renderPreviewText(data, msgs)
			}
		})
	}()
}

func (se *SubjectExplorer) renderPreviewText(data *subjectNodeData, msgs []*nats.RawMessage) {
	dim := theme.TagFgDim()
	accent := theme.TagAccent()

	var b strings.Builder
	fmt.Fprintf(&b, "[%s]Subject:[-]  [%s]%s[-]\n", dim, accent, data.FullPath)
	fmt.Fprintf(&b, "[%s]Messages:[-] %s\n", dim, formatNumber(data.Count))

	if len(data.Streams) > 0 {
		fmt.Fprintf(&b, "\n[%s]Streams:[-]\n", dim)
		for _, s := range data.Streams {
			fmt.Fprintf(&b, "  [%s]%s[-]\n", accent, s)
		}
	}

	if len(msgs) > 0 {
		fmt.Fprintf(&b, "\n[%s]Recent Messages:[-]\n", dim)
		for _, msg := range msgs {
			ts := msg.Time.Format("15:04:05")
			fmt.Fprintf(&b, "\n  [%s]%s[-] [%s]seq=%d  %s  %s[-]\n", accent, msg.Subject, dim, msg.Sequence, ts, formatBytes(uint64(len(msg.Data))))
			payload := string(msg.Data)
			if json.Valid(msg.Data) {
				var pretty bytes.Buffer
				if err := json.Indent(&pretty, msg.Data, "    ", "  "); err == nil {
					payload = pretty.String()
				}
			}
			payload = strings.ReplaceAll(payload, "[", "[[")
			if len(payload) > 500 {
				payload = payload[:500] + "..."
			}
			fmt.Fprintf(&b, "    %s\n", payload)
		}
	}

	se.preview.SetText(b.String())
	se.preview.ScrollToBeginning()
}
