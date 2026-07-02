package view

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/atterpac/dado/components"
	"github.com/atterpac/dado/validators"
	"github.com/nats-io/nats.go/jetstream"
)

// ── Stream Edit Form ──────────────────────────────────────────────────────

func showStreamEditForm(app *App, info *jetstream.StreamInfo, onUpdated func()) {
	cfg := info.Config

	// Determine current selection indices for dropdowns
	storageIdx := 0
	if cfg.Storage == jetstream.MemoryStorage {
		storageIdx = 1
	}
	retentionIdx := 0
	switch cfg.Retention {
	case jetstream.InterestPolicy:
		retentionIdx = 1
	case jetstream.WorkQueuePolicy:
		retentionIdx = 2
	}
	discardIdx := 0
	if cfg.Discard == jetstream.DiscardNew {
		discardIdx = 1
	}
	replicaIdx := 0
	switch cfg.Replicas {
	case 3:
		replicaIdx = 1
	case 5:
		replicaIdx = 2
	}

	maxMsgs := ""
	if cfg.MaxMsgs > 0 {
		maxMsgs = fmt.Sprintf("%d", cfg.MaxMsgs)
	}
	maxBytes := ""
	if cfg.MaxBytes > 0 {
		maxBytes = formatBytes(uint64(cfg.MaxBytes))
	}
	maxAge := ""
	if cfg.MaxAge > 0 {
		maxAge = cfg.MaxAge.String()
	}
	maxMsgSize := ""
	if cfg.MaxMsgSize > 0 {
		maxMsgSize = formatBytes(uint64(cfg.MaxMsgSize))
	}
	maxMsgsPerSubject := ""
	if cfg.MaxMsgsPerSubject > 0 {
		maxMsgsPerSubject = fmt.Sprintf("%d", cfg.MaxMsgsPerSubject)
	}
	dedupWindow := ""
	if cfg.Duplicates > 0 {
		dedupWindow = cfg.Duplicates.String()
	}

	// Build storage options with current value first
	storageOpts := []string{"File", "Memory"}
	if storageIdx == 1 {
		storageOpts = []string{"Memory", "File"}
	}
	retentionOpts := []string{"Limits", "Interest", "WorkQueue"}
	if retentionIdx == 1 {
		retentionOpts = []string{"Interest", "Limits", "WorkQueue"}
	} else if retentionIdx == 2 {
		retentionOpts = []string{"WorkQueue", "Limits", "Interest"}
	}
	discardOpts := []string{"Old", "New"}
	if discardIdx == 1 {
		discardOpts = []string{"New", "Old"}
	}
	replicaOpts := []string{"1", "3", "5"}
	if replicaIdx == 1 {
		replicaOpts = []string{"3", "1", "5"}
	} else if replicaIdx == 2 {
		replicaOpts = []string{"5", "1", "3"}
	}

	modal := components.NewFormBuilder().
		Text("name", "Name").
		Value(cfg.Name).
		Placeholder(cfg.Name).
		Done().
		Text("subjects", "Subjects").
		Value(strings.Join(cfg.Subjects, ", ")).
		Done().
		Text("description", "Description").
		Value(cfg.Description).
		Done().
		Select("storage", "Storage", storageOpts).
		Done().
		Select("retention", "Retention", retentionOpts).
		Done().
		Select("discard", "Discard", discardOpts).
		Done().
		Select("replicas", "Replicas", replicaOpts).
		Done().
		Text("max_msgs", "Max Messages").
		Value(maxMsgs).
		Placeholder("unlimited").
		Done().
		Text("max_bytes", "Max Bytes").
		Value(maxBytes).
		Placeholder("1GB, 500MB, or empty").
		Done().
		Text("max_age", "Max Age").
		Value(maxAge).
		Placeholder("24h, 7d, or empty").
		Done().
		Text("max_msg_size", "Max Msg Size").
		Value(maxMsgSize).
		Placeholder("1MB or empty").
		Done().
		Text("max_msgs_per_subject", "Max Msgs/Subject").
		Value(maxMsgsPerSubject).
		Placeholder("unlimited").
		Done().
		Text("dedup_window", "Dedup Window").
		Value(dedupWindow).
		Placeholder("2m or empty").
		Done().
		OnSubmit(func(values map[string]any) {
			// Force the name to match the original
			values["name"] = cfg.Name
			updCfg, err := buildStreamConfig(values)
			if err != nil {
				app.ShowError(err.Error())
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := app.Provider().UpdateStream(ctx, updCfg); err != nil {
					app.ShowError(err.Error())
				} else {
					app.ShowSuccess("Stream updated: " + updCfg.Name)
					if onUpdated != nil {
						onUpdated()
					}
				}
			}()
		}).
		AsFormModal(fmt.Sprintf("Edit Stream: %s", cfg.Name), 70, 28)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Save"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

// ── Consumer Edit Form ────────────────────────────────────────────────────

func showConsumerEditForm(app *App, streamName string, info *jetstream.ConsumerInfo, onUpdated func()) {
	cfg := info.Config

	name := cfg.Name
	if name == "" {
		name = cfg.Durable
	}

	// Editable fields: ack_wait, max_deliver, max_ack_pending
	ackWait := cfg.AckWait.String()
	maxDeliver := fmt.Sprintf("%d", cfg.MaxDeliver)
	maxAckPending := fmt.Sprintf("%d", cfg.MaxAckPending)
	description := cfg.Description

	modal := components.NewFormBuilder().
		Text("name", "Name (read-only)").
		Value(name).
		Done().
		Text("description", "Description").
		Value(description).
		Done().
		Text("ack_wait", "Ack Wait").
		Value(ackWait).
		Placeholder("30s").
		Done().
		Text("max_deliver", "Max Deliver").
		Value(maxDeliver).
		Placeholder("-1 = unlimited").
		Done().
		Text("max_ack_pending", "Max Ack Pending").
		Value(maxAckPending).
		Done().
		OnSubmit(func(values map[string]any) {
			// Start from the existing config to preserve immutable fields
			updCfg := cfg

			if desc := getString(values, "description"); desc != cfg.Description {
				updCfg.Description = desc
			}

			if v := getString(values, "ack_wait"); v != "" {
				d, err := parseDuration(v)
				if err != nil {
					app.ShowError(fmt.Sprintf("ack wait: %v", err))
					return
				}
				updCfg.AckWait = d
			}

			if v := getString(values, "max_deliver"); v != "" {
				n, err := strconv.Atoi(v)
				if err != nil {
					app.ShowError("max deliver: invalid number")
					return
				}
				updCfg.MaxDeliver = n
			}

			if v := getString(values, "max_ack_pending"); v != "" {
				n, err := strconv.Atoi(v)
				if err != nil {
					app.ShowError("max ack pending: invalid number")
					return
				}
				updCfg.MaxAckPending = n
			}

			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := app.Provider().UpdateConsumer(ctx, streamName, updCfg); err != nil {
					app.ShowError(err.Error())
				} else {
					app.ShowSuccess("Consumer updated: " + name)
					if onUpdated != nil {
						onUpdated()
					}
				}
			}()
		}).
		AsFormModal(fmt.Sprintf("Edit Consumer: %s", name), 70, 18)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Save"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

func showStreamCreateForm(app *App, onCreated func()) {
	modal := components.NewFormBuilder().
		Text("name", "Name").
		Placeholder("my-stream").
		Validate(validators.Required(), validators.PatternWithMessage(`^[a-zA-Z0-9_-]+$`, "alphanumeric, hyphens, underscores only")).
		Done().
		Text("subjects", "Subjects").
		Placeholder("orders.>").
		Done().
		Text("description", "Description").
		Placeholder("optional").
		Done().
		Select("storage", "Storage", []string{"File", "Memory"}).
		Done().
		Select("retention", "Retention", []string{"Limits", "Interest", "WorkQueue"}).
		Done().
		Select("discard", "Discard", []string{"Old", "New"}).
		Done().
		Select("replicas", "Replicas", []string{"1", "3", "5"}).
		Done().
		Text("max_msgs", "Max Messages").
		Placeholder("unlimited").
		Done().
		Text("max_bytes", "Max Bytes").
		Placeholder("1GB, 500MB, or empty").
		Done().
		Text("max_age", "Max Age").
		Placeholder("24h, 7d, or empty").
		Done().
		Text("max_msg_size", "Max Msg Size").
		Placeholder("1MB or empty").
		Done().
		Text("max_msgs_per_subject", "Max Msgs/Subject").
		Placeholder("unlimited").
		Done().
		Text("dedup_window", "Dedup Window").
		Placeholder("2m or empty").
		Done().
		OnSubmit(func(values map[string]any) {
			cfg, err := buildStreamConfig(values)
			if err != nil {
				app.ShowError(err.Error())
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := app.Provider().CreateStream(ctx, cfg); err != nil {
					app.ShowError(err.Error())
				} else {
					app.ShowSuccess("Stream created: " + cfg.Name)
					if onCreated != nil {
						onCreated()
					}
				}
			}()
		}).
		AsFormModal("Create Stream", 70, 28)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Create"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

func buildStreamConfig(values map[string]any) (jetstream.StreamConfig, error) {
	name := getString(values, "name")
	if name == "" {
		return jetstream.StreamConfig{}, fmt.Errorf("name is required")
	}

	cfg := jetstream.StreamConfig{
		Name: name,
	}

	if subj := getString(values, "subjects"); subj != "" {
		parts := strings.Split(subj, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		cfg.Subjects = parts
	}

	cfg.Description = getString(values, "description")

	switch getString(values, "storage") {
	case "Memory":
		cfg.Storage = jetstream.MemoryStorage
	default:
		cfg.Storage = jetstream.FileStorage
	}

	switch getString(values, "retention") {
	case "Interest":
		cfg.Retention = jetstream.InterestPolicy
	case "WorkQueue":
		cfg.Retention = jetstream.WorkQueuePolicy
	default:
		cfg.Retention = jetstream.LimitsPolicy
	}

	switch getString(values, "discard") {
	case "New":
		cfg.Discard = jetstream.DiscardNew
	default:
		cfg.Discard = jetstream.DiscardOld
	}

	if r := getString(values, "replicas"); r != "" {
		n, err := strconv.Atoi(r)
		if err != nil {
			return cfg, fmt.Errorf("invalid replicas: %s", r)
		}
		cfg.Replicas = n
	} else {
		cfg.Replicas = 1
	}

	if v := getString(values, "max_msgs"); v != "" {
		n, err := parseOptionalInt64(v)
		if err != nil {
			return cfg, fmt.Errorf("max messages: %w", err)
		}
		cfg.MaxMsgs = n
	} else {
		cfg.MaxMsgs = -1
	}

	if v := getString(values, "max_bytes"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return cfg, fmt.Errorf("max bytes: %w", err)
		}
		cfg.MaxBytes = n
	} else {
		cfg.MaxBytes = -1
	}

	if v := getString(values, "max_age"); v != "" {
		d, err := parseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("max age: %w", err)
		}
		cfg.MaxAge = d
	}

	if v := getString(values, "max_msg_size"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return cfg, fmt.Errorf("max msg size: %w", err)
		}
		cfg.MaxMsgSize = int32(n)
	} else {
		cfg.MaxMsgSize = -1
	}

	if v := getString(values, "max_msgs_per_subject"); v != "" {
		n, err := parseOptionalInt64(v)
		if err != nil {
			return cfg, fmt.Errorf("max msgs/subject: %w", err)
		}
		cfg.MaxMsgsPerSubject = n
	} else {
		cfg.MaxMsgsPerSubject = -1
	}

	if v := getString(values, "dedup_window"); v != "" {
		d, err := parseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("dedup window: %w", err)
		}
		cfg.Duplicates = d
	}

	return cfg, nil
}

func showConsumerCreateForm(app *App, streamName string, onCreated func()) {
	modal := components.NewFormBuilder().
		Text("name", "Name").
		Placeholder("my-consumer").
		Validate(validators.Required()).
		Done().
		Text("filter_subject", "Filter Subject").
		Placeholder("optional subject filter").
		Done().
		Select("deliver_policy", "Deliver Policy", []string{"All", "Last", "New", "LastPerSubject"}).
		Done().
		Select("ack_policy", "Ack Policy", []string{"Explicit", "None", "All"}).
		Done().
		Text("ack_wait", "Ack Wait").
		Value("30s").
		Placeholder("30s").
		Done().
		Text("max_deliver", "Max Deliver").
		Value("-1").
		Placeholder("-1 = unlimited").
		Done().
		Text("max_ack_pending", "Max Ack Pending").
		Value("1000").
		Done().
		Select("replay_policy", "Replay Policy", []string{"Instant", "Original"}).
		Done().
		OnSubmit(func(values map[string]any) {
			cfg, err := buildConsumerConfig(values)
			if err != nil {
				app.ShowError(err.Error())
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := app.Provider().CreateConsumer(ctx, streamName, cfg); err != nil {
					app.ShowError(err.Error())
				} else {
					app.ShowSuccess("Consumer created: " + cfg.Name)
					if onCreated != nil {
						onCreated()
					}
				}
			}()
		}).
		AsFormModal(fmt.Sprintf("Create Consumer: %s", streamName), 70, 24)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Create"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

func buildConsumerConfig(values map[string]any) (jetstream.ConsumerConfig, error) {
	name := getString(values, "name")
	if name == "" {
		return jetstream.ConsumerConfig{}, fmt.Errorf("name is required")
	}

	cfg := jetstream.ConsumerConfig{
		Name:    name,
		Durable: name,
	}

	if fs := getString(values, "filter_subject"); fs != "" {
		cfg.FilterSubject = fs
	}

	switch getString(values, "deliver_policy") {
	case "Last":
		cfg.DeliverPolicy = jetstream.DeliverLastPolicy
	case "New":
		cfg.DeliverPolicy = jetstream.DeliverNewPolicy
	case "LastPerSubject":
		cfg.DeliverPolicy = jetstream.DeliverLastPerSubjectPolicy
	default:
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	}

	switch getString(values, "ack_policy") {
	case "None":
		cfg.AckPolicy = jetstream.AckNonePolicy
	case "All":
		cfg.AckPolicy = jetstream.AckAllPolicy
	default:
		cfg.AckPolicy = jetstream.AckExplicitPolicy
	}

	if v := getString(values, "ack_wait"); v != "" {
		d, err := parseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("ack wait: %w", err)
		}
		cfg.AckWait = d
	}

	if v := getString(values, "max_deliver"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("max deliver: invalid number")
		}
		cfg.MaxDeliver = n
	}

	if v := getString(values, "max_ack_pending"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("max ack pending: invalid number")
		}
		cfg.MaxAckPending = n
	}

	switch getString(values, "replay_policy") {
	case "Original":
		cfg.ReplayPolicy = jetstream.ReplayOriginalPolicy
	default:
		cfg.ReplayPolicy = jetstream.ReplayInstantPolicy
	}

	return cfg, nil
}

func showKVCreateForm(app *App, onCreated func()) {
	modal := components.NewFormBuilder().
		Text("bucket", "Bucket Name").
		Placeholder("my-bucket").
		Validate(validators.Required(), validators.PatternWithMessage(`^[a-zA-Z0-9_-]+$`, "alphanumeric, hyphens, underscores only")).
		Done().
		Text("description", "Description").
		Placeholder("optional").
		Done().
		Select("history", "History", []string{"1", "2", "5", "10", "64"}).
		Done().
		Text("ttl", "TTL").
		Placeholder("24h, 7d, or empty").
		Done().
		Text("max_value_size", "Max Value Size").
		Placeholder("1MB or empty").
		Done().
		Text("max_bytes", "Max Bytes").
		Placeholder("1GB or empty").
		Done().
		Select("replicas", "Replicas", []string{"1", "3", "5"}).
		Done().
		OnSubmit(func(values map[string]any) {
			cfg, err := buildKVConfig(values)
			if err != nil {
				app.ShowError(err.Error())
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := app.Provider().CreateKeyValue(ctx, cfg); err != nil {
					app.ShowError(err.Error())
				} else {
					app.ShowSuccess("KV bucket created: " + cfg.Bucket)
					if onCreated != nil {
						onCreated()
					}
				}
			}()
		}).
		AsFormModal("Create KV Bucket", 60, 20)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Create"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

func buildKVConfig(values map[string]any) (jetstream.KeyValueConfig, error) {
	bucket := getString(values, "bucket")
	if bucket == "" {
		return jetstream.KeyValueConfig{}, fmt.Errorf("bucket name is required")
	}

	cfg := jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: getString(values, "description"),
	}

	if h := getString(values, "history"); h != "" {
		n, err := strconv.Atoi(h)
		if err != nil {
			return cfg, fmt.Errorf("history: invalid number")
		}
		cfg.History = uint8(n)
	} else {
		cfg.History = 1
	}

	if v := getString(values, "ttl"); v != "" {
		d, err := parseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("ttl: %w", err)
		}
		cfg.TTL = d
	}

	if v := getString(values, "max_value_size"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return cfg, fmt.Errorf("max value size: %w", err)
		}
		cfg.MaxValueSize = int32(n)
	}

	if v := getString(values, "max_bytes"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return cfg, fmt.Errorf("max bytes: %w", err)
		}
		cfg.MaxBytes = n
	}

	if r := getString(values, "replicas"); r != "" {
		n, err := strconv.Atoi(r)
		if err != nil {
			return cfg, fmt.Errorf("invalid replicas: %s", r)
		}
		cfg.Replicas = n
	} else {
		cfg.Replicas = 1
	}

	return cfg, nil
}

func showObjectStoreCreateForm(app *App, onCreated func()) {
	modal := components.NewFormBuilder().
		Text("bucket", "Bucket Name").
		Placeholder("my-objects").
		Validate(validators.Required(), validators.PatternWithMessage(`^[a-zA-Z0-9_-]+$`, "alphanumeric, hyphens, underscores only")).
		Done().
		Text("description", "Description").
		Placeholder("optional").
		Done().
		Text("max_bytes", "Max Bytes").
		Placeholder("1GB or empty").
		Done().
		Select("replicas", "Replicas", []string{"1", "3", "5"}).
		Done().
		OnSubmit(func(values map[string]any) {
			cfg, err := buildObjectStoreConfig(values)
			if err != nil {
				app.ShowError(err.Error())
				return
			}
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := app.Provider().CreateObjectStore(ctx, cfg); err != nil {
					app.ShowError(err.Error())
				} else {
					app.ShowSuccess("Object store created: " + cfg.Bucket)
					if onCreated != nil {
						onCreated()
					}
				}
			}()
		}).
		AsFormModal("Create Object Store", 60, 14)

	modal.SetHints([]components.KeyHint{
		{Key: "Tab", Description: "Next"},
		{Key: "Ctrl+S", Description: "Create"},
		{Key: "Esc", Description: "Cancel"},
	})

	app.app.Pages().Push(modal)
}

func buildObjectStoreConfig(values map[string]any) (jetstream.ObjectStoreConfig, error) {
	bucket := getString(values, "bucket")
	if bucket == "" {
		return jetstream.ObjectStoreConfig{}, fmt.Errorf("bucket name is required")
	}

	cfg := jetstream.ObjectStoreConfig{
		Bucket:      bucket,
		Description: getString(values, "description"),
	}

	if v := getString(values, "max_bytes"); v != "" {
		n, err := parseBytes(v)
		if err != nil {
			return cfg, fmt.Errorf("max bytes: %w", err)
		}
		cfg.MaxBytes = n
	}

	if r := getString(values, "replicas"); r != "" {
		n, err := strconv.Atoi(r)
		if err != nil {
			return cfg, fmt.Errorf("invalid replicas: %s", r)
		}
		cfg.Replicas = n
	} else {
		cfg.Replicas = 1
	}

	return cfg, nil
}
