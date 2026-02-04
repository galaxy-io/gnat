package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	liveMode  = flag.Bool("live", false, "Run in live mode, continuously publishing messages")
	liveRate  = flag.Duration("rate", 500*time.Millisecond, "Message publish rate in live mode")
	showStats = flag.Bool("stats", false, "Show stream statistics after seeding")
	extraMsgs = flag.Int("extra", 0, "Publish additional messages to streams for history testing")
)

func main() {
	flag.Parse()

	nc, err := nats.Connect("nats://localhost:4222", nats.Name("gnat-seeder"))
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedStreams(ctx, js)
	seedConsumers(ctx, js)
	seedMessages(ctx, js, nc)
	seedKV(ctx, js)
	seedObjectStore(ctx, js)

	// Publish extra messages for history testing
	if *extraMsgs > 0 {
		seedExtraMessages(ctx, js, *extraMsgs)
	}

	fmt.Println("\nseeding complete")

	// Show stream statistics
	if *showStats || *extraMsgs > 0 {
		printStreamStats(ctx, js)
	}

	if *liveMode {
		fmt.Printf("\nStarting live publisher (rate: %v)...\n", *liveRate)
		fmt.Println("Publishing to: orders.*, events.*, logs.*, metrics.*, notify.*")
		fmt.Println("Press Ctrl+C to stop\n")
		runLivePublisher(nc, js, *liveRate)
	}
}

func seedStreams(ctx context.Context, js jetstream.JetStream) {
	streams := []jetstream.StreamConfig{
		{
			Name:        "ORDERS",
			Subjects:    []string{"orders.>"},
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      24 * time.Hour,
			MaxBytes:    1024 * 1024 * 100,
			MaxMsgSize:  1024 * 64,
			Storage:     jetstream.FileStorage,
			Replicas:    1,
			Description: "Order processing pipeline",
			Discard:     jetstream.DiscardOld,
		},
		{
			Name:        "EVENTS",
			Subjects:    []string{"events.>"},
			Retention:   jetstream.InterestPolicy,
			MaxAge:      72 * time.Hour,
			Storage:     jetstream.FileStorage,
			Replicas:    1,
			Description: "Application event bus",
		},
		{
			Name:        "LOGS",
			Subjects:    []string{"logs.>"},
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      12 * time.Hour,
			MaxMsgs:     50000,
			Storage:     jetstream.FileStorage,
			Replicas:    1,
			Description: "Centralized log aggregation",
			Discard:     jetstream.DiscardOld,
		},
		{
			Name:        "NOTIFICATIONS",
			Subjects:    []string{"notify.>"},
			Retention:   jetstream.WorkQueuePolicy,
			MaxAge:      1 * time.Hour,
			Storage:     jetstream.MemoryStorage,
			Replicas:    1,
			Description: "Transient notification delivery",
		},
		{
			Name:        "METRICS",
			Subjects:    []string{"metrics.>"},
			Retention:   jetstream.LimitsPolicy,
			MaxAge:      6 * time.Hour,
			MaxMsgs:     100000,
			Storage:     jetstream.FileStorage,
			Replicas:    1,
			Description: "System metrics collection",
			Discard:     jetstream.DiscardOld,
		},
	}

	for _, cfg := range streams {
		s, err := js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			log.Printf("stream %s: %v", cfg.Name, err)
			continue
		}
		info, _ := s.Info(ctx)
		fmt.Printf("stream %-15s msgs=%d bytes=%d\n", cfg.Name, info.State.Msgs, info.State.Bytes)
	}
}

func seedConsumers(ctx context.Context, js jetstream.JetStream) {
	consumers := []struct {
		stream string
		cfg    jetstream.ConsumerConfig
	}{
		{
			stream: "ORDERS",
			cfg: jetstream.ConsumerConfig{
				Name:           "order-processor",
				Durable:        "order-processor",
				Description:    "Main order processing worker",
				AckPolicy:      jetstream.AckExplicitPolicy,
				AckWait:        30 * time.Second,
				MaxDeliver:     5,
				MaxAckPending:  1000,
				FilterSubject:  "orders.created",
				DeliverPolicy:  jetstream.DeliverAllPolicy,
				MaxWaiting:     512,
			},
		},
		{
			stream: "ORDERS",
			cfg: jetstream.ConsumerConfig{
				Name:           "order-analytics",
				Durable:        "order-analytics",
				Description:    "Analytics consumer for order data",
				AckPolicy:      jetstream.AckExplicitPolicy,
				AckWait:        60 * time.Second,
				MaxDeliver:     3,
				MaxAckPending:  500,
				FilterSubjects: []string{"orders.created", "orders.completed", "orders.cancelled"},
				DeliverPolicy:  jetstream.DeliverAllPolicy,
			},
		},
		{
			stream: "ORDERS",
			cfg: jetstream.ConsumerConfig{
				Name:          "order-notifications",
				Durable:       "order-notifications",
				Description:   "Sends notifications on order status changes",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       10 * time.Second,
				MaxDeliver:    10,
				MaxAckPending: 200,
				FilterSubject: "orders.>",
				DeliverPolicy: jetstream.DeliverNewPolicy,
			},
		},
		{
			stream: "EVENTS",
			cfg: jetstream.ConsumerConfig{
				Name:          "event-handler",
				Durable:       "event-handler",
				Description:   "Primary event handler",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       15 * time.Second,
				MaxDeliver:    3,
				MaxAckPending: 256,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			},
		},
		{
			stream: "EVENTS",
			cfg: jetstream.ConsumerConfig{
				Name:          "event-archiver",
				Durable:       "event-archiver",
				Description:   "Archives events to cold storage",
				AckPolicy:     jetstream.AckAllPolicy,
				AckWait:       120 * time.Second,
				MaxDeliver:    2,
				MaxAckPending: 2000,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			},
		},
		{
			stream: "LOGS",
			cfg: jetstream.ConsumerConfig{
				Name:          "log-indexer",
				Durable:       "log-indexer",
				Description:   "Indexes logs for search",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       20 * time.Second,
				MaxDeliver:    3,
				MaxAckPending: 5000,
				FilterSubject: "logs.>",
				DeliverPolicy: jetstream.DeliverAllPolicy,
			},
		},
		{
			stream: "LOGS",
			cfg: jetstream.ConsumerConfig{
				Name:          "log-alerts",
				Durable:       "log-alerts",
				Description:   "Monitors logs for error patterns",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       5 * time.Second,
				MaxDeliver:    1,
				MaxAckPending: 100,
				FilterSubject: "logs.error.>",
				DeliverPolicy: jetstream.DeliverNewPolicy,
			},
		},
		{
			stream: "METRICS",
			cfg: jetstream.ConsumerConfig{
				Name:          "metrics-aggregator",
				Durable:       "metrics-aggregator",
				Description:   "Aggregates raw metrics into summaries",
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       10 * time.Second,
				MaxDeliver:    2,
				MaxAckPending: 10000,
				DeliverPolicy: jetstream.DeliverAllPolicy,
			},
		},
	}

	for _, c := range consumers {
		s, err := js.Stream(ctx, c.stream)
		if err != nil {
			log.Printf("consumer %s: stream %s: %v", c.cfg.Name, c.stream, err)
			continue
		}
		_, err = s.CreateOrUpdateConsumer(ctx, c.cfg)
		if err != nil {
			log.Printf("consumer %s: %v", c.cfg.Name, err)
			continue
		}
		fmt.Printf("consumer %-20s on %s\n", c.cfg.Name, c.stream)
	}
}

func seedMessages(ctx context.Context, js jetstream.JetStream, nc *nats.Conn) {
	type msg struct {
		subject string
		data    string
	}

	regions := []string{"us-east", "us-west", "eu-west", "ap-south"}
	statuses := []string{"created", "processing", "completed", "cancelled", "refunded"}
	levels := []string{"info", "warn", "error", "debug"}
	services := []string{"api", "worker", "scheduler", "gateway", "auth"}
	eventTypes := []string{"user.signup", "user.login", "user.logout", "payment.success", "payment.failed", "item.viewed", "item.purchased", "cart.updated"}
	metricNames := []string{"cpu_usage", "mem_usage", "disk_io", "net_rx", "net_tx", "req_latency", "queue_depth", "gc_pause"}

	var messages []msg

	// Orders
	for i := 0; i < 200; i++ {
		status := statuses[rand.Intn(len(statuses))]
		region := regions[rand.Intn(len(regions))]
		amount := rand.Float64()*500 + 5
		messages = append(messages, msg{
			subject: fmt.Sprintf("orders.%s", status),
			data:    fmt.Sprintf(`{"order_id":"ORD-%06d","status":"%s","region":"%s","amount":%.2f,"items":%d,"customer":"cust-%04d"}`, i, status, region, amount, rand.Intn(10)+1, rand.Intn(5000)),
		})
	}

	// Events
	for i := 0; i < 300; i++ {
		evt := eventTypes[rand.Intn(len(eventTypes))]
		region := regions[rand.Intn(len(regions))]
		messages = append(messages, msg{
			subject: fmt.Sprintf("events.%s", evt),
			data:    fmt.Sprintf(`{"event":"%s","user_id":"usr-%05d","region":"%s","session":"%08x","metadata":{"source":"web","version":"2.1.0"}}`, evt, rand.Intn(10000), region, rand.Uint32()),
		})
	}

	// Logs
	for i := 0; i < 500; i++ {
		level := levels[rand.Intn(len(levels))]
		svc := services[rand.Intn(len(services))]
		messages = append(messages, msg{
			subject: fmt.Sprintf("logs.%s.%s", level, svc),
			data:    fmt.Sprintf(`{"level":"%s","service":"%s","message":"Sample log message #%d","trace_id":"%08x%08x","host":"host-%02d"}`, level, svc, i, rand.Uint32(), rand.Uint32(), rand.Intn(20)),
		})
	}

	// Notifications
	for i := 0; i < 50; i++ {
		channels := []string{"email", "sms", "push", "slack"}
		ch := channels[rand.Intn(len(channels))]
		messages = append(messages, msg{
			subject: fmt.Sprintf("notify.%s", ch),
			data:    fmt.Sprintf(`{"channel":"%s","recipient":"user-%04d","template":"tmpl-%02d","priority":"%s"}`, ch, rand.Intn(5000), rand.Intn(15), []string{"low", "normal", "high"}[rand.Intn(3)]),
		})
	}

	// Metrics
	for i := 0; i < 400; i++ {
		metric := metricNames[rand.Intn(len(metricNames))]
		host := fmt.Sprintf("host-%02d", rand.Intn(20))
		messages = append(messages, msg{
			subject: fmt.Sprintf("metrics.%s.%s", host, metric),
			data:    fmt.Sprintf(`{"metric":"%s","host":"%s","value":%.4f,"unit":"%s","tags":{"env":"prod","region":"%s"}}`, metric, host, rand.Float64()*100, []string{"percent", "bytes", "ms", "count"}[rand.Intn(4)], regions[rand.Intn(len(regions))]),
		})
	}

	// Shuffle for realistic interleaving
	rand.Shuffle(len(messages), func(i, j int) {
		messages[i], messages[j] = messages[j], messages[i]
	})

	published := 0
	for _, m := range messages {
		if _, err := js.Publish(ctx, m.subject, []byte(m.data)); err != nil {
			log.Printf("publish %s: %v", m.subject, err)
			continue
		}
		published++
	}
	fmt.Printf("published %d messages across all streams\n", published)

	// Consume some messages to create realistic ack/pending state
	simulateConsumption(ctx, js)
}

func simulateConsumption(ctx context.Context, js jetstream.JetStream) {
	consume := []struct {
		stream   string
		consumer string
		count    int
		ackRate  float64 // fraction of messages to ack
	}{
		{"ORDERS", "order-processor", 120, 0.9},
		{"ORDERS", "order-analytics", 80, 0.95},
		{"EVENTS", "event-handler", 200, 0.85},
		{"EVENTS", "event-archiver", 150, 1.0},
		{"LOGS", "log-indexer", 300, 0.7},
		{"METRICS", "metrics-aggregator", 250, 0.8},
	}

	for _, c := range consume {
		s, err := js.Stream(ctx, c.stream)
		if err != nil {
			continue
		}
		cons, err := s.Consumer(ctx, c.consumer)
		if err != nil {
			continue
		}

		fetched := 0
		acked := 0
		batch, err := cons.Fetch(c.count, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			log.Printf("fetch %s/%s: %v", c.stream, c.consumer, err)
			continue
		}
		for msg := range batch.Messages() {
			fetched++
			if rand.Float64() < c.ackRate {
				_ = msg.Ack()
				acked++
			}
			// Leave some un-acked to create pending state
		}
		fmt.Printf("consumed %-20s fetched=%d acked=%d pending=%d\n", c.consumer, fetched, acked, fetched-acked)
	}
}

func seedKV(ctx context.Context, js jetstream.JetStream) {
	buckets := []struct {
		cfg     jetstream.KeyValueConfig
		entries map[string]string
	}{
		{
			cfg: jetstream.KeyValueConfig{
				Bucket:      "config",
				Description: "Application configuration",
				History:     5,
				TTL:         0,
			},
			entries: map[string]string{
				"app.name":                  `"gnat"`,
				"app.version":               `"1.0.0"`,
				"app.debug":                 `false`,
				"database.host":             `"db.internal:5432"`,
				"database.pool_size":        `20`,
				"database.timeout_ms":       `5000`,
				"cache.ttl_seconds":         `300`,
				"cache.max_size_mb":         `512`,
				"rate_limit.requests":       `1000`,
				"rate_limit.window_seconds": `60`,
				"feature.dark_mode":         `true`,
				"feature.beta_access":       `false`,
				"feature.notifications":     `true`,
			},
		},
		{
			cfg: jetstream.KeyValueConfig{
				Bucket:      "sessions",
				Description: "Active user sessions",
				History:     1,
				TTL:         30 * time.Minute,
			},
			entries: map[string]string{
				"sess-a1b2c3": `{"user_id":"usr-00042","role":"admin","ip":"10.0.1.5","started":"2025-01-15T10:00:00Z"}`,
				"sess-d4e5f6": `{"user_id":"usr-01337","role":"user","ip":"10.0.2.18","started":"2025-01-15T10:05:00Z"}`,
				"sess-g7h8i9": `{"user_id":"usr-00099","role":"editor","ip":"10.0.1.22","started":"2025-01-15T09:45:00Z"}`,
				"sess-j0k1l2": `{"user_id":"usr-02500","role":"user","ip":"192.168.1.100","started":"2025-01-15T10:12:00Z"}`,
				"sess-m3n4o5": `{"user_id":"usr-00001","role":"superadmin","ip":"10.0.0.1","started":"2025-01-15T08:00:00Z"}`,
			},
		},
		{
			cfg: jetstream.KeyValueConfig{
				Bucket:      "feature-flags",
				Description: "Feature flag toggles",
				History:     10,
			},
			entries: map[string]string{
				"new-dashboard":     `{"enabled":true,"rollout":100,"description":"New dashboard UI"}`,
				"dark-mode":         `{"enabled":true,"rollout":100,"description":"Dark mode support"}`,
				"export-csv":        `{"enabled":true,"rollout":50,"description":"CSV export feature"}`,
				"ai-suggestions":    `{"enabled":false,"rollout":0,"description":"AI-powered suggestions"}`,
				"multi-tenant":      `{"enabled":true,"rollout":25,"description":"Multi-tenant support"}`,
				"websocket-streams": `{"enabled":false,"rollout":0,"description":"WebSocket streaming"}`,
			},
		},
		{
			cfg: jetstream.KeyValueConfig{
				Bucket:      "cache",
				Description: "Application cache layer",
				History:     1,
				TTL:         5 * time.Minute,
			},
			entries: map[string]string{
				"user.42.profile":   `{"name":"Alice","email":"alice@example.com","plan":"pro"}`,
				"user.1337.profile": `{"name":"Bob","email":"bob@example.com","plan":"free"}`,
				"product.101":       `{"name":"Widget","price":9.99,"stock":142}`,
				"product.102":       `{"name":"Gadget","price":24.99,"stock":37}`,
				"product.103":       `{"name":"Doohickey","price":4.50,"stock":500}`,
				"stats.daily":       `{"requests":145832,"errors":23,"p99_ms":142}`,
				"stats.hourly":      `{"requests":6120,"errors":1,"p99_ms":98}`,
			},
		},
	}

	for _, b := range buckets {
		kv, err := js.CreateOrUpdateKeyValue(ctx, b.cfg)
		if err != nil {
			log.Printf("kv %s: %v", b.cfg.Bucket, err)
			continue
		}
		for k, v := range b.entries {
			if _, err := kv.PutString(ctx, k, v); err != nil {
				log.Printf("kv put %s/%s: %v", b.cfg.Bucket, k, err)
			}
		}
		// Do a few updates on config bucket to generate history
		if b.cfg.Bucket == "config" {
			kv.PutString(ctx, "app.version", `"0.9.0"`)
			kv.PutString(ctx, "app.version", `"0.9.5"`)
			kv.PutString(ctx, "app.version", `"1.0.0"`)
			kv.PutString(ctx, "cache.ttl_seconds", `600`)
			kv.PutString(ctx, "cache.ttl_seconds", `300`)
		}
		fmt.Printf("kv %-20s keys=%d\n", b.cfg.Bucket, len(b.entries))
	}
}

func seedObjectStore(ctx context.Context, js jetstream.JetStream) {
	stores := []struct {
		cfg     jetstream.ObjectStoreConfig
		objects []struct {
			name string
			data []byte
		}
	}{
		{
			cfg: jetstream.ObjectStoreConfig{
				Bucket:      "uploads",
				Description: "User file uploads",
			},
			objects: []struct {
				name string
				data []byte
			}{
				{"reports/2025-q1.csv", []byte("date,revenue,orders\n2025-01-01,15234.50,142\n2025-01-02,18921.00,189\n2025-01-03,12003.75,98\n")},
				{"reports/2025-q2.csv", []byte("date,revenue,orders\n2025-04-01,22100.00,201\n2025-04-02,19450.25,178\n")},
				{"avatars/usr-00042.png", generateFakeImage(128)},
				{"avatars/usr-01337.png", generateFakeImage(256)},
				{"docs/readme.txt", []byte("This is a sample readme file for testing the object store.\nIt contains multiple lines of text.\n")},
				{"docs/changelog.md", []byte("# Changelog\n\n## v1.0.0\n- Initial release\n- Added core features\n\n## v0.9.0\n- Beta release\n")},
			},
		},
		{
			cfg: jetstream.ObjectStoreConfig{
				Bucket:      "backups",
				Description: "Database backup snapshots",
			},
			objects: []struct {
				name string
				data []byte
			}{
				{"db-2025-01-14.sql.gz", generateFakeData(4096)},
				{"db-2025-01-15.sql.gz", generateFakeData(4200)},
				{"config-2025-01-15.tar.gz", generateFakeData(1024)},
			},
		},
		{
			cfg: jetstream.ObjectStoreConfig{
				Bucket:      "templates",
				Description: "Email and notification templates",
			},
			objects: []struct {
				name string
				data []byte
			}{
				{"email/welcome.html", []byte("<html><body><h1>Welcome {{.Name}}!</h1><p>Thanks for signing up.</p></body></html>")},
				{"email/reset-password.html", []byte("<html><body><h1>Password Reset</h1><p>Click <a href='{{.Link}}'>here</a> to reset.</p></body></html>")},
				{"email/order-confirmation.html", []byte("<html><body><h1>Order #{{.OrderID}}</h1><p>Total: ${{.Total}}</p></body></html>")},
				{"sms/verification.txt", []byte("Your verification code is {{.Code}}. Expires in 10 minutes.")},
				{"push/new-message.json", []byte(`{"title":"New Message","body":"{{.Sender}} sent you a message","data":{"type":"message"}}`)},
			},
		},
	}

	for _, s := range stores {
		os, err := js.CreateOrUpdateObjectStore(ctx, s.cfg)
		if err != nil {
			log.Printf("obj store %s: %v", s.cfg.Bucket, err)
			continue
		}
		for _, obj := range s.objects {
			_, err := os.PutBytes(ctx, obj.name, obj.data)
			if err != nil {
				log.Printf("obj put %s/%s: %v", s.cfg.Bucket, obj.name, err)
			}
		}
		fmt.Printf("obj store %-15s objects=%d\n", s.cfg.Bucket, len(s.objects))
	}
}

func generateFakeImage(size int) []byte {
	data := make([]byte, size)
	// PNG header
	copy(data, []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A})
	for i := 8; i < size; i++ {
		data[i] = byte(rand.Intn(256))
	}
	return data
}

func generateFakeData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rand.Intn(256))
	}
	return data
}

// printStreamStats shows current message counts for all streams
func printStreamStats(ctx context.Context, js jetstream.JetStream) {
	fmt.Println("\n=== Stream Statistics ===")
	streamNames := []string{"ORDERS", "EVENTS", "LOGS", "NOTIFICATIONS", "METRICS"}
	for _, name := range streamNames {
		s, err := js.Stream(ctx, name)
		if err != nil {
			fmt.Printf("%-15s error: %v\n", name, err)
			continue
		}
		info, err := s.Info(ctx)
		if err != nil {
			fmt.Printf("%-15s error: %v\n", name, err)
			continue
		}
		fmt.Printf("%-15s msgs=%-6d bytes=%-10s subjects=%d\n",
			name, info.State.Msgs, formatBytes(info.State.Bytes), info.State.NumSubjects)
	}
	fmt.Println("\nUse these subjects in the monitor with JetStream mode:")
	fmt.Println("  orders.>     - Order events (created, processing, completed, cancelled)")
	fmt.Println("  events.>     - Application events (user.*, payment.*)")
	fmt.Println("  logs.>       - Log messages (info, warn, error, debug)")
	fmt.Println("  metrics.>    - System metrics")
	fmt.Println("  notify.>     - Notifications (email, sms, push, slack)")
}

func formatBytes(b uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
	)
	switch {
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// seedExtraMessages publishes additional messages for history testing
func seedExtraMessages(ctx context.Context, js jetstream.JetStream, count int) {
	fmt.Printf("\nPublishing %d extra messages for history testing...\n", count)

	regions := []string{"us-east", "us-west", "eu-west", "ap-south"}
	statuses := []string{"created", "processing", "completed", "cancelled"}
	levels := []string{"info", "warn", "error", "debug"}
	services := []string{"api", "worker", "scheduler", "gateway", "auth"}

	published := 0
	for i := 0; i < count; i++ {
		var subject, data string
		ts := time.Now().Add(-time.Duration(count-i) * time.Second).Format(time.RFC3339)

		switch i % 5 {
		case 0: // Order
			status := statuses[rand.Intn(len(statuses))]
			subject = fmt.Sprintf("orders.%s", status)
			data = fmt.Sprintf(`{"ts":"%s","order_id":"HIST-%06d","status":"%s","region":"%s","amount":%.2f}`,
				ts, i, status, regions[rand.Intn(len(regions))], rand.Float64()*500+5)
		case 1: // Event
			subject = fmt.Sprintf("events.user.action")
			data = fmt.Sprintf(`{"ts":"%s","event":"user.action","user_id":"usr-%05d","action":"history_test"}`, ts, rand.Intn(10000))
		case 2: // Log
			level := levels[rand.Intn(len(levels))]
			svc := services[rand.Intn(len(services))]
			subject = fmt.Sprintf("logs.%s.%s", level, svc)
			data = fmt.Sprintf(`{"ts":"%s","level":"%s","service":"%s","msg":"History test message #%d"}`, ts, level, svc, i)
		case 3: // Metric
			subject = fmt.Sprintf("metrics.host-%02d.cpu_usage", rand.Intn(10))
			data = fmt.Sprintf(`{"ts":"%s","metric":"cpu_usage","value":%.2f}`, ts, rand.Float64()*100)
		case 4: // Notification
			subject = fmt.Sprintf("notify.test")
			data = fmt.Sprintf(`{"ts":"%s","channel":"test","msg":"History test #%d"}`, ts, i)
		}

		if _, err := js.Publish(ctx, subject, []byte(data)); err != nil {
			log.Printf("publish %s: %v", subject, err)
			continue
		}
		published++
	}
	fmt.Printf("Published %d extra messages\n", published)
}

// runLivePublisher continuously publishes messages until interrupted
func runLivePublisher(nc *nats.Conn, js jetstream.JetStream, rate time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(rate)
	defer ticker.Stop()

	regions := []string{"us-east", "us-west", "eu-west", "ap-south"}
	statuses := []string{"created", "processing", "completed", "cancelled"}
	levels := []string{"info", "warn", "error", "debug"}
	services := []string{"api", "worker", "scheduler", "gateway", "auth"}
	eventTypes := []string{"user.signup", "user.login", "user.logout", "payment.success", "payment.failed"}
	metricNames := []string{"cpu_usage", "mem_usage", "disk_io", "req_latency", "queue_depth"}
	channels := []string{"email", "sms", "push", "slack"}

	msgCount := 0
	startTime := time.Now()

	for {
		select {
		case <-sigCh:
			elapsed := time.Since(startTime)
			fmt.Printf("\nStopped. Published %d messages in %v (%.1f msg/s)\n",
				msgCount, elapsed.Round(time.Second), float64(msgCount)/elapsed.Seconds())
			return

		case <-ticker.C:
			var subject, data string
			now := time.Now().Format(time.RFC3339Nano)

			// Rotate through different message types
			switch rand.Intn(5) {
			case 0: // Order
				status := statuses[rand.Intn(len(statuses))]
				region := regions[rand.Intn(len(regions))]
				subject = fmt.Sprintf("orders.%s", status)
				data = fmt.Sprintf(`{"ts":"%s","order_id":"ORD-%06d","status":"%s","region":"%s","amount":%.2f}`,
					now, rand.Intn(999999), status, region, rand.Float64()*500+5)

			case 1: // Event
				evt := eventTypes[rand.Intn(len(eventTypes))]
				subject = fmt.Sprintf("events.%s", evt)
				data = fmt.Sprintf(`{"ts":"%s","event":"%s","user_id":"usr-%05d","session":"%08x"}`,
					now, evt, rand.Intn(10000), rand.Uint32())

			case 2: // Log
				level := levels[rand.Intn(len(levels))]
				svc := services[rand.Intn(len(services))]
				subject = fmt.Sprintf("logs.%s.%s", level, svc)
				data = fmt.Sprintf(`{"ts":"%s","level":"%s","service":"%s","msg":"Request processed","trace":"%08x"}`,
					now, level, svc, rand.Uint32())

			case 3: // Metric
				metric := metricNames[rand.Intn(len(metricNames))]
				host := fmt.Sprintf("host-%02d", rand.Intn(10))
				subject = fmt.Sprintf("metrics.%s.%s", host, metric)
				data = fmt.Sprintf(`{"ts":"%s","metric":"%s","host":"%s","value":%.2f}`,
					now, metric, host, rand.Float64()*100)

			case 4: // Notification
				ch := channels[rand.Intn(len(channels))]
				subject = fmt.Sprintf("notify.%s", ch)
				data = fmt.Sprintf(`{"ts":"%s","channel":"%s","recipient":"user-%04d","priority":"%s"}`,
					now, ch, rand.Intn(5000), []string{"low", "normal", "high"}[rand.Intn(3)])
			}

			// Publish to JetStream
			if _, err := js.Publish(ctx, subject, []byte(data)); err != nil {
				log.Printf("publish error: %v", err)
				continue
			}

			msgCount++
			if msgCount%100 == 0 {
				fmt.Printf("Published %d messages...\n", msgCount)
			}
		}
	}
}
