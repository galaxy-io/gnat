package nats

import (
	"context"
	"time"

	"github.com/atterpac/gnat/internal/config"
	"github.com/nats-io/nats.go/jetstream"
)

// RawMessage wraps message data retrieved directly from a stream.
type RawMessage struct {
	Subject  string
	Sequence uint64
	Data     []byte
	Headers  map[string][]string
	Time     time.Time
}

// ServerInfo bundles identity and configuration of the connected NATS server.
type ServerInfo struct {
	Name        string        // Server name (may be empty for older servers)
	ID          string        // Unique server ID
	Version     string        // Server version string, e.g. "2.10.4"
	Cluster     string        // Cluster name (empty if standalone)
	Host        string        // Server address (IP:port)
	RTT         time.Duration // Last measured round-trip time
	TLS         bool          // Whether the connection uses TLS
	MaxPayload  int64         // Maximum message payload size in bytes
	ClientID    uint64        // Client ID assigned by this server
	Servers     []string      // All known server URLs (configured + discovered)
	Domain      string        // JetStream domain (empty if default)
	Reconnects  uint64        // Cumulative reconnect count
}

// Provider abstracts all NATS JetStream operations for the TUI.
type Provider interface {
	// Connection
	Close()
	IsConnected() bool
	ConnectionStats() ConnectionStats
	ServerInfo() ServerInfo
	RTT() (time.Duration, error)
	ServerURL() string
	Reconnect(ctx context.Context, cfg config.ConnectionConfig) error

	// Account
	AccountInfo(ctx context.Context) (*jetstream.AccountInfo, error)

	// Streams
	ListStreams(ctx context.Context) ([]*jetstream.StreamInfo, error)
	ListStreamsIter(ctx context.Context, fn func(info *jetstream.StreamInfo)) error
	GetStream(ctx context.Context, name string) (jetstream.Stream, error)
	GetStreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error)
	CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	DeleteStream(ctx context.Context, name string) error
	StreamNameBySubject(ctx context.Context, subject string) (string, error)

	// Stream operations
	StreamSubjects(ctx context.Context, streamName string) (map[string]uint64, error)
	PurgeStream(ctx context.Context, name string) error
	PurgeStreamSubject(ctx context.Context, name, subject string) error
	GetMessage(ctx context.Context, streamName string, seq uint64) (*RawMessage, error)
	GetLastMessageForSubject(ctx context.Context, streamName, subject string) (*RawMessage, error)
	DeleteMessage(ctx context.Context, streamName string, seq uint64) error

	// Consumers
	ListConsumers(ctx context.Context, streamName string) ([]*jetstream.ConsumerInfo, error)
	GetConsumer(ctx context.Context, streamName, consumerName string) (jetstream.Consumer, error)
	GetConsumerInfo(ctx context.Context, streamName, consumerName string) (*jetstream.ConsumerInfo, error)
	CreateConsumer(ctx context.Context, streamName string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
	UpdateConsumer(ctx context.Context, streamName string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
	DeleteConsumer(ctx context.Context, streamName, consumerName string) error

	// Key-Value
	ListKeyValueStores(ctx context.Context) ([]jetstream.KeyValueStatus, error)
	GetKeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error)
	ListKeyValueKeys(ctx context.Context, bucket string) ([]string, error)
	CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error)
	DeleteKeyValue(ctx context.Context, bucket string) error

	// Object Store
	ListObjectStores(ctx context.Context) ([]jetstream.ObjectStoreStatus, error)
	GetObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error)
	CreateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error)
	DeleteObjectStore(ctx context.Context, bucket string) error

	// Key-Value Watch
	WatchKeyValue(ctx context.Context, bucket string, handler func(KVWatchEvent)) (KVWatcher, error)

	// Request/Reply
	Request(ctx context.Context, subject string, data []byte, headers map[string][]string, timeout time.Duration) (*RequestResponse, error)

	// Advisories
	SubscribeAdvisories(ctx context.Context, handler func(Advisory)) error

	// Publishing
	Publish(ctx context.Context, subject string, data []byte, headers map[string][]string) error

	// Message subscription
	Subscribe(ctx context.Context, subject string, handler func(LiveMessage)) (Subscription, error)

	// JetStream subscription with replay capability
	SubscribeJetStream(ctx context.Context, subject string, policy DeliverPolicy, handler func(LiveMessage)) (Subscription, error)
}

// ConnectionStats holds basic connection statistics.
type ConnectionStats struct {
	InMsgs     uint64
	OutMsgs    uint64
	InBytes    uint64
	OutBytes   uint64
	Reconnects uint64
}

// Advisory represents a JetStream advisory event.
type Advisory struct {
	Type      string
	Stream    string
	Consumer  string
	Message   string
	Timestamp time.Time
}

// LiveMessage represents a message received from a subscription.
type LiveMessage struct {
	Subject   string
	Data      []byte
	Headers   map[string][]string
	Timestamp time.Time
	Sequence  uint64 // JetStream sequence (0 for core NATS)
	Stream    string // JetStream stream name (empty for core NATS)
}

// DeliverPolicy determines where to start delivering messages.
type DeliverPolicy int

const (
	DeliverAll        DeliverPolicy = iota // Deliver all available messages
	DeliverLast                            // Deliver starting with the last message
	DeliverNew                             // Deliver only new messages (after subscription)
	DeliverLastPerSubject                  // Deliver last message for each subject
)

// KVWatchEvent represents a key-value change event.
type KVWatchEvent struct {
	Key       string
	Operation string // "PUT", "DELETE", "PURGE"
	Value     []byte
	Revision  uint64
	Timestamp time.Time
}

// KVWatcher represents an active KV watch.
type KVWatcher interface {
	Stop() error
}

// RequestResponse holds a NATS request/reply response.
type RequestResponse struct {
	Subject string
	Data    []byte
	Headers map[string][]string
}

// Subscription represents an active message subscription.
type Subscription interface {
	Unsubscribe() error
}
