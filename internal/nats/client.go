package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/galaxy-io/gnat/internal/config"
)

// Client implements Provider using the nats.go SDK.
type Client struct {
	mu     sync.RWMutex
	nc     *natsclient.Conn
	js     jetstream.JetStream
	advSub *natsclient.Subscription

	// jsMu guards the JetStream-availability probe cache. It is separate
	// from mu so probing never blocks (or is blocked by) a Reconnect swap.
	jsMu      sync.Mutex
	jsProbed  bool
	jsEnabled bool
}

// conn returns the current NATS connection under a read lock, so callers
// always observe a fully-swapped connection rather than racing with
// Reconnect mid-swap (which can deadlock on the SDK's internal mutexes).
func (c *Client) conn() *natsclient.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nc
}

// jetStream returns the current JetStream context under a read lock.
func (c *Client) jetStream() jetstream.JetStream {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.js
}

// Connect establishes a NATS connection and creates a JetStream context.
func Connect(ctx context.Context, cfg config.ConnectionConfig) (*Client, error) {
	expanded := cfg.ExpandEnv()

	opts := []natsclient.Option{
		natsclient.Name("gnat-tui"),
		natsclient.ReconnectWait(2 * time.Second),
		natsclient.MaxReconnects(60),
	}

	if expanded.Credentials != "" {
		opts = append(opts, natsclient.UserCredentials(expanded.Credentials))
	}
	if expanded.Token != "" {
		opts = append(opts, natsclient.Token(expanded.Token))
	}
	if expanded.User != "" && expanded.Password != "" {
		opts = append(opts, natsclient.UserInfo(expanded.User, expanded.Password))
	}
	if expanded.NKey != "" {
		opt, err := natsclient.NkeyOptionFromSeed(expanded.NKey)
		if err != nil {
			return nil, fmt.Errorf("loading nkey: %w", err)
		}
		opts = append(opts, opt)
	}
	if expanded.TLS.CA != "" || expanded.TLS.Cert != "" {
		if expanded.TLS.CA != "" {
			opts = append(opts, natsclient.RootCAs(expanded.TLS.CA))
		}
		if expanded.TLS.Cert != "" && expanded.TLS.Key != "" {
			opts = append(opts, natsclient.ClientCert(expanded.TLS.Cert, expanded.TLS.Key))
		}
	}

	nc, err := natsclient.Connect(expanded.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}

	var js jetstream.JetStream
	if expanded.Domain != "" {
		js, err = jetstream.NewWithDomain(nc, expanded.Domain)
	} else {
		js, err = jetstream.New(nc)
	}
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}

	return &Client{nc: nc, js: js}, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.advSub != nil {
		_ = c.advSub.Unsubscribe()
		c.advSub = nil
	}
	c.nc.Close()
}

func (c *Client) IsConnected() bool {
	return c.conn().IsConnected()
}

func (c *Client) ConnectionStats() ConnectionStats {
	s := c.conn().Stats()
	return ConnectionStats{
		InMsgs:     s.InMsgs,
		OutMsgs:    s.OutMsgs,
		InBytes:    s.InBytes,
		OutBytes:   s.OutBytes,
		Reconnects: s.Reconnects,
	}
}

func (c *Client) RTT() (time.Duration, error) {
	return c.conn().RTT()
}

func (c *Client) ServerURL() string {
	return c.conn().ConnectedUrl()
}

func (c *Client) ServerInfo() ServerInfo {
	nc := c.conn()
	rtt, _ := nc.RTT()
	stats := nc.Stats()
	cid, _ := nc.GetClientID()

	_, tlsErr := nc.TLSConnectionState()
	isTLS := tlsErr == nil

	return ServerInfo{
		Name:       nc.ConnectedServerName(),
		ID:         nc.ConnectedServerId(),
		Version:    nc.ConnectedServerVersion(),
		Cluster:    nc.ConnectedClusterName(),
		Host:       nc.ConnectedAddr(),
		RTT:        rtt,
		TLS:        isTLS,
		MaxPayload: nc.MaxPayload(),
		ClientID:   cid,
		Servers:    nc.Servers(),
		Reconnects: stats.Reconnects,
	}
}

// JetStream availability

func (c *Client) JetStreamEnabled(ctx context.Context) bool {
	c.jsMu.Lock()
	defer c.jsMu.Unlock()
	if c.jsProbed {
		return c.jsEnabled
	}

	js := c.jetStream()
	if js == nil {
		return false
	}

	// Bound the probe so a hung/unreachable server can't block every
	// caller indefinitely (callers commonly pass context.Background()).
	probeCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		probeCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	_, err := js.AccountInfo(probeCtx)
	c.jsEnabled = err == nil
	c.jsProbed = true
	return c.jsEnabled
}

// Account

func (c *Client) AccountInfo(ctx context.Context) (*jetstream.AccountInfo, error) {
	return c.jetStream().AccountInfo(ctx)
}

// Streams

func (c *Client) ListStreams(ctx context.Context) ([]*jetstream.StreamInfo, error) {
	var streams []*jetstream.StreamInfo
	lister := c.jetStream().ListStreams(ctx)
	for info := range lister.Info() {
		streams = append(streams, info)
	}
	if err := lister.Err(); err != nil {
		return streams, err
	}
	return streams, nil
}

func (c *Client) ListStreamsIter(ctx context.Context, fn func(info *jetstream.StreamInfo)) error {
	lister := c.jetStream().ListStreams(ctx)
	for info := range lister.Info() {
		fn(info)
	}
	return lister.Err()
}

func (c *Client) GetStream(ctx context.Context, name string) (jetstream.Stream, error) {
	return c.jetStream().Stream(ctx, name)
}

func (c *Client) GetStreamInfo(ctx context.Context, name string) (*jetstream.StreamInfo, error) {
	stream, err := c.jetStream().Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	return stream.Info(ctx)
}

func (c *Client) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	return c.jetStream().CreateStream(ctx, cfg)
}

func (c *Client) UpdateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	return c.jetStream().UpdateStream(ctx, cfg)
}

func (c *Client) DeleteStream(ctx context.Context, name string) error {
	return c.jetStream().DeleteStream(ctx, name)
}

func (c *Client) StreamNameBySubject(ctx context.Context, subject string) (string, error) {
	return c.jetStream().StreamNameBySubject(ctx, subject)
}

// Stream operations

func (c *Client) StreamSubjects(ctx context.Context, streamName string) (map[string]uint64, error) {
	return c.streamSubjectsPaginated(ctx, streamName)
}

// streamSubjectsPaginated fetches the subject→count map using the raw
// JetStream API with offset-based pagination so that large subject sets
// are retrieved in bounded pages rather than a single unbounded response.
func (c *Client) streamSubjectsPaginated(ctx context.Context, streamName string) (map[string]uint64, error) {
	apiSubject := fmt.Sprintf("$JS.API.STREAM.INFO.%s", streamName)
	all := make(map[string]uint64)
	offset := 0

	for {
		if ctx.Err() != nil {
			return all, ctx.Err()
		}

		req := fmt.Sprintf(`{"subjects_filter":">","offset":%d}`, offset)
		msg, err := c.conn().RequestWithContext(ctx, apiSubject, []byte(req))
		if err != nil {
			if len(all) > 0 {
				return all, nil // return what we have
			}
			return nil, err
		}

		var resp struct {
			Total  int `json:"total"`
			Offset int `json:"offset"`
			Limit  int `json:"limit"`
			State  struct {
				Subjects map[string]uint64 `json:"subjects"`
			} `json:"state"`
			Error *struct {
				Description string `json:"description"`
			} `json:"error"`
		}
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			if len(all) > 0 {
				return all, nil
			}
			return nil, err
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("stream %s subjects: %s", streamName, resp.Error.Description)
		}

		for subj, count := range resp.State.Subjects {
			all[subj] = count
		}

		// No subjects in response or we've collected them all — done.
		if len(resp.State.Subjects) == 0 || len(all) >= resp.Total {
			break
		}
		offset = len(all)
	}

	return all, nil
}

func (c *Client) PurgeStream(ctx context.Context, name string) error {
	stream, err := c.jetStream().Stream(ctx, name)
	if err != nil {
		return err
	}
	return stream.Purge(ctx)
}

func (c *Client) PurgeStreamSubject(ctx context.Context, name, subject string) error {
	stream, err := c.jetStream().Stream(ctx, name)
	if err != nil {
		return err
	}
	return stream.Purge(ctx, jetstream.WithPurgeSubject(subject))
}

func (c *Client) GetMessage(ctx context.Context, streamName string, seq uint64) (*RawMessage, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	msg, err := stream.GetMsg(ctx, seq)
	if err != nil {
		return nil, err
	}
	return convertRawMsg(msg), nil
}

func (c *Client) GetLastMessageForSubject(ctx context.Context, streamName, subject string) (*RawMessage, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	msg, err := stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return nil, err
	}
	return convertRawMsg(msg), nil
}

func convertRawMsg(msg *jetstream.RawStreamMsg) *RawMessage {
	headers := make(map[string][]string)
	for k, v := range msg.Header {
		headers[k] = v
	}
	return &RawMessage{
		Subject:  msg.Subject,
		Sequence: msg.Sequence,
		Data:     msg.Data,
		Headers:  headers,
		Time:     msg.Time,
	}
}

func (c *Client) GetRecentMessagesForSubject(ctx context.Context, streamName, subject string, maxBytes, maxMsgs int) ([]*RawMessage, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}

	// Create an ephemeral consumer filtered to the subject, delivering the last N messages.
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject:     subject,
		DeliverPolicy:     jetstream.DeliverLastPerSubjectPolicy,
		AckPolicy:         jetstream.AckNonePolicy,
		InactiveThreshold: 30 * time.Second,
	})
	if err != nil {
		// Fall back to DeliverAll if LastPerSubject isn't applicable (single subject)
		cons, err = stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			FilterSubject:     subject,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			AckPolicy:         jetstream.AckNonePolicy,
			InactiveThreshold: 30 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("creating consumer: %w", err)
		}
	}

	consName := ""
	if info, err := cons.Info(ctx); err == nil && info != nil {
		consName = info.Name
	}
	defer func() {
		if consName != "" {
			dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = stream.DeleteConsumer(dctx, consName)
		}
	}()

	fetchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	batch, err := cons.FetchBytes(maxBytes, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		return nil, fmt.Errorf("fetching messages: %w", err)
	}

	var msgs []*RawMessage
	for msg := range batch.Messages() {
		if fetchCtx.Err() != nil {
			break
		}
		meta, _ := msg.Metadata()
		headers := make(map[string][]string)
		for k, v := range msg.Headers() {
			headers[k] = v
		}
		rm := &RawMessage{
			Subject: msg.Subject(),
			Data:    msg.Data(),
			Headers: headers,
		}
		if meta != nil {
			rm.Sequence = meta.Sequence.Stream
			rm.Time = meta.Timestamp
		}
		msgs = append(msgs, rm)
		if maxMsgs > 0 && len(msgs) >= maxMsgs {
			break
		}
	}

	return msgs, nil
}

func (c *Client) DeleteMessage(ctx context.Context, streamName string, seq uint64) error {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return err
	}
	return stream.DeleteMsg(ctx, seq)
}

// Consumers

func (c *Client) ListConsumers(ctx context.Context, streamName string) ([]*jetstream.ConsumerInfo, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	var consumers []*jetstream.ConsumerInfo
	lister := stream.ListConsumers(ctx)
	for info := range lister.Info() {
		consumers = append(consumers, info)
	}
	if err := lister.Err(); err != nil {
		return consumers, err
	}
	return consumers, nil
}

func (c *Client) GetConsumer(ctx context.Context, streamName, consumerName string) (jetstream.Consumer, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	return stream.Consumer(ctx, consumerName)
}

func (c *Client) GetConsumerInfo(ctx context.Context, streamName, consumerName string) (*jetstream.ConsumerInfo, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	consumer, err := stream.Consumer(ctx, consumerName)
	if err != nil {
		return nil, err
	}
	return consumer.Info(ctx)
}

func (c *Client) CreateConsumer(ctx context.Context, streamName string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	return stream.CreateConsumer(ctx, cfg)
}

func (c *Client) UpdateConsumer(ctx context.Context, streamName string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, err
	}
	return stream.UpdateConsumer(ctx, cfg)
}

func (c *Client) DeleteConsumer(ctx context.Context, streamName, consumerName string) error {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return err
	}
	return stream.DeleteConsumer(ctx, consumerName)
}

// Key-Value

func (c *Client) ListKeyValueStores(ctx context.Context) ([]jetstream.KeyValueStatus, error) {
	var stores []jetstream.KeyValueStatus
	lister := c.jetStream().KeyValueStores(ctx)
	for status := range lister.Status() {
		stores = append(stores, status)
	}
	if err := lister.Error(); err != nil {
		return stores, err
	}
	return stores, nil
}

func (c *Client) GetKeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	return c.jetStream().KeyValue(ctx, bucket)
}

func (c *Client) ListKeyValueKeys(ctx context.Context, bucket string) ([]string, error) {
	kv, err := c.jetStream().KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}

	// Keys()/ListKeys() use an internal watcher that can race on
	// InitialConsumerPending, returning ErrNoKeysFound for non-empty
	// buckets.  We check the bucket status and retry when this happens.
	status, err := kv.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting bucket status: %w", err)
	}
	expectKeys := status.Values() > 0

	const maxAttempts = 3
	for attempt := range maxAttempts {
		keys, err := kv.Keys(ctx)
		if err != nil {
			if errors.Is(err, jetstream.ErrNoKeysFound) {
				if expectKeys && attempt < maxAttempts-1 {
					time.Sleep(50 * time.Millisecond)
					continue
				}
				return nil, nil // genuinely empty
			}
			return nil, err
		}
		return keys, nil
	}
	return nil, nil
}

func (c *Client) CreateKeyValue(ctx context.Context, cfg jetstream.KeyValueConfig) (jetstream.KeyValue, error) {
	return c.jetStream().CreateKeyValue(ctx, cfg)
}

func (c *Client) DeleteKeyValue(ctx context.Context, bucket string) error {
	return c.jetStream().DeleteKeyValue(ctx, bucket)
}

// Object Store

func (c *Client) ListObjectStores(ctx context.Context) ([]jetstream.ObjectStoreStatus, error) {
	var stores []jetstream.ObjectStoreStatus
	lister := c.jetStream().ObjectStores(ctx)
	for status := range lister.Status() {
		stores = append(stores, status)
	}
	if err := lister.Error(); err != nil {
		return stores, err
	}
	return stores, nil
}

func (c *Client) GetObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	return c.jetStream().ObjectStore(ctx, bucket)
}

func (c *Client) CreateObjectStore(ctx context.Context, cfg jetstream.ObjectStoreConfig) (jetstream.ObjectStore, error) {
	return c.jetStream().CreateObjectStore(ctx, cfg)
}

func (c *Client) DeleteObjectStore(ctx context.Context, bucket string) error {
	return c.jetStream().DeleteObjectStore(ctx, bucket)
}

// Advisories

func (c *Client) SubscribeAdvisories(ctx context.Context, handler func(Advisory)) error {
	c.mu.Lock()
	if c.advSub != nil {
		_ = c.advSub.Unsubscribe()
		c.advSub = nil
	}
	c.mu.Unlock()

	sub, err := c.conn().Subscribe("$JS.EVENT.ADVISORY.>", func(msg *natsclient.Msg) {
		adv := parseAdvisory(msg)
		handler(adv)
	})
	if err != nil {
		return err
	}
	// Cap the pending buffer so a high-volume advisory stream cannot
	// grow unboundedly 
	_ = sub.SetPendingLimits(1024, 8*1024*1024) // 1024 msgs / 8 MB
	c.mu.Lock()
	c.advSub = sub
	c.mu.Unlock()
	return nil
}

func (c *Client) UnsubscribeAdvisories() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.advSub != nil {
		_ = c.advSub.Unsubscribe()
		c.advSub = nil
	}
}

func parseAdvisory(msg *natsclient.Msg) Advisory {
	parts := strings.Split(msg.Subject, ".")
	adv := Advisory{
		Timestamp: time.Now(),
		Type:      msg.Subject,
	}

	// Extract meaningful type from subject
	// e.g. $JS.EVENT.ADVISORY.STREAM.CREATED.mystream
	if len(parts) >= 5 {
		adv.Type = strings.Join(parts[3:len(parts)-1], ".")
	}
	if len(parts) >= 6 {
		adv.Stream = parts[len(parts)-1]
	}
	if len(parts) >= 7 {
		adv.Consumer = parts[len(parts)-1]
		adv.Stream = parts[len(parts)-2]
	}

	// Try to extract message from JSON payload
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Data, &payload); err == nil {
		if m, ok := payload["type"].(string); ok {
			adv.Type = m
		}
	}

	return adv
}

// Reconnect connects with new config and atomically swaps in the new
// connection. The (blocking) dial happens WITHOUT holding c.mu so that
// UI-thread calls into other Client methods are never blocked behind an
// in-progress reconnect; the lock is only held for the pointer swap.
func (c *Client) Reconnect(ctx context.Context, cfg config.ConnectionConfig) error {
	// Build new connection
	expanded := cfg.ExpandEnv()

	opts := []natsclient.Option{
		natsclient.Name("gnat-tui"),
		natsclient.ReconnectWait(2 * time.Second),
		natsclient.MaxReconnects(60),
	}

	if expanded.Credentials != "" {
		opts = append(opts, natsclient.UserCredentials(expanded.Credentials))
	}
	if expanded.Token != "" {
		opts = append(opts, natsclient.Token(expanded.Token))
	}
	if expanded.User != "" && expanded.Password != "" {
		opts = append(opts, natsclient.UserInfo(expanded.User, expanded.Password))
	}
	if expanded.NKey != "" {
		opt, err := natsclient.NkeyOptionFromSeed(expanded.NKey)
		if err != nil {
			return fmt.Errorf("loading nkey: %w", err)
		}
		opts = append(opts, opt)
	}
	if expanded.TLS.CA != "" || expanded.TLS.Cert != "" {
		if expanded.TLS.CA != "" {
			opts = append(opts, natsclient.RootCAs(expanded.TLS.CA))
		}
		if expanded.TLS.Cert != "" && expanded.TLS.Key != "" {
			opts = append(opts, natsclient.ClientCert(expanded.TLS.Cert, expanded.TLS.Key))
		}
	}

	nc, err := natsclient.Connect(expanded.URL, opts...)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}

	var js jetstream.JetStream
	if expanded.Domain != "" {
		js, err = jetstream.NewWithDomain(nc, expanded.Domain)
	} else {
		js, err = jetstream.New(nc)
	}
	if err != nil {
		nc.Close()
		return fmt.Errorf("creating JetStream context: %w", err)
	}

	// Swap in the new connection under the write lock, tearing down the old
	// one. Readers using conn()/jetStream() see either the fully-old or the
	// fully-new connection, never a torn state.
	c.mu.Lock()
	oldNC := c.nc
	oldSub := c.advSub
	c.advSub = nil
	c.nc = nc
	c.js = js
	c.mu.Unlock()

	// Reset the JetStream-availability probe so it re-probes the new server.
	c.jsMu.Lock()
	c.jsProbed = false
	c.jsEnabled = false
	c.jsMu.Unlock()

	// Close the old connection outside the lock — Unsubscribe/Close can block
	// on the SDK's internal goroutines, which we must not do while holding mu.
	if oldSub != nil {
		_ = oldSub.Unsubscribe()
	}
	if oldNC != nil {
		oldNC.Close()
	}
	return nil
}

// WatchKeyValue watches all keys in a KV bucket for changes.
func (c *Client) WatchKeyValue(ctx context.Context, bucket string, handler func(KVWatchEvent)) (KVWatcher, error) {
	kv, err := c.jetStream().KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}

	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		return nil, err
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Prevent silent goroutine death from handler panics
			}
		}()
		updates := watcher.Updates()
		for {
			select {
			case <-watchCtx.Done():
				return
			case entry, ok := <-updates:
				if !ok {
					return
				}
				if entry == nil {
					continue
				}
				op := "PUT"
				switch entry.Operation() {
				case jetstream.KeyValueDelete:
					op = "DELETE"
				case jetstream.KeyValuePurge:
					op = "PURGE"
				}
				handler(KVWatchEvent{
					Key:       entry.Key(),
					Operation: op,
					ValueSize: len(entry.Value()),
					Revision:  entry.Revision(),
					Timestamp: entry.Created(),
				})
			}
		}
	}()

	return &kvWatcherImpl{watcher: watcher, cancel: watchCancel}, nil
}

type kvWatcherImpl struct {
	watcher jetstream.KeyWatcher
	cancel  context.CancelFunc
}

func (w *kvWatcherImpl) Stop() error {
	w.cancel()
	return w.watcher.Stop()
}

// Request sends a request and waits for a reply.
func (c *Client) Request(ctx context.Context, subject string, data []byte, headers map[string][]string, timeout time.Duration) (*RequestResponse, error) {
	msg := natsclient.NewMsg(subject)
	msg.Data = data
	for k, vals := range headers {
		for _, v := range vals {
			msg.Header.Add(k, v)
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := c.conn().RequestMsgWithContext(reqCtx, msg)
	if err != nil {
		return nil, err
	}

	respHeaders := make(map[string][]string)
	for k, v := range resp.Header {
		respHeaders[k] = v
	}

	return &RequestResponse{
		Subject: resp.Subject,
		Data:    resp.Data,
		Headers: respHeaders,
	}, nil
}

// Publish sends a message to the given subject with optional headers.
func (c *Client) Publish(_ context.Context, subject string, data []byte, headers map[string][]string) error {
	msg := natsclient.NewMsg(subject)
	msg.Data = data
	for k, vals := range headers {
		for _, v := range vals {
			msg.Header.Add(k, v)
		}
	}
	return c.conn().PublishMsg(msg)
}

// Subscribe creates a subscription to the given subject pattern.
func (c *Client) Subscribe(ctx context.Context, subject string, handler func(LiveMessage)) (Subscription, error) {
	sub, err := c.conn().Subscribe(subject, func(msg *natsclient.Msg) {
		headers := make(map[string][]string)
		for k, v := range msg.Header {
			headers[k] = v
		}
		handler(LiveMessage{
			Subject:   msg.Subject,
			Data:      msg.Data,
			Headers:   headers,
			Timestamp: time.Now(),
		})
	})
	if err != nil {
		return nil, err
	}
	return &clientSubscription{sub: sub}, nil
}

type clientSubscription struct {
	sub *natsclient.Subscription
}

func (s *clientSubscription) Unsubscribe() error {
	return s.sub.Unsubscribe()
}

// SubscribeJetStream creates a JetStream subscription with replay capability.
// It automatically finds the stream for the given subject and creates an ephemeral consumer.
func (c *Client) SubscribeJetStream(ctx context.Context, subject string, policy DeliverPolicy, handler func(LiveMessage)) (Subscription, error) {
	// Find the stream that handles this subject
	streamName, err := c.jetStream().StreamNameBySubject(ctx, subject)
	if err != nil {
		return nil, fmt.Errorf("no stream found for subject %q: %w", subject, err)
	}
	return c.subscribeJetStream(ctx, streamName, subject, policy, handler)
}

// SubscribeJetStreamStream creates a JetStream subscription over an entire
// stream. The ephemeral consumer carries no subject filter, so every message
// in the stream is delivered regardless of which configured subject it matched.
func (c *Client) SubscribeJetStreamStream(ctx context.Context, streamName string, policy DeliverPolicy, handler func(LiveMessage)) (Subscription, error) {
	return c.subscribeJetStream(ctx, streamName, "", policy, handler)
}

// subscribeJetStream creates an ephemeral consumer on streamName. An empty
// filterSubject watches the whole stream; a non-empty one narrows to that subject.
func (c *Client) subscribeJetStream(ctx context.Context, streamName, filterSubject string, policy DeliverPolicy, handler func(LiveMessage)) (Subscription, error) {
	stream, err := c.jetStream().Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("getting stream %s: %w", streamName, err)
	}

	// Convert our policy to JetStream policy
	var jsPolicy jetstream.DeliverPolicy
	switch policy {
	case DeliverAll:
		jsPolicy = jetstream.DeliverAllPolicy
	case DeliverLast:
		jsPolicy = jetstream.DeliverLastPolicy
	case DeliverNew:
		jsPolicy = jetstream.DeliverNewPolicy
	case DeliverLastPerSubject:
		jsPolicy = jetstream.DeliverLastPerSubjectPolicy
	default:
		jsPolicy = jetstream.DeliverAllPolicy
	}

	// Create ephemeral consumer. An empty FilterSubject delivers all subjects.
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject:     filterSubject,
		DeliverPolicy:     jsPolicy,
		AckPolicy:         jetstream.AckNonePolicy, // No acks needed for monitoring
		InactiveThreshold: 5 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("creating consumer: %w", err)
	}

	// Start consuming
	consCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		meta, _ := msg.Metadata()
		var seq uint64
		var ts time.Time
		if meta != nil {
			seq = meta.Sequence.Stream
			ts = meta.Timestamp
		} else {
			ts = time.Now()
		}

		headers := make(map[string][]string)
		for k, v := range msg.Headers() {
			headers[k] = v
		}

		handler(LiveMessage{
			Subject:   msg.Subject(),
			Data:      msg.Data(),
			Headers:   headers,
			Timestamp: ts,
			Sequence:  seq,
			Stream:    streamName,
		})
	})
	if err != nil {
		return nil, fmt.Errorf("starting consume: %w", err)
	}

	return &jsSubscription{
		ctx:      consCtx,
		consumer: consumer,
		stream:   stream,
	}, nil
}

type jsSubscription struct {
	ctx      jetstream.ConsumeContext
	consumer jetstream.Consumer
	stream   jetstream.Stream
}

func (s *jsSubscription) Unsubscribe() error {
	s.ctx.Stop()
	// Delete the ephemeral consumer with a bounded timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := s.consumer.Info(ctx)
	if err == nil && info != nil {
		_ = s.stream.DeleteConsumer(ctx, info.Name)
	}
	return nil
}

// Verify interface compliance.
var _ Provider = (*Client)(nil)
