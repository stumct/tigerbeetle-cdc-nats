package cdcnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

type progressRecord struct {
	Timestamp uint64 `json:"timestamp"`
	Version   string `json:"version"`
}

type pendingPublish struct {
	future    nats.PubAckFuture
	timestamp uint64
	subject   string
}

// changeEventSource is the part of the TigerBeetle client that the runner uses. Tests substitute a
// fake.
type changeEventSource interface {
	GetChangeEvents(filter types.ChangeEventsFilter) ([]types.ChangeEvent, error)
	Close()
}

// openTigerBeetle connects to the configured TigerBeetle cluster.
func openTigerBeetle(cfg config) (changeEventSource, error) {
	return tigerbeetle_go.NewClient(cfg.clusterID, cfg.addresses)
}

// run provisions JetStream resources, takes the single-writer lock (waiting while another instance
// holds it), then publishes change events until ctx is cancelled. A requested shutdown returns nil;
// losing the lock returns the reason.
func run(ctx context.Context, cfg config, openSource func(config) (changeEventSource, error)) error {
	log.Printf(
		"starting CDC cluster=%s nats=%s stream=%s publish_mode=%s",
		cfg.clusterIDDecimal,
		redactURLs(cfg.natsURL),
		cfg.eventStream,
		cfg.publishMode,
	)

	nc, err := connectNATS(cfg)
	if err != nil {
		return err
	}
	defer nc.Close()

	jsOptions := make([]nats.JSOpt, 0, 2)
	if cfg.publishMode == publishModeAsync {
		jsOptions = append(
			jsOptions,
			nats.PublishAsyncMaxPending(cfg.publishAsyncMaxPending),
			nats.PublishAsyncErrHandler(func(_ nats.JetStream, msg *nats.Msg, err error) {
				if msg == nil {
					log.Printf("warning: async publish failed: %v", err)
					return
				}
				log.Printf("warning: async publish failed subject=%q: %v", msg.Subject, err)
			}),
		)
	}

	js, err := nc.JetStream(jsOptions...)
	if err != nil {
		return fmt.Errorf("create JetStream context: %w", err)
	}

	if err := ensureEventStream(js, cfg, false); err != nil {
		return err
	}

	progressKV, err := ensureKV(js, desiredProgressKVConfig(cfg), cfg.provision)
	if err != nil {
		return err
	}

	lockKV, err := ensureKV(js, desiredLockKVConfig(cfg), cfg.provision)
	if err != nil {
		return err
	}

	lock, err := acquireLock(ctx, lockKV, cfg.lockKey(), cfg.version, cfg.lockRefresh)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	// runCtx stops replication on shutdown, or when the lock is lost (with the loss as its cause).
	runCtx, stopRun := context.WithCancelCause(ctx)
	keepAliveDone := make(chan struct{})
	go func() {
		defer close(keepAliveDone)
		lock.keepAlive(runCtx, cfg.lockRefresh, cfg.lockTTL, stopRun)
	}()
	defer func() {
		stopRun(context.Canceled)
		<-keepAliveDone
		if err := lock.release(); err != nil {
			log.Printf("warning: %v", err)
		}
	}()

	if cfg.streamUpdate {
		if err := ensureEventStream(js, cfg, true); err != nil {
			return err
		}
	}

	err = replicate(runCtx, js, progressKV, cfg, openSource)
	// The first cancellation wins: report a lost lock even if a shutdown was requested afterwards.
	if cause := context.Cause(runCtx); errors.Is(cause, errLockLost) {
		return cause
	}
	if ctx.Err() != nil {
		return nil
	}
	return err
}

// replicate recovers progress, then repeatedly fetches change events from TigerBeetle and publishes
// them until ctx is done or an error occurs.
func replicate(
	ctx context.Context,
	js nats.JetStreamContext,
	progressKV nats.KeyValue,
	cfg config,
	openSource func(config) (changeEventSource, error),
) error {
	lastTimestamp, err := recoverProgress(cfg, progressKV)
	if err != nil {
		return err
	}

	source, err := openSource(cfg)
	if err != nil {
		return fmt.Errorf("create TigerBeetle client: %w", err)
	}

	// The TigerBeetle client retries a request forever while the cluster is unreachable, and closing
	// the client is the only way to interrupt it. Close it as soon as ctx is done.
	closeSource := sync.OnceFunc(source.Close)
	defer closeSource()
	stopCloseOnDone := context.AfterFunc(ctx, closeSource)
	defer stopCloseOnDone()

	rateLimiter := newRequestRateLimiter(cfg.requestsPerSecondLimit)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := rateLimiter.wait(ctx); err != nil {
			return fmt.Errorf("wait for rate limiter: %w", err)
		}

		nextTimestamp, err := nextQueryTimestamp(lastTimestamp)
		if err != nil {
			return err
		}

		events, err := source.GetChangeEvents(types.ChangeEventsFilter{
			TimestampMin: nextTimestamp,
			TimestampMax: 0,
			Limit:        cfg.eventCountMax,
		})
		if err != nil {
			return fmt.Errorf("get_change_events(timestamp_min=%d): %w", nextTimestamp, err)
		}

		if len(events) == 0 {
			if err := sleepContext(ctx, cfg.idleInterval); err != nil {
				return err
			}
			continue
		}

		if err := publishEventsAndCheckpoint(ctx, js, progressKV, cfg, events, &lastTimestamp); err != nil {
			return err
		}
	}
}

func recoverProgress(cfg config, kv nats.KeyValue) (uint64, error) {
	if cfg.timestampLast != nil {
		log.Printf("using timestamp override --timestamp-last=%d", *cfg.timestampLast)
		return *cfg.timestampLast, nil
	}

	entry, err := kv.Get(cfg.progressKey())
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			log.Printf("no prior progress found for %q, starting from beginning", cfg.progressKey())
			return 0, nil
		}
		return 0, fmt.Errorf("read progress from %q: %w", cfg.progressKey(), err)
	}

	var progress progressRecord
	if err := json.Unmarshal(entry.Value(), &progress); err != nil {
		return 0, fmt.Errorf("invalid progress payload in %q: %w", cfg.progressKey(), err)
	}

	log.Printf("recovered progress timestamp=%d version=%q", progress.Timestamp, progress.Version)
	return progress.Timestamp, nil
}

func writeProgress(kv nats.KeyValue, key string, progress progressRecord) error {
	payload, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("marshal progress: %w", err)
	}

	if _, err := kv.Put(key, payload); err != nil {
		return fmt.Errorf("write progress key %q: %w", key, err)
	}

	return nil
}

func publishEventsAndCheckpoint(
	ctx context.Context,
	js nats.JetStreamContext,
	progressKV nats.KeyValue,
	cfg config,
	events []types.ChangeEvent,
	lastTimestamp *uint64,
) error {
	if len(events) == 0 {
		return nil
	}

	chunkSize := len(events)
	if cfg.progressEveryEvents > 0 && int(cfg.progressEveryEvents) < chunkSize {
		chunkSize = int(cfg.progressEveryEvents)
	}

	for start := 0; start < len(events); start += chunkSize {
		end := start + chunkSize
		if end > len(events) {
			end = len(events)
		}

		chunk := events[start:end]
		if err := publishEventChunk(ctx, js, cfg, chunk); err != nil {
			return err
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		chunkLastTimestamp := chunk[len(chunk)-1].Timestamp
		if err := writeProgress(progressKV, cfg.progressKey(), progressRecord{
			Timestamp: chunkLastTimestamp,
			Version:   cfg.version,
		}); err != nil {
			return err
		}

		*lastTimestamp = chunkLastTimestamp
	}

	log.Printf("published events=%d last_timestamp=%d", len(events), *lastTimestamp)
	return nil
}

func publishEventChunk(
	ctx context.Context,
	js nats.JetStreamContext,
	cfg config,
	events []types.ChangeEvent,
) error {
	switch cfg.publishMode {
	case publishModeSync:
		return publishEventChunkSync(ctx, js, cfg, events)
	case publishModeAsync:
		return publishEventChunkAsync(ctx, js, cfg, events)
	default:
		return fmt.Errorf("unsupported publish mode %q", cfg.publishMode)
	}
}

func publishEventChunkSync(
	ctx context.Context,
	js nats.JetStreamContext,
	cfg config,
	events []types.ChangeEvent,
) error {
	for _, event := range events {
		msg, err := buildEventMessage(cfg, event)
		if err != nil {
			return err
		}

		publishCtx, cancel := context.WithTimeout(ctx, cfg.publishAckTimeout)
		ack, err := js.PublishMsg(msg, nats.Context(publishCtx))
		cancel()
		if err != nil {
			return fmt.Errorf("publish event timestamp=%d subject=%q: %w", event.Timestamp, msg.Subject, err)
		}
		if ack != nil && ack.Duplicate {
			log.Printf("duplicate publish acknowledged for timestamp=%d subject=%q", event.Timestamp, msg.Subject)
		}
	}

	return nil
}

func publishEventChunkAsync(
	ctx context.Context,
	js nats.JetStreamContext,
	cfg config,
	events []types.ChangeEvent,
) error {
	pending := make([]pendingPublish, 0, len(events))

	for _, event := range events {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		msg, err := buildEventMessage(cfg, event)
		if err != nil {
			return err
		}

		future, err := js.PublishMsgAsync(msg)
		if err != nil {
			return fmt.Errorf("queue async publish timestamp=%d subject=%q: %w", event.Timestamp, msg.Subject, err)
		}

		pending = append(pending, pendingPublish{
			future:    future,
			timestamp: event.Timestamp,
			subject:   msg.Subject,
		})
	}

	duplicateCount := 0
	for _, p := range pending {
		ack, err := waitForPublishAck(ctx, p.future, cfg.publishAckTimeout)
		if err != nil {
			return fmt.Errorf("await async publish ack timestamp=%d subject=%q: %w", p.timestamp, p.subject, err)
		}
		if ack.Duplicate {
			duplicateCount++
		}
	}

	if duplicateCount > 0 {
		log.Printf("async publish completed with duplicates=%d", duplicateCount)
	}

	return nil
}

func waitForPublishAck(
	ctx context.Context,
	future nats.PubAckFuture,
	timeout time.Duration,
) (*nats.PubAck, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-future.Err():
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("async publish failed without error details")
	case ack := <-future.Ok():
		if ack == nil {
			return nil, fmt.Errorf("received nil publish ack")
		}
		return ack, nil
	case <-timer.C:
		return nil, fmt.Errorf("timed out after %s", timeout)
	}
}

func buildEventMessage(cfg config, event types.ChangeEvent) (*nats.Msg, error) {
	body, eventType, err := encodeEventJSON(event)
	if err != nil {
		return nil, fmt.Errorf("encode change event timestamp=%d: %w", event.Timestamp, err)
	}

	subject := cfg.subjectForEvent(event.Ledger, eventType)
	msg := nats.NewMsg(subject)
	msg.Data = body
	msg.Header = nats.Header{}
	msg.Header.Set("Content-Type", "application/json")
	msg.Header.Set("event_type", eventType)
	msg.Header.Set("ledger", strconv.FormatUint(uint64(event.Ledger), 10))
	msg.Header.Set("transfer_code", strconv.FormatUint(uint64(event.TransferCode), 10))
	msg.Header.Set("debit_account_code", strconv.FormatUint(uint64(event.DebitAccountCode), 10))
	msg.Header.Set("credit_account_code", strconv.FormatUint(uint64(event.CreditAccountCode), 10))
	msg.Header.Set(nats.MsgIdHdr, fmt.Sprintf("%s/%d", cfg.clusterIDDecimal, event.Timestamp))

	return msg, nil
}

func nextQueryTimestamp(lastTimestamp uint64) (uint64, error) {
	if lastTimestamp == 0 {
		return 1, nil
	}

	if lastTimestamp >= maxTigerBeetleTimestamp {
		return 0, fmt.Errorf("cannot continue from timestamp %d: it is the largest TigerBeetle timestamp", lastTimestamp)
	}

	return lastTimestamp + 1, nil
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type requestRateLimiter struct {
	limit       uint32
	windowStart time.Time
	count       uint32
}

func newRequestRateLimiter(limit uint32) *requestRateLimiter {
	return &requestRateLimiter{limit: limit}
}

func (r *requestRateLimiter) wait(ctx context.Context) error {
	if r.limit == 0 {
		return nil
	}

	for {
		now := time.Now()
		if r.windowStart.IsZero() || now.Sub(r.windowStart) >= time.Second {
			r.windowStart = now
			r.count = 1
			return nil
		}

		if r.count < r.limit {
			r.count++
			return nil
		}

		waitFor := time.Second - now.Sub(r.windowStart)
		if waitFor <= 0 {
			r.windowStart = now
			r.count = 1
			return nil
		}

		timer := time.NewTimer(waitFor)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
