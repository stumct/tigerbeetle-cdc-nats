package cdcnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

type progressRecord struct {
	Timestamp uint64 `json:"timestamp"`
	Version   string `json:"version"`
}

type lockRecord struct {
	Owner     string `json:"owner"`
	Hostname  string `json:"hostname"`
	PID       int    `json:"pid"`
	Version   string `json:"version"`
	UpdatedAt string `json:"updated_at"`
}

type lockHandle struct {
	kv       nats.KeyValue
	key      string
	owner    string
	hostname string
	pid      int
	version  string
	revision uint64
}

type pendingPublish struct {
	future    nats.PubAckFuture
	timestamp uint64
	subject   string
}

func run(ctx context.Context, cfg config) error {
	log.Printf(
		"starting CDC cluster=%s nats=%s stream=%s publish_mode=%s",
		cfg.clusterIDDecimal,
		cfg.natsURL,
		cfg.eventStream,
		cfg.publishMode,
	)

	nc, err := nats.Connect(cfg.natsURL, nats.Name("tb-cdc-nats"))
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
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

	if err := ensureEventStream(js, cfg); err != nil {
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

	lock, err := acquireLock(lockKV, cfg.lockKey(), cfg.version)
	if err != nil {
		return err
	}
	defer func() {
		if err := lock.release(); err != nil {
			log.Printf("warning: release lock: %v", err)
		}
	}()

	lockCtx, lockCancel := context.WithCancel(ctx)
	defer lockCancel()
	lockErrCh := make(chan error, 1)
	go lock.refreshLoop(lockCtx, cfg.lockRefresh, lockErrCh)

	lastTimestamp, err := recoverProgress(cfg, progressKV)
	if err != nil {
		return err
	}

	tbClient, err := tigerbeetle_go.NewClient(cfg.clusterID, cfg.addresses)
	if err != nil {
		return fmt.Errorf("create TigerBeetle client: %w", err)
	}
	defer tbClient.Close()

	rateLimiter := newRequestRateLimiter(cfg.requestsPerSecondLimit)

	for {
		select {
		case <-ctx.Done():
			return nil
		case lockErr := <-lockErrCh:
			return lockErr
		default:
		}

		if err := rateLimiter.wait(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("wait for rate limiter: %w", err)
		}

		nextTimestamp, err := nextQueryTimestamp(lastTimestamp)
		if err != nil {
			return err
		}

		events, err := tbClient.GetChangeEvents(types.ChangeEventsFilter{
			TimestampMin: nextTimestamp,
			TimestampMax: 0,
			Limit:        cfg.eventCountMax,
		})
		if err != nil {
			return fmt.Errorf("get_change_events(timestamp_min=%d): %w", nextTimestamp, err)
		}

		if len(events) == 0 {
			if err := sleepContext(ctx, cfg.idleInterval); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			continue
		}

		if err := publishEventsAndCheckpoint(ctx, js, progressKV, cfg, events, &lastTimestamp); err != nil {
			return err
		}
	}
}

func desiredEventStreamConfig(cfg config) *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:       cfg.eventStream,
		Subjects:   cfg.eventStreamSubjects(),
		Retention:  nats.LimitsPolicy,
		Storage:    cfg.streamStorage,
		Replicas:   cfg.streamReplicas,
		Discard:    nats.DiscardOld,
		Duplicates: cfg.dedupeWindow,
		MaxAge:     cfg.streamMaxAge,
		MaxBytes:   normalizeUnlimitedBytes(cfg.streamMaxBytes),
	}
}

func desiredProgressKVConfig(cfg config) nats.KeyValueConfig {
	return nats.KeyValueConfig{
		Bucket:      cfg.progressBucket,
		Description: "TigerBeetle CDC progress",
		History:     1,
		Storage:     cfg.kvStorage,
		Replicas:    cfg.kvReplicas,
	}
}

func desiredLockKVConfig(cfg config) nats.KeyValueConfig {
	return nats.KeyValueConfig{
		Bucket:      cfg.lockBucket,
		Description: "TigerBeetle CDC lock",
		History:     1,
		Storage:     cfg.kvStorage,
		Replicas:    cfg.kvReplicas,
		TTL:         cfg.lockTTL,
	}
}

func ensureEventStream(js nats.JetStreamContext, cfg config) error {
	desired := desiredEventStreamConfig(cfg)

	info, err := js.StreamInfo(desired.Name)
	if err == nil {
		mismatches := streamConfigMismatches(info.Config, *desired)
		if len(mismatches) == 0 {
			return nil
		}

		if cfg.provision && cfg.streamUpdate {
			if _, err := js.UpdateStream(desired); err != nil {
				return fmt.Errorf("update stream %q: %w", desired.Name, err)
			}
			log.Printf("updated stream %q to expected CDC configuration", desired.Name)
			return nil
		}

		advice := "rerun with --stream-update (and --provision=true) to apply the expected stream config"
		if !cfg.provision {
			advice = "enable --provision=true and --stream-update, or update the stream manually"
		}

		return fmt.Errorf(
			"stream %q config mismatch: %s; %s",
			desired.Name,
			strings.Join(mismatches, "; "),
			advice,
		)
	}

	if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("lookup stream %q: %w", desired.Name, err)
	}

	if !cfg.provision {
		return fmt.Errorf("stream %q not found and --provision=false", desired.Name)
	}

	if _, err := js.AddStream(desired); err != nil {
		if _, lookupErr := js.StreamInfo(desired.Name); lookupErr == nil {
			return nil
		}
		return fmt.Errorf("create stream %q: %w", desired.Name, err)
	}

	log.Printf("created stream %q", desired.Name)
	return nil
}

func ensureKV(
	js nats.JetStreamContext,
	desired nats.KeyValueConfig,
	provision bool,
) (nats.KeyValue, error) {
	kv, err := js.KeyValue(desired.Bucket)
	if err == nil {
		if err := validateKVConfig(js, kv, desired); err != nil {
			return nil, err
		}
		return kv, nil
	}

	if !errors.Is(err, nats.ErrBucketNotFound) {
		return nil, fmt.Errorf("lookup kv bucket %q: %w", desired.Bucket, err)
	}

	if !provision {
		return nil, fmt.Errorf("kv bucket %q not found and --provision=false", desired.Bucket)
	}

	kv, err = js.CreateKeyValue(&desired)
	if err != nil {
		if kv, lookupErr := js.KeyValue(desired.Bucket); lookupErr == nil {
			if err := validateKVConfig(js, kv, desired); err != nil {
				return nil, err
			}
			return kv, nil
		}
		return nil, fmt.Errorf("create kv bucket %q: %w", desired.Bucket, err)
	}

	log.Printf("created kv bucket %q", desired.Bucket)
	return kv, nil
}

func validateKVConfig(js nats.JetStreamContext, kv nats.KeyValue, desired nats.KeyValueConfig) error {
	status, err := kv.Status()
	if err != nil {
		return fmt.Errorf("read kv bucket status for %q: %w", desired.Bucket, err)
	}

	mismatches := make([]string, 0, 4)

	if status.History() != int64(desired.History) {
		mismatches = append(mismatches, fmt.Sprintf("history=%d (expected %d)", status.History(), desired.History))
	}

	if status.TTL() != desired.TTL {
		mismatches = append(mismatches, fmt.Sprintf("ttl=%s (expected %s)", status.TTL(), desired.TTL))
	}

	if streamInfo, err := js.StreamInfo(kvStreamName(desired.Bucket)); err == nil {
		if streamInfo.Config.Storage != desired.Storage {
			mismatches = append(
				mismatches,
				fmt.Sprintf(
					"storage=%s (expected %s)",
					storageTypeLabel(streamInfo.Config.Storage),
					storageTypeLabel(desired.Storage),
				),
			)
		}

		if streamInfo.Config.Replicas != desired.Replicas {
			mismatches = append(
				mismatches,
				fmt.Sprintf("replicas=%d (expected %d)", streamInfo.Config.Replicas, desired.Replicas),
			)
		}
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		mismatches = append(mismatches, fmt.Sprintf("unable to inspect kv stream replicas: %v", err))
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("kv bucket %q config mismatch: %s", desired.Bucket, strings.Join(mismatches, "; "))
	}

	return nil
}

func streamConfigMismatches(actual nats.StreamConfig, expected nats.StreamConfig) []string {
	mismatches := make([]string, 0, 8)

	if !stringSlicesEqual(actual.Subjects, expected.Subjects) {
		mismatches = append(
			mismatches,
			fmt.Sprintf("subjects=%v (expected %v)", actual.Subjects, expected.Subjects),
		)
	}

	if actual.Storage != expected.Storage {
		mismatches = append(
			mismatches,
			fmt.Sprintf("storage=%s (expected %s)", storageTypeLabel(actual.Storage), storageTypeLabel(expected.Storage)),
		)
	}

	if actual.Replicas != expected.Replicas {
		mismatches = append(mismatches, fmt.Sprintf("replicas=%d (expected %d)", actual.Replicas, expected.Replicas))
	}

	if actual.Retention != expected.Retention {
		mismatches = append(mismatches, fmt.Sprintf("retention=%v (expected %v)", actual.Retention, expected.Retention))
	}

	if actual.Discard != expected.Discard {
		mismatches = append(mismatches, fmt.Sprintf("discard=%v (expected %v)", actual.Discard, expected.Discard))
	}

	if normalizeUnlimitedBytes(actual.MaxBytes) != normalizeUnlimitedBytes(expected.MaxBytes) {
		mismatches = append(
			mismatches,
			fmt.Sprintf("max_bytes=%d (expected %d)", actual.MaxBytes, expected.MaxBytes),
		)
	}

	if actual.MaxAge != expected.MaxAge {
		mismatches = append(mismatches, fmt.Sprintf("max_age=%s (expected %s)", actual.MaxAge, expected.MaxAge))
	}

	if actual.Duplicates != expected.Duplicates {
		mismatches = append(
			mismatches,
			fmt.Sprintf("duplicate_window=%s (expected %s)", actual.Duplicates, expected.Duplicates),
		)
	}

	return mismatches
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

func acquireLock(kv nats.KeyValue, key string, version string) (*lockHandle, error) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	lock := &lockHandle{
		kv:       kv,
		key:      key,
		hostname: hostname,
		pid:      os.Getpid(),
		version:  version,
		owner:    fmt.Sprintf("%s/%d/%d", hostname, os.Getpid(), time.Now().UnixNano()),
	}

	payload, err := lock.payload()
	if err != nil {
		return nil, err
	}

	revision, err := kv.Create(key, payload)
	if err == nil {
		lock.revision = revision
		log.Printf("acquired lock %q", key)
		return lock, nil
	}

	if errors.Is(err, nats.ErrKeyExists) {
		entry, getErr := kv.Get(key)
		if getErr == nil {
			return nil, fmt.Errorf("lock %q is already held: %s", key, lockHolderDescription(entry))
		}
		return nil, fmt.Errorf("lock %q is already held", key)
	}

	return nil, fmt.Errorf("acquire lock %q: %w", key, err)
}

func lockHolderDescription(entry nats.KeyValueEntry) string {
	if entry == nil {
		return "owner unknown"
	}

	var holder lockRecord
	if err := json.Unmarshal(entry.Value(), &holder); err != nil {
		return fmt.Sprintf("revision=%d (unparseable lock payload)", entry.Revision())
	}

	return fmt.Sprintf(
		"owner=%s host=%s pid=%d version=%s updated_at=%s revision=%d",
		holder.Owner,
		holder.Hostname,
		holder.PID,
		holder.Version,
		holder.UpdatedAt,
		entry.Revision(),
	)
}

func (l *lockHandle) refreshLoop(ctx context.Context, interval time.Duration, errCh chan<- error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.refresh(); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}
}

func (l *lockHandle) refresh() error {
	payload, err := l.payload()
	if err != nil {
		return err
	}

	revision, err := l.kv.Update(l.key, payload, l.revision)
	if err != nil {
		return fmt.Errorf("refresh lock %q: %w", l.key, err)
	}

	l.revision = revision
	return nil
}

func (l *lockHandle) payload() ([]byte, error) {
	value := lockRecord{
		Owner:     l.owner,
		Hostname:  l.hostname,
		PID:       l.pid,
		Version:   l.version,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal lock payload: %w", err)
	}

	return payload, nil
}

func (l *lockHandle) release() error {
	err := l.kv.Delete(l.key)
	if err == nil || errors.Is(err, nats.ErrKeyNotFound) || errors.Is(err, nats.ErrKeyDeleted) {
		return nil
	}
	return fmt.Errorf("release lock %q: %w", l.key, err)
}

func nextQueryTimestamp(lastTimestamp uint64) (uint64, error) {
	if lastTimestamp == 0 {
		return 1, nil
	}

	if lastTimestamp == math.MaxUint64 {
		return 0, fmt.Errorf("cannot continue from timestamp %d", lastTimestamp)
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

func normalizeUnlimitedBytes(value int64) int64 {
	if value == 0 || value == -1 {
		return -1
	}
	return value
}

func kvStreamName(bucket string) string {
	return "KV_" + bucket
}

func stringSlicesEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
