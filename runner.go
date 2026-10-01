package cdcnats

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	tigerbeetle_go "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

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

	js, err := nc.JetStream(
		// The publisher bounds its own outstanding messages, so the client's limit is never the one hit.
		nats.PublishAsyncMaxPending(cfg.maxInFlight()),
		// Resolve every async publish within the timeout, even if its acknowledgement is lost, so no
		// message stays pending after a failure (see publisher.publish).
		nats.PublishAsyncTimeout(cfg.publishAckTimeout),
	)
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

	err = replicate(runCtx, nc, js, progressKV, cfg, openSource)
	// The first cancellation wins: report a lost lock even if a shutdown was requested afterwards.
	if cause := context.Cause(runCtx); errors.Is(cause, errLockLost) {
		return cause
	}
	if ctx.Err() != nil {
		return nil
	}
	return err
}

const (
	// minRetryDelay and maxRetryDelay bound the backoff between attempts to resume publishing after a
	// NATS failure, such as a stream leader election.
	minRetryDelay = 500 * time.Millisecond
	maxRetryDelay = 30 * time.Second
)

// replicate repeatedly fetches change events from TigerBeetle and publishes them until ctx is done
// or an error it can't recover from occurs.
//
// It resumes from the event stream (see recoverPosition). When a publish fails, for example during a
// NATS leader election, it backs off and resumes from the stream again rather than exiting. That is
// safe because the publisher only appends events that directly follow the stream's last one.
func replicate(
	ctx context.Context,
	nc *nats.Conn,
	js nats.JetStreamContext,
	progressKV nats.KeyValue,
	cfg config,
	openSource func(config) (changeEventSource, error),
) error {
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

	var (
		// publisher is nil until the position has been recovered from the stream, and again after a
		// failure, so the next attempt resumes from whatever the stream now holds.
		publisher     *publisher
		lastTimestamp uint64
		streamCreated time.Time
		failures      int
		window        = cfg.maxInFlight()
	)

	// retryLater waits after a failure that resuming from the stream can get past, backing off while
	// failures repeat. Other failures are returned, to stop the run.
	retryLater := func(err error) error {
		if !isTransient(err) {
			return err
		}
		failures++
		delay := min(minRetryDelay<<min(failures-1, 16), maxRetryDelay)
		log.Printf("warning: %v; resuming from the stream in %s", err, delay)
		publisher = nil
		return sleepContext(ctx, delay)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if publisher == nil {
			resumeAt, err := recoverPosition(js, cfg)
			if err != nil {
				if err := retryLater(err); err != nil {
					return err
				}
				continue
			}
			lastTimestamp, streamCreated = resumeAt.timestamp, resumeAt.streamCreated
			publisher = newPublisher(js, cfg, resumeAt.streamSeq, window)
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

		// Check before every batch: a reconnect between batches can land on a different server version
		// without any publish failing. Nothing is in flight between batches, so the limit can change.
		if next := publishWindow(cfg, nc.ConnectedServerVersion()); next != window {
			log.Printf("publishing up to %d messages at a time (connected to nats-server %s)", next, nc.ConnectedServerVersion())
			window = next
		}
		publisher.maxInFlight = window

		if err := publisher.publish(ctx, events); err != nil {
			if ctx.Err() != nil {
				return err
			}
			if err := retryLater(err); err != nil {
				return err
			}
			continue
		}
		failures = 0
		lastTimestamp = events[len(events)-1].Timestamp

		// The stream itself records progress. This checkpoint is only read if retention empties the
		// stream, so a failure here doesn't risk losing or repeating events and need not stop publishing.
		if err := writeProgress(progressKV, cfg, lastTimestamp, publisher.lastSeq, streamCreated); err != nil {
			log.Printf("warning: %v", err)
		}
		log.Printf("published events=%d last_timestamp=%d stream_seq=%d", len(events), lastTimestamp, publisher.lastSeq)
	}
}

// publishWindow is how many messages the publisher may have awaiting acknowledgement.
//
// Before 2.14, nats-server ignores a failed write while applying a message to a replicated stream, so
// a message pipelined behind it can take its place and leave a gap. Publishing to a replicated stream
// is serialised when connected to such a server. The client only sees the version of the server it is
// connected to, so this runs before every batch (covering reconnects), and clusters running mixed
// versions must use --publish-mode=sync until every server runs 2.14 or later.
func publishWindow(cfg config, connectedServerVersion string) int {
	if cfg.streamReplicas > 1 && !serverAtLeast(connectedServerVersion, 2, 14) {
		return 1
	}
	return cfg.maxInFlight()
}

// serverAtLeast reports whether a nats-server version such as "2.12.15" is at least major.minor.
func serverAtLeast(version string, major, minor int) bool {
	var gotMajor, gotMinor int
	if _, err := fmt.Sscanf(version, "%d.%d", &gotMajor, &gotMinor); err != nil {
		return false
	}
	return gotMajor > major || (gotMajor == major && gotMinor >= minor)
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
