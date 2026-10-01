package cdcnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

// errLockLost marks errors reporting that this instance no longer holds the lock.
var errLockLost = errors.New("lost lock")

// lockRecord is the JSON value stored under the lock key. It identifies the holder so that a
// waiting instance can report who holds the lock.
type lockRecord struct {
	Owner     string `json:"owner"`
	Hostname  string `json:"hostname"`
	PID       int    `json:"pid"`
	Version   string `json:"version"`
	UpdatedAt string `json:"updated_at"`
}

// lockHandle is a held single-writer lock: a key in a TTL-enabled KV bucket that the holder renews
// by compare-and-swap on its revision. If renewals stop, the bucket TTL expires the key and another
// instance can acquire it.
//
// revision and renewedAt are owned by whichever goroutine is currently driving the lock: acquireLock,
// then keepAlive, then release. Callers must wait for keepAlive to return before calling release.
type lockHandle struct {
	kv       nats.KeyValue
	key      string
	owner    string
	hostname string
	pid      int
	version  string

	revision uint64
	// renewedAt is when the last successful write was sent. The server stamps the write later, so
	// renewedAt + TTL is a lower bound on when the lock expires.
	renewedAt time.Time
}

// acquireLock creates the lock key, waiting while another instance holds it. It retries every
// retryInterval until the key is free (released, or expired by the bucket TTL) or ctx is cancelled.
func acquireLock(ctx context.Context, kv nats.KeyValue, key string, version string, retryInterval time.Duration) (*lockHandle, error) {
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

	lastHolder := ""
	for {
		payload, err := lock.payload()
		if err != nil {
			return nil, err
		}

		sentAt := time.Now()
		revision, err := kv.Create(key, payload)
		if err == nil {
			lock.revision = revision
			lock.renewedAt = sentAt
			log.Printf("acquired lock %q", key)
			return lock, nil
		}

		// Create reports a held key as ErrKeyExists, but when racing another instance to replace a
		// deleted key it can return the raw wrong-last-sequence error instead.
		if !errors.Is(err, nats.ErrKeyExists) && !isWrongLastSequence(err) {
			return nil, fmt.Errorf("acquire lock %q: %w", key, err)
		}

		holder, description := lockHolder(kv, key)
		if holder != lastHolder {
			log.Printf("lock %q is held by another instance (%s); waiting for it to be released or expire", key, description)
			lastHolder = holder
		}

		if err := sleepContext(ctx, retryInterval); err != nil {
			return nil, err
		}
	}
}

// lockHolder reads the current lock value and returns the holder's owner ID (to detect handovers)
// and a human-readable description for logs.
func lockHolder(kv nats.KeyValue, key string) (owner string, description string) {
	entry, err := kv.Get(key)
	if err != nil {
		return "", "owner unknown"
	}

	var holder lockRecord
	if err := json.Unmarshal(entry.Value(), &holder); err != nil {
		return "", fmt.Sprintf("revision=%d (unparseable lock payload)", entry.Revision())
	}

	return holder.Owner, fmt.Sprintf(
		"owner=%s host=%s pid=%d version=%s updated_at=%s revision=%d",
		holder.Owner,
		holder.Hostname,
		holder.PID,
		holder.Version,
		holder.UpdatedAt,
		entry.Revision(),
	)
}

// keepAlive renews the lock every interval until ctx is cancelled. Failed renewals are retried. It
// calls onLost and returns if another instance takes the lock, or if no renewal succeeds before the
// lock comes within ttl/10 of expiring. That deadline has its own timer, so a slow or hung renewal
// request cannot delay onLost.
func (l *lockHandle) keepAlive(ctx context.Context, interval time.Duration, ttl time.Duration, onLost func(error)) {
	// Give up this long before the lock could expire, leaving time to stop publishing.
	safeUntil := func() time.Duration { return time.Until(l.renewedAt.Add(ttl - ttl/10)) }
	retryInterval := min(time.Second, interval)

	renewTimer := time.NewTimer(interval)
	defer renewTimer.Stop()
	expiryTimer := time.NewTimer(safeUntil())
	defer expiryTimer.Stop()

	// inFlight receives the result of the outstanding renewal request, and is nil when there is none.
	// renew writes l.revision and l.renewedAt, so keepAlive waits for it before returning: release
	// must not run concurrently with it.
	var inFlight chan error
	defer func() {
		if inFlight != nil {
			<-inFlight
		}
	}()

	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return

		case <-expiryTimer.C:
			reason := "a renewal request did not complete in time"
			if lastErr != nil {
				reason = lastErr.Error()
			}
			onLost(fmt.Errorf("%w %q: could not renew it before it could expire: %s", errLockLost, l.key, reason))
			return

		case <-renewTimer.C:
			inFlight = make(chan error, 1)
			go func(result chan<- error) { result <- l.renew() }(inFlight)

		case err := <-inFlight:
			inFlight = nil
			switch {
			case err == nil:
				lastErr = nil
				expiryTimer.Reset(safeUntil())
				renewTimer.Reset(interval)
			case errors.Is(err, nats.ErrKeyRevisionMismatch):
				onLost(fmt.Errorf("%w %q: another instance holds it or it expired: %w", errLockLost, l.key, err))
				return
			default:
				lastErr = err
				log.Printf("warning: renew lock %q: %v; retrying", l.key, err)
				renewTimer.Reset(retryInterval)
			}
		}
	}
}

// renew rewrites the lock value, conditional on this instance's last revision still being current.
func (l *lockHandle) renew() error {
	payload, err := l.payload()
	if err != nil {
		return err
	}

	sentAt := time.Now()
	revision, err := l.kv.Update(l.key, payload, l.revision)
	if err != nil {
		return err
	}

	l.revision = revision
	l.renewedAt = sentAt
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

// release deletes the lock key if this instance still holds it. The delete is conditional on the
// last revision this instance wrote, so an instance that lost the lock never removes its successor's.
func (l *lockHandle) release() error {
	err := l.kv.Delete(l.key, nats.LastRevision(l.revision))
	switch {
	case err == nil:
		log.Printf("released lock %q", l.key)
		return nil
	case errors.Is(err, nats.ErrKeyRevisionMismatch):
		log.Printf("lock %q is no longer held by this instance; leaving it in place", l.key)
		return nil
	default:
		return fmt.Errorf("release lock %q: %w", l.key, err)
	}
}

// isWrongLastSequence reports whether err is JetStream rejecting a conditional write because the
// stream or subject has moved on. Non-replicated streams report code 10071, replicated ones 10164.
func isWrongLastSequence(err error) bool {
	var apiErr *nats.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode == nats.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == nats.JSErrCodeStreamWrongLastSequenceConstant
}
