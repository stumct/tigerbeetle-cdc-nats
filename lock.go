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

		if !errors.Is(err, nats.ErrKeyExists) {
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

// keepAlive renews the lock every interval until ctx is cancelled. A failed renewal is retried until
// the lock is close to expiring. If another instance now holds the lock, or it cannot be renewed in
// time, keepAlive calls onLost with the reason and returns.
func (l *lockHandle) keepAlive(ctx context.Context, interval time.Duration, ttl time.Duration, onLost func(error)) {
	// Stop trying this long before the lock could expire, leaving time to stop publishing.
	margin := ttl / 10
	retryInterval := min(time.Second, interval)

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		err := l.renew()
		switch {
		case err == nil:
			timer.Reset(interval)
		case errors.Is(err, nats.ErrKeyRevisionMismatch):
			onLost(fmt.Errorf("lost lock %q: another instance holds it or it expired: %w", l.key, err))
			return
		case time.Until(l.renewedAt.Add(ttl)) <= margin:
			onLost(fmt.Errorf("lost lock %q: could not renew it before it expires: %w", l.key, err))
			return
		default:
			log.Printf("warning: renew lock %q: %v; retrying", l.key, err)
			timer.Reset(retryInterval)
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
