package cdcnats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func newLockBucket(t *testing.T) nats.KeyValue {
	t.Helper()

	js := connectJetStream(t, startJetStream(t))
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "LOCKS", History: 1, TTL: 2 * time.Second})
	if err != nil {
		t.Fatalf("CreateKeyValue(): %v", err)
	}
	return kv
}

func TestLockRelease_KeepsSuccessorsLock(t *testing.T) {
	t.Parallel()
	kv := newLockBucket(t)
	ctx := context.Background()

	first, err := acquireLock(ctx, kv, "lock.7", "test", time.Second)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}

	// Simulate the first holder's lock expiring and a second instance taking over.
	if err := kv.Delete("lock.7"); err != nil {
		t.Fatalf("Delete(): %v", err)
	}
	second, err := acquireLock(ctx, kv, "lock.7", "test", time.Second)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}

	if err := first.release(); err != nil {
		t.Fatalf("release first: %v", err)
	}
	if owner, _ := lockHolder(kv, "lock.7"); owner != second.owner {
		t.Fatalf("lock owner after stale release = %q, want successor %q", owner, second.owner)
	}

	if err := second.release(); err != nil {
		t.Fatalf("release second: %v", err)
	}
	if _, err := kv.Get("lock.7"); !errors.Is(err, nats.ErrKeyNotFound) {
		t.Fatalf("Get() after release error = %v, want ErrKeyNotFound", err)
	}
}

func TestAcquireLock_WaitsForHolderToRelease(t *testing.T) {
	t.Parallel()
	kv := newLockBucket(t)

	holder, err := acquireLock(context.Background(), kv, "lock.7", "test", time.Second)
	if err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	acquired := make(chan *lockHandle, 1)
	go func() {
		waiter, err := acquireLock(context.Background(), kv, "lock.7", "test", 20*time.Millisecond)
		if err != nil {
			t.Errorf("acquire waiter: %v", err)
		}
		acquired <- waiter
	}()

	select {
	case <-acquired:
		t.Fatalf("waiter acquired a held lock")
	case <-time.After(200 * time.Millisecond):
	}

	if err := holder.release(); err != nil {
		t.Fatalf("release holder: %v", err)
	}

	select {
	case waiter := <-acquired:
		if owner, _ := lockHolder(kv, "lock.7"); owner != waiter.owner {
			t.Fatalf("lock owner = %q, want waiter %q", owner, waiter.owner)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("waiter did not acquire the released lock")
	}
}

func TestAcquireLock_StopsWaitingOnCancel(t *testing.T) {
	t.Parallel()
	kv := newLockBucket(t)

	if _, err := acquireLock(context.Background(), kv, "lock.7", "test", time.Second); err != nil {
		t.Fatalf("acquire holder: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := acquireLock(ctx, kv, "lock.7", "test", 20*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquireLock() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestKeepAlive_ReportsTakeover(t *testing.T) {
	t.Parallel()
	kv := newLockBucket(t)

	lock, err := acquireLock(context.Background(), kv, "lock.7", "test", time.Second)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	lost := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go lock.keepAlive(ctx, 20*time.Millisecond, 2*time.Second, func(err error) { lost <- err })

	// Let a few renewals succeed, then have another instance take the lock over.
	time.Sleep(100 * time.Millisecond)
	if err := kv.Delete("lock.7"); err != nil {
		t.Fatalf("Delete(): %v", err)
	}
	if _, err := kv.Create("lock.7", []byte(`{"owner":"other"}`)); err != nil {
		t.Fatalf("Create(): %v", err)
	}

	select {
	case err := <-lost:
		if !errors.Is(err, nats.ErrKeyRevisionMismatch) {
			t.Fatalf("loss error = %v, want ErrKeyRevisionMismatch", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("keepAlive did not report the takeover")
	}
}
