package cdcnats

import (
	"context"
	"errors"
	"fmt"
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

// stallingKV is a KeyValue whose Update blocks until release is closed, like a request to an
// unresponsive server.
type stallingKV struct {
	nats.KeyValue
	release chan struct{}
}

func (s stallingKV) Update(string, []byte, uint64) (uint64, error) {
	<-s.release
	return 0, nats.ErrTimeout
}

func TestKeepAlive_ReportsLossWhenRenewalHangs(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	lock := &lockHandle{kv: stallingKV{release: release}, key: "lock.7", renewedAt: time.Now()}

	lost := make(chan error, 1)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		lock.keepAlive(context.Background(), 20*time.Millisecond, 500*time.Millisecond, func(err error) { lost <- err })
	}()

	select {
	case err := <-lost:
		if !errors.Is(err, errLockLost) {
			t.Fatalf("loss error = %v, want errLockLost", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("keepAlive did not report loss while a renewal was hanging")
	}

	// keepAlive waits for the hung request before returning, so release never races a renewal.
	select {
	case <-returned:
		t.Fatalf("keepAlive returned while a renewal was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-returned
}

func TestIsWrongLastSequence(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&nats.APIError{ErrorCode: nats.JSErrCodeStreamWrongLastSequence}, true},
		{fmt.Errorf("wrapped: %w", &nats.APIError{ErrorCode: nats.JSErrCodeStreamWrongLastSequenceConstant}), true},
		{&nats.APIError{ErrorCode: nats.JSErrCodeStreamNotFound}, false},
		{nats.ErrTimeout, false},
	} {
		if got := isWrongLastSequence(tc.err); got != tc.want {
			t.Errorf("isWrongLastSequence(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}
