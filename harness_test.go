package cdcnats

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	tberrors "github.com/tigerbeetle/tigerbeetle-go/pkg/errors"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// startJetStream runs an in-process NATS server with JetStream enabled and returns its client URL.
func startJetStream(t *testing.T) string {
	t.Helper()

	s, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		t.Fatalf("server.NewServer(): %v", err)
	}

	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatalf("NATS server did not start")
	}
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})

	return s.ClientURL()
}

// connectJetStream opens a client connection for test assertions.
func connectJetStream(t *testing.T, url string) nats.JetStreamContext {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect(): %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nc.JetStream(): %v", err)
	}
	return js
}

// testConfig returns a valid config for cluster 7 with short intervals, as parseConfig would build it
// from these flags plus extraArgs.
func testConfig(t *testing.T, natsURL string, extraArgs ...string) config {
	t.Helper()

	cfg, err := parseConfig(append([]string{
		"--cluster-id=7",
		"--addresses=127.0.0.1:3000",
		"--nats-url=" + natsURL,
		"--idle-interval-ms=20",
		"--lock-ttl=2s",
		"--lock-refresh=100ms",
	}, extraArgs...), "test")
	if err != nil {
		t.Fatalf("parseConfig(): %v", err)
	}
	return cfg
}

// fakeSource is an in-memory changeEventSource that serves its events in timestamp order. When
// unreachable is set, GetChangeEvents blocks until Close, like the real client when no replica
// answers, and closes blocked first.
type fakeSource struct {
	unreachable bool
	blocked     chan struct{}
	blockOnce   sync.Once

	mu        sync.Mutex
	events    []types.ChangeEvent
	closed    chan struct{}
	closeOnce sync.Once
}

func newFakeSource(events ...types.ChangeEvent) *fakeSource {
	return &fakeSource{events: events, blocked: make(chan struct{}), closed: make(chan struct{})}
}

func (f *fakeSource) GetChangeEvents(filter types.ChangeEventsFilter) ([]types.ChangeEvent, error) {
	if f.unreachable {
		f.blockOnce.Do(func() { close(f.blocked) })
		<-f.closed
	}
	select {
	case <-f.closed:
		return nil, tberrors.ErrClientClosed{}
	default:
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var batch []types.ChangeEvent
	for _, event := range f.events {
		if event.Timestamp >= filter.TimestampMin && len(batch) < int(filter.Limit) {
			batch = append(batch, event)
		}
	}
	return batch, nil
}

func (f *fakeSource) Close() {
	f.closeOnce.Do(func() { close(f.closed) })
}

func (f *fakeSource) open(config) (changeEventSource, error) {
	return f, nil
}

// testEvent returns a single-phase change event on ledger 1 with the given timestamp.
func testEvent(timestamp uint64) types.ChangeEvent {
	return types.ChangeEvent{
		Timestamp:         timestamp,
		Type:              types.ChangeEventSinglePhase,
		Ledger:            1,
		TransferID:        types.ToUint128(timestamp),
		TransferAmount:    types.ToUint128(10),
		TransferCode:      1,
		TransferTimestamp: timestamp,
	}
}

// startRun runs the publisher in the background. The returned channel yields run's result.
func startRun(t *testing.T, cfg config, source *fakeSource) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- run(ctx, cfg, source.open)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("run did not stop after cancellation")
		}
	})
	return cancel, result
}

// awaitResult waits for run to return.
func awaitResult(t *testing.T, result <-chan error, timeout time.Duration) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		t.Fatalf("run did not return within %s", timeout)
		return nil
	}
}

// eventually polls condition until it holds or the timeout expires.
func eventually(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// lockOwner returns the owner recorded under the lock key, or "" if the key is absent.
func lockOwner(t *testing.T, js nats.JetStreamContext, cfg config) string {
	t.Helper()

	kv, err := js.KeyValue(cfg.lockBucket)
	if errors.Is(err, nats.ErrBucketNotFound) {
		return ""
	}
	if err != nil {
		t.Fatalf("KeyValue(%q): %v", cfg.lockBucket, err)
	}

	owner, _ := lockHolder(kv, cfg.lockKey())
	return owner
}
