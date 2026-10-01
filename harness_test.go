package cdcnats

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
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

// startJetStreamCluster runs three in-process NATS servers as a JetStream cluster, so tests can use
// replicated (R3) streams and buckets. It returns a comma-separated list of client URLs.
func startJetStreamCluster(t *testing.T) string {
	t.Helper()

	// Route ports must be known up front. Another test can take a reserved port before a server binds
	// it, so start the servers together right after reserving, and retry with new ports if one fails.
	for attempt := 1; ; attempt++ {
		servers, ok := tryStartJetStreamCluster(t)
		if !ok {
			if attempt == 3 {
				t.Fatalf("JetStream cluster did not start after %d attempts", attempt)
			}
			continue
		}

		eventually(t, 30*time.Second, "the JetStream cluster to elect a leader with all peers", func() bool {
			for _, s := range servers {
				if s.JetStreamIsLeader() && len(s.JetStreamClusterPeers()) == 3 {
					return true
				}
			}
			return false
		})

		urls := make([]string, len(servers))
		for i, s := range servers {
			urls[i] = s.ClientURL()
		}
		return strings.Join(urls, ",")
	}
}

// tryStartJetStreamCluster starts three clustered servers on freshly reserved route ports. It reports
// false, after shutting them down, if any fails to start.
func tryStartJetStreamCluster(t *testing.T) ([]*server.Server, bool) {
	t.Helper()

	routes := make([]*url.URL, 3)
	ports := make([]int, 3)
	for i := range ports {
		ports[i] = freePort(t)
		routes[i] = &url.URL{Scheme: "nats-route", Host: fmt.Sprintf("127.0.0.1:%d", ports[i])}
	}

	servers := make([]*server.Server, 3)
	for i := range servers {
		s, err := server.NewServer(&server.Options{
			ServerName: fmt.Sprintf("n%d", i+1),
			Host:       "127.0.0.1",
			Port:       server.RANDOM_PORT,
			JetStream:  true,
			StoreDir:   t.TempDir(),
			Cluster:    server.ClusterOpts{Name: "test", Host: "127.0.0.1", Port: ports[i]},
			Routes:     routes,
			NoLog:      true,
			NoSigs:     true,
		})
		if err != nil {
			t.Fatalf("server.NewServer(): %v", err)
		}
		servers[i] = s
		go s.Start()
	}

	stop := func() {
		for _, s := range servers {
			s.Shutdown()
			s.WaitForShutdown()
		}
	}
	for _, s := range servers {
		if !s.ReadyForConnections(10 * time.Second) {
			stop()
			return nil, false
		}
	}
	t.Cleanup(stop)
	return servers, true
}

// freePort returns a TCP port that was free a moment ago.
func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve TCP port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP port: %v", err)
	}
	return port
}

// readStreamTimestamps returns the TigerBeetle timestamp of every event in the stream, in stream order.
func readStreamTimestamps(js nats.JetStreamContext, stream string) ([]uint64, error) {
	info, err := js.StreamInfo(stream)
	if err != nil {
		return nil, fmt.Errorf("StreamInfo(%q): %w", stream, err)
	}

	var timestamps []uint64
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		msg, err := js.GetMsg(stream, seq)
		if err != nil {
			return nil, fmt.Errorf("GetMsg(%q, %d): %w", stream, seq, err)
		}
		_, timestamp, ok := parseEventMsgID(msg.Header.Get(nats.MsgIdHdr))
		if !ok {
			return nil, fmt.Errorf("message %d has no event Nats-Msg-Id", seq)
		}
		timestamps = append(timestamps, timestamp)
	}
	return timestamps, nil
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
	// beforeBatch, if set, runs before GetChangeEvents returns a non-empty batch.
	beforeBatch func()
	blocked     chan struct{}
	blockOnce   sync.Once

	mu        sync.Mutex
	events    []types.ChangeEvent
	closed    chan struct{}
	closeOnce sync.Once
	// highestServed is the latest timestamp returned so far. Normally each query starts after it;
	// refetches counts queries that start at or before it, which happen only when publishing resumes
	// after a failure left some served events unpublished.
	highestServed uint64
	refetches     int
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
	if len(batch) > 0 {
		if filter.TimestampMin <= f.highestServed {
			f.refetches++
		}
		f.highestServed = max(f.highestServed, batch[len(batch)-1].Timestamp)
		if f.beforeBatch != nil {
			f.beforeBatch()
		}
	}
	return batch, nil
}

// refetchCount returns how many times a batch was fetched again after publishing it failed.
func (f *fakeSource) refetchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.refetches
}

// add makes more events available, as if they were just committed.
func (f *fakeSource) add(events ...types.ChangeEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, events...)
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

// testEvents returns single-phase events with timestamps 1..n times step.
func testEvents(n int, step uint64) []types.ChangeEvent {
	events := make([]types.ChangeEvent, n)
	for i := range events {
		events[i] = testEvent(uint64(i+1) * step)
	}
	return events
}

// timestampsOf returns the timestamps of events, in order.
func timestampsOf(events []types.ChangeEvent) []uint64 {
	timestamps := make([]uint64, len(events))
	for i, event := range events {
		timestamps[i] = event.Timestamp
	}
	return timestamps
}

// awaitStream waits until the stream holds exactly the events with the given timestamps, in order.
// It fails as soon as the stream holds anything else.
func awaitStream(t *testing.T, js nats.JetStreamContext, stream string, want []uint64) {
	t.Helper()
	awaitStreamWhileRunning(t, js, stream, want, nil)
}

// awaitStreamWhileRunning is awaitStream for a stream a run is publishing to: it also fails, with
// the run's error, if the run stops first.
func awaitStreamWhileRunning(t *testing.T, js nats.JetStreamContext, stream string, want []uint64, result <-chan error) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for {
		select {
		case err := <-result:
			t.Fatalf("run stopped before the stream held every event: %v", err)
		default:
		}

		// Reads fail until the stream exists, and while a leader election is in progress.
		got, err := readStreamTimestamps(js, stream)
		if err == nil {
			if slices.Equal(got, want) {
				return
			}
			if len(got) > len(want) || !slices.Equal(got, want[:len(got)]) {
				t.Fatalf("stream %q timestamps = %v, want %v", stream, got, want)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %q timestamps = %v (err %v), want %v", stream, got, err, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
