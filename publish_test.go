package cdcnats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// runUntilPublished runs the publisher until the stream holds exactly want and the checkpoint
// records its last event, then stops it cleanly.
func runUntilPublished(t *testing.T, js nats.JetStreamContext, cfg config, source *fakeSource, want []uint64) {
	t.Helper()

	cancel, result := startRun(t, cfg, source)
	awaitStreamWhileRunning(t, js, cfg.eventStream, want, result)
	eventually(t, 10*time.Second, "the checkpoint to record the last event", func() bool {
		progress, found, err := readProgress(js, cfg)
		return err == nil && found && progress.Timestamp == want[len(want)-1]
	})
	cancel()
	if err := awaitResult(t, result, 5*time.Second); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

// setProgress overwrites the progress checkpoint for the current stream, as a run that stopped at
// another point would.
func setProgress(t *testing.T, js nats.JetStreamContext, cfg config, timestamp uint64, streamSeq uint64) {
	t.Helper()

	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		t.Fatalf("StreamInfo(): %v", err)
	}
	writeCheckpoint(t, js, cfg, progressRecord{Timestamp: timestamp, StreamSeq: streamSeq, StreamCreated: info.Created})
}

func writeCheckpoint(t *testing.T, js nats.JetStreamContext, cfg config, progress progressRecord) {
	t.Helper()

	kv, err := js.KeyValue(cfg.progressBucket)
	if err != nil {
		t.Fatalf("KeyValue(): %v", err)
	}
	if err := writeProgress(kv, cfg, progress.Timestamp, progress.StreamSeq, progress.StreamCreated); err != nil {
		t.Fatalf("writeProgress(): %v", err)
	}
}

// expectRunError runs the publisher and checks that it stops with an error mentioning want.
func expectRunError(t *testing.T, cfg config, source *fakeSource, want string) {
	t.Helper()

	_, result := startRun(t, cfg, source)
	err := awaitResult(t, result, 10*time.Second)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("run() error = %v, want it to mention %q", err, want)
	}
}

func TestRun_PipelinesManyEventsInOrder(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url, "--publish-async-max-pending=64", "--event-count-max=500")

	events := testEvents(2000, 10)
	runUntilPublished(t, js, cfg, newFakeSource(events...), timestampsOf(events))
}

func TestRun_ResumesFromStreamWhenCheckpointIsBehind(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	// A short dedupe window, so JetStream would not catch a replay of already published events.
	cfg := testConfig(t, url, "--dedupe-window=100ms")

	runUntilPublished(t, js, cfg, newFakeSource(testEvent(10), testEvent(20), testEvent(30)), []uint64{10, 20, 30})

	// The previous run crashed after publishing but before checkpointing, long enough ago that the
	// dedupe window has passed.
	setProgress(t, js, cfg, 10, 1)
	time.Sleep(200 * time.Millisecond)

	more := newFakeSource(testEvent(10), testEvent(20), testEvent(30), testEvent(40))
	runUntilPublished(t, js, cfg, more, []uint64{10, 20, 30, 40})
}

func TestRun_RepublishesEventsMissingFromStreamWhenCheckpointIsAhead(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	runUntilPublished(t, js, cfg, newFakeSource(testEvent(10), testEvent(20)), []uint64{10, 20})

	// The checkpoint survived a NATS failure that lost events 30 and 40.
	setProgress(t, js, cfg, 40, 4)

	all := newFakeSource(testEvent(10), testEvent(20), testEvent(30), testEvent(40))
	runUntilPublished(t, js, cfg, all, []uint64{10, 20, 30, 40})
}

func TestRun_RefusesToResumeIntoRecreatedStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	events := []types.ChangeEvent{testEvent(10), testEvent(20)}
	runUntilPublished(t, js, cfg, newFakeSource(events...), []uint64{10, 20})

	if err := js.DeleteStream(cfg.eventStream); err != nil {
		t.Fatalf("DeleteStream(): %v", err)
	}
	expectRunError(t, cfg, newFakeSource(events...), "recreated")

	// --timestamp-last=0 is the explicit choice to republish everything into the new stream.
	override := testConfig(t, url, "--timestamp-last=0")
	runUntilPublished(t, js, override, newFakeSource(events...), []uint64{10, 20})
}

func TestRun_ResumesFromCheckpointWhenRetentionEmptiedStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	runUntilPublished(t, js, cfg, newFakeSource(testEvent(10), testEvent(20)), []uint64{10, 20})
	if err := js.PurgeStream(cfg.eventStream); err != nil {
		t.Fatalf("PurgeStream(): %v", err)
	}

	events := newFakeSource(testEvent(10), testEvent(20), testEvent(30))
	runUntilPublished(t, js, cfg, events, []uint64{30})
}

func TestRun_RefusesStaleCheckpointWhenRetentionEmptiedStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	all := []types.ChangeEvent{testEvent(10), testEvent(20), testEvent(30)}
	runUntilPublished(t, js, cfg, newFakeSource(all...), []uint64{10, 20, 30})

	// The run crashed before checkpointing its last event, then retention emptied the stream. The
	// checkpoint can't say which events followed it, so republishing from it could repeat events.
	setProgress(t, js, cfg, 20, 2)
	if err := js.PurgeStream(cfg.eventStream); err != nil {
		t.Fatalf("PurgeStream(): %v", err)
	}
	expectRunError(t, cfg, newFakeSource(all...), "no checkpoint for that sequence")

	// The operator decides where to resume.
	override := testConfig(t, url, "--timestamp-last=30")
	runUntilPublished(t, js, override, newFakeSource(append(all, testEvent(40))...), []uint64{40})
}

func TestRun_RefusesCheckpointFromAnEarlierStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	runUntilPublished(t, js, cfg, newFakeSource(testEvent(10)), []uint64{10})
	earlier, _, err := readProgress(js, cfg)
	if err != nil {
		t.Fatalf("readProgress(): %v", err)
	}

	// The stream is recreated and restarted after timestamp 100. Its first event lands at sequence 1,
	// like event 10 did in the earlier stream. The run crashes before checkpointing, so the earlier
	// checkpoint remains, and then retention empties the new stream.
	if err := js.DeleteStream(cfg.eventStream); err != nil {
		t.Fatalf("DeleteStream(): %v", err)
	}
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=100"), newFakeSource(testEvent(10), testEvent(110)), []uint64{110})
	writeCheckpoint(t, js, cfg, earlier)
	if err := js.PurgeStream(cfg.eventStream); err != nil {
		t.Fatalf("PurgeStream(): %v", err)
	}

	// The sequences match, but the checkpoint belongs to the earlier stream: resuming after event 10
	// would append events consumers already read after event 110.
	expectRunError(t, cfg, newFakeSource(testEvent(10), testEvent(20), testEvent(110)), "no checkpoint for that sequence of this stream")
}

func TestRun_TimestampLastAppliesOnlyWithoutAPosition(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	source := func(timestamps ...uint64) *fakeSource {
		events := newFakeSource()
		for _, timestamp := range timestamps {
			events.add(testEvent(timestamp))
		}
		return events
	}

	// A new stream starts after the given timestamp.
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=20"), source(10, 20, 30, 40), []uint64{30, 40})

	// Once the stream holds events, the flag is ignored: skipping ahead would change which event
	// belongs at each stream position under messages an earlier instance may still have in flight.
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=60"), source(10, 20, 30, 40, 50, 70), []uint64{30, 40, 50, 70})

	// After retention empties the stream, a leftover flag doesn't republish history either: the
	// checkpoint matches the stream's last sequence, so publishing continues from it.
	if err := js.PurgeStream(testConfig(t, url).eventStream); err != nil {
		t.Fatalf("PurgeStream(): %v", err)
	}
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=0"), source(10, 20, 30, 40, 50, 70, 80), []uint64{80})
}

func TestRun_StopsWhenAnotherWriterAppendsToStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	source := newFakeSource(testEvent(10))
	_, result := startRun(t, cfg, source)
	awaitStream(t, js, cfg.eventStream, []uint64{10})

	foreign, err := json.Marshal(map[string]string{"not": "an event"})
	if err != nil {
		t.Fatalf("json.Marshal(): %v", err)
	}
	if _, err := js.Publish(cfg.subjectForEvent(1, "single_phase"), foreign); err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	source.add(testEvent(20))

	// The publish after the foreign write is rejected. Resuming finds the stream no longer ends with
	// an event this publisher wrote, and stops rather than guess.
	err = awaitResult(t, result, 10*time.Second)
	if err == nil || !strings.Contains(err.Error(), "did not write") {
		t.Fatalf("run() error = %v, want it to refuse a stream another writer appended to", err)
	}
	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		t.Fatalf("StreamInfo(): %v", err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stream messages = %d, want 2 (event 10 and the foreign message, nothing after it)", info.State.Msgs)
	}
}

func TestRun_StopsOnPermanentPublishError(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	// A stream whose message size limit is below an event's size rejects every publish. Retrying
	// can't fix that.
	stream := desiredEventStreamConfig(cfg)
	stream.MaxMsgSize = 64
	if _, err := js.AddStream(stream); err != nil {
		t.Fatalf("AddStream(): %v", err)
	}

	expectRunError(t, cfg, newFakeSource(testEvent(10)), "maximum")
}

func TestPublisher_RefusesToLeaveAGap(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)
	if err := ensureEventStream(js, cfg, false); err != nil {
		t.Fatalf("ensureEventStream(): %v", err)
	}

	// The publisher believes an earlier event was stored at sequence 1, but that message was lost and
	// the stream is empty. Later events must not be stored after the gap.
	p := newPublisher(js, cfg, 1, cfg.maxInFlight())
	err := p.publish(context.Background(), []types.ChangeEvent{testEvent(20), testEvent(30)})
	if err == nil || !strings.Contains(err.Error(), "no longer ends at sequence 1") {
		t.Fatalf("publish() error = %v, want it to refuse to leave a gap", err)
	}

	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		t.Fatalf("StreamInfo(): %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("stream messages = %d, want 0", info.State.Msgs)
	}
}

func TestRun_RefusesToResumeAfterTheLastEventWasDeleted(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url, "--timestamp-last=0")

	events := []types.ChangeEvent{testEvent(10), testEvent(20)}
	runUntilPublished(t, js, cfg, newFakeSource(events...), []uint64{10, 20})
	if err := js.DeleteMsg(cfg.eventStream, 2); err != nil {
		t.Fatalf("DeleteMsg(): %v", err)
	}

	// Resuming after event 10 would republish event 20, and a leftover --timestamp-last=0 would
	// republish both. Neither is safe without knowing what consumers received.
	expectRunError(t, cfg, newFakeSource(events...), "was deleted")
}

func TestRun_PublishesInOrderOnReplicatedStream(t *testing.T) {
	// Not parallel: each runs a 3-node cluster, and two at once alongside the parallel tests
	// overloaded a 2-CPU CI runner until one run stalled.
	url := startJetStreamCluster(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url, "--stream-replicas=3", "--kv-replicas=3", "--publish-async-max-pending=64", "--event-count-max=250")

	events := testEvents(1000, 10)
	runUntilPublished(t, js, cfg, newFakeSource(events...), timestampsOf(events))
}

func TestRun_RecoversInOrderAcrossStreamLeaderChanges(t *testing.T) {
	// Not parallel: each runs a 3-node cluster, and two at once alongside the parallel tests
	// overloaded a 2-CPU CI runner until one run stalled.
	url := startJetStreamCluster(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url,
		"--stream-replicas=3", "--kv-replicas=3", "--publish-async-max-pending=16",
		"--event-count-max=50", "--publish-ack-timeout=2s")

	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect(): %v", err)
	}
	defer conn.Close()

	// Before every fourth batch, ask the stream to elect a new leader, so the batch is published while
	// the election is in progress and fails. The publisher must recover by itself, in process.
	var batches, stepdowns atomic.Int32
	source := newFakeSource(testEvents(1000, 10)...)
	source.beforeBatch = func() {
		if batches.Add(1)%4 != 0 {
			return
		}
		resp, err := conn.Request("$JS.API.STREAM.LEADER.STEPDOWN."+cfg.eventStream, nil, time.Second)
		if err == nil && strings.Contains(string(resp.Data), `"success":true`) {
			stepdowns.Add(1)
		}
	}

	_, result := startRun(t, cfg, source)
	awaitStreamWhileRunning(t, js, cfg.eventStream, timestampsOf(source.events), result)
	select {
	case err := <-result:
		t.Fatalf("run stopped during leader changes: %v", err)
	default:
	}
	if n := stepdowns.Load(); n < 3 {
		t.Fatalf("only %d stream leader changes happened while publishing; want at least 3", n)
	}
	// A batch is fetched again only when publishing it failed and the publisher resumed.
	if n := source.refetchCount(); n == 0 {
		t.Fatalf("no publish failed during %d leader changes, so recovery wasn't exercised", stepdowns.Load())
	}
	t.Logf("published %d events across %d stream leader changes and %d recoveries",
		len(source.events), stepdowns.Load(), source.refetchCount())
}

func TestIsTransient(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"wrong last sequence", &nats.APIError{Code: 400, ErrorCode: nats.JSErrCodeStreamWrongLastSequence}, true},
		{"temporarily unavailable", &nats.APIError{Code: 503, ErrorCode: 10008}, true},
		{"inbound queue full", &nats.APIError{Code: 429, ErrorCode: 10167}, true},
		{"duplicate in process", &nats.APIError{Code: 409, ErrorCode: 10158}, true},
		{"stream offline", &nats.APIError{Code: 500, ErrorCode: 10118}, true},
		{"ack timeout", nats.ErrAsyncPublishTimeout, true},
		{"no responders", nats.ErrNoResponders, true},
		{"reconnecting", nats.ErrReconnectBufExceeded, true},
		{"message too large", &nats.APIError{Code: 400, ErrorCode: 10054}, false},
		{"stream sealed", &nats.APIError{Code: 400, ErrorCode: 10109}, false},
		{"connection closed", nats.ErrConnectionClosed, false},
		{"cannot resume", errCannotResume, false},
	} {
		if got := isTransient(fmt.Errorf("wrapped: %w", tc.err)); got != tc.want {
			t.Errorf("isTransient(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestPublishWindow_SerialisesReplicatedStreamsOnOldServers(t *testing.T) {
	t.Parallel()

	replicated := config{publishMode: publishModeAsync, publishAsyncMaxPending: 64, streamReplicas: 3}
	single := config{publishMode: publishModeAsync, publishAsyncMaxPending: 64, streamReplicas: 1}
	for _, tc := range []struct {
		cfg     config
		version string
		want    int
	}{
		{replicated, "2.12.15", 1},
		{replicated, "2.15.0", 64},
		{single, "2.12.15", 64},
	} {
		if got := publishWindow(tc.cfg, tc.version); got != tc.want {
			t.Errorf("publishWindow(replicas=%d, %s) = %d, want %d", tc.cfg.streamReplicas, tc.version, got, tc.want)
		}
	}
}

func TestServerAtLeast(t *testing.T) {
	t.Parallel()

	for version, want := range map[string]bool{
		"2.12.15": false, "2.13.0": false, "2.14.0": true, "2.15.0": true, "3.0.0": true, "": false, "dev": false,
	} {
		if got := serverAtLeast(version, 2, 14); got != want {
			t.Errorf("serverAtLeast(%q, 2, 14) = %v, want %v", version, got, want)
		}
	}
}
