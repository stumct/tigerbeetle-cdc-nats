package cdcnats

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

// runUntilPublished runs the publisher until the stream holds exactly want, then stops it cleanly.
func runUntilPublished(t *testing.T, js nats.JetStreamContext, cfg config, source *fakeSource, want []uint64) {
	t.Helper()

	cancel, result := startRun(t, cfg, source)
	awaitStream(t, js, cfg.eventStream, want)
	cancel()
	if err := awaitResult(t, result, 5*time.Second); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

// setProgress overwrites the progress checkpoint, as a run that stopped at another point would.
func setProgress(t *testing.T, js nats.JetStreamContext, cfg config, timestamp uint64) {
	t.Helper()

	kv, err := js.KeyValue(cfg.progressBucket)
	if err != nil {
		t.Fatalf("KeyValue(): %v", err)
	}
	if err := writeProgress(kv, cfg, timestamp); err != nil {
		t.Fatalf("writeProgress(): %v", err)
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

	source := newFakeSource(testEvent(10), testEvent(20), testEvent(30))
	runUntilPublished(t, js, cfg, source, []uint64{10, 20, 30})

	// The previous run crashed after publishing but before checkpointing, long enough ago that the
	// dedupe window has passed.
	setProgress(t, js, cfg, 10)
	time.Sleep(200 * time.Millisecond)

	source.add(testEvent(40))
	runUntilPublished(t, js, cfg, newFakeSource(source.events...), []uint64{10, 20, 30, 40})
}

func TestRun_RepublishesEventsMissingFromStreamWhenCheckpointIsAhead(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	runUntilPublished(t, js, cfg, newFakeSource(testEvent(10), testEvent(20)), []uint64{10, 20})

	// The checkpoint survived a NATS failure that lost events 30 and 40.
	setProgress(t, js, cfg, 40)

	all := newFakeSource(testEvent(10), testEvent(20), testEvent(30), testEvent(40))
	runUntilPublished(t, js, cfg, all, []uint64{10, 20, 30, 40})
}

func TestRun_RefusesToResumeIntoRecreatedStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	events := []uint64{10, 20}
	source := newFakeSource(testEvent(10), testEvent(20))
	runUntilPublished(t, js, cfg, source, events)

	if err := js.DeleteStream(cfg.eventStream); err != nil {
		t.Fatalf("DeleteStream(): %v", err)
	}

	_, result := startRun(t, cfg, newFakeSource(source.events...))
	err := awaitResult(t, result, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "recreated") {
		t.Fatalf("run() error = %v, want recreated stream error", err)
	}

	// --timestamp-last=0 is the explicit choice to republish everything into the new stream.
	override := testConfig(t, url, "--timestamp-last=0")
	runUntilPublished(t, js, override, newFakeSource(source.events...), events)
}

func TestRun_ResumesFromCheckpointWhenRetentionEmptiedStream(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	source := newFakeSource(testEvent(10), testEvent(20))
	runUntilPublished(t, js, cfg, source, []uint64{10, 20})

	if err := js.PurgeStream(cfg.eventStream); err != nil {
		t.Fatalf("PurgeStream(): %v", err)
	}

	source.add(testEvent(30))
	runUntilPublished(t, js, cfg, newFakeSource(source.events...), []uint64{30})
}

func TestRun_TimestampLastOnlyMovesForward(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)

	all := []uint64{10, 20, 30, 40, 50}
	source := func() *fakeSource {
		events := newFakeSource()
		for _, timestamp := range all {
			events.add(testEvent(timestamp))
		}
		return events
	}

	// Starting a new stream after timestamp 20 skips earlier events.
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=20"), source(), []uint64{30, 40, 50})

	// A restart that still passes the flag must not republish anything.
	cancel, result := startRun(t, testConfig(t, url, "--timestamp-last=20"), source())
	time.Sleep(300 * time.Millisecond)
	cancel()
	if err := awaitResult(t, result, 5*time.Second); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if got := streamTimestamps(t, js, testConfig(t, url).eventStream); len(got) != 3 {
		t.Fatalf("stream timestamps after restart = %v, want [30 40 50]", got)
	}
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

func TestRun_PublishesInOrderOnReplicatedStream(t *testing.T) {
	t.Parallel()
	url := startJetStreamCluster(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url, "--stream-replicas=3", "--kv-replicas=3", "--publish-async-max-pending=64", "--event-count-max=250")

	events := testEvents(1000, 10)
	runUntilPublished(t, js, cfg, newFakeSource(events...), timestampsOf(events))
}

func TestRun_RecoversInOrderAcrossStreamLeaderChanges(t *testing.T) {
	t.Parallel()
	url := startJetStreamCluster(t)
	js := connectJetStream(t, url)
	// Small, rate-limited batches spread publishing over a few seconds of leader changes.
	cfg := testConfig(t, url,
		"--stream-replicas=3", "--kv-replicas=3", "--publish-async-max-pending=16",
		"--event-count-max=25", "--requests-per-second-limit=40", "--publish-ack-timeout=2s")

	events := testEvents(1500, 10)
	want := timestampsOf(events)

	// published reports whether the stream holds as many messages as there are events. Lookups fail
	// while a leader election is in progress; that counts as not yet.
	published := func() bool {
		info, err := js.StreamInfo(cfg.eventStream)
		return err == nil && info.State.Msgs == uint64(len(want))
	}

	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect(): %v", err)
	}
	defer conn.Close()

	// Keep forcing stream leader elections while publishing.
	var stepdowns atomic.Int32
	stopStepdowns := make(chan struct{})
	stepdownsDone := make(chan struct{})
	go func() {
		defer close(stepdownsDone)
		for {
			select {
			case <-stopStepdowns:
				return
			case <-time.After(400 * time.Millisecond):
			}
			resp, err := conn.Request("$JS.API.STREAM.LEADER.STEPDOWN."+cfg.eventStream, nil, time.Second)
			if err == nil && strings.Contains(string(resp.Data), `"success":true`) {
				stepdowns.Add(1)
			}
		}
	}()

	// The publisher resumes by itself after most leader changes. If a run does stop, restart it like a
	// supervisor would, until every event is published.
	deadline := time.Now().Add(90 * time.Second)
	for restarts := 0; !published(); restarts++ {
		if time.Now().After(deadline) {
			t.Fatalf("not all events published after %d restarts", restarts)
		}

		cancel, result := startRun(t, cfg, newFakeSource(events...))
		for running := true; running; {
			select {
			case err := <-result:
				t.Logf("run %d stopped: %v", restarts, err)
				running = false
			case <-time.After(50 * time.Millisecond):
				if published() {
					cancel()
					<-result
					running = false
				}
			}
		}
	}

	close(stopStepdowns)
	<-stepdownsDone
	if n := stepdowns.Load(); n < 3 {
		t.Fatalf("only %d stream leader changes happened while publishing; the test needs at least 3", n)
	}
	t.Logf("published %d events across %d stream leader changes", len(want), stepdowns.Load())
	awaitStream(t, js, cfg.eventStream, want)
}

func TestPublisher_RefusesToLeaveAGap(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)
	if err := ensureEventStream(js, cfg); err != nil {
		t.Fatalf("ensureEventStream(): %v", err)
	}

	// The publisher believes an earlier event was stored at sequence 1, but that message was lost and
	// the stream is empty. Later events must not be stored after the gap.
	p := newPublisher(js, cfg, 1)
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
