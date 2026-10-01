package cdcnats

import (
	"context"
	"encoding/json"
	"strings"
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
	awaitStream(t, js, cfg.eventStream, want)
	eventually(t, 10*time.Second, "the checkpoint to record the last event", func() bool {
		progress, found, err := readProgress(js, cfg)
		return err == nil && found && progress.Timestamp == want[len(want)-1]
	})
	cancel()
	if err := awaitResult(t, result, 5*time.Second); err != nil {
		t.Fatalf("run() error = %v", err)
	}
}

// setProgress overwrites the progress checkpoint, as a run that stopped at another point would.
func setProgress(t *testing.T, js nats.JetStreamContext, cfg config, timestamp uint64, streamSeq uint64) {
	t.Helper()

	kv, err := js.KeyValue(cfg.progressBucket)
	if err != nil {
		t.Fatalf("KeyValue(): %v", err)
	}
	if err := writeProgress(kv, cfg, timestamp, streamSeq); err != nil {
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

func TestRun_TimestampLastOnlyMovesForwardInAStreamWithEvents(t *testing.T) {
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

	// Restarting with the flag still set resumes from the stream, not from the flag.
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=20"), source(10, 20, 30, 40, 50), []uint64{30, 40, 50})

	// A later timestamp skips ahead.
	runUntilPublished(t, js, testConfig(t, url, "--timestamp-last=70"), source(10, 20, 30, 40, 50, 60, 80), []uint64{30, 40, 50, 80})
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
	p := newPublisher(js, cfg, 1)
	err := p.publish(context.Background(), []types.ChangeEvent{testEvent(20), testEvent(30)})
	if err == nil || !strings.Contains(err.Error(), "no longer ends with the event before this one") {
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

func TestPublisher_RefusesToFollowAnotherWritersMessage(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)
	if err := ensureEventStream(js, cfg, false); err != nil {
		t.Fatalf("ensureEventStream(): %v", err)
	}

	// The publisher read the stream when it ended with event 10 at sequence 1. Before its batch
	// arrives, another message lands at sequence 2.
	p := newPublisher(js, cfg, 0)
	if err := p.publish(context.Background(), []types.ChangeEvent{testEvent(10)}); err != nil {
		t.Fatalf("publish(10): %v", err)
	}
	if _, err := js.Publish(cfg.subjectForEvent(1, "single_phase"), []byte("{}")); err != nil {
		t.Fatalf("Publish(): %v", err)
	}

	// Event 20 expects sequence 1 and is rejected. Event 30 expects sequence 2, which the foreign
	// message occupies: only the predecessor's message ID stops event 30 from being stored after it,
	// which would skip event 20 for good.
	err := p.publish(context.Background(), []types.ChangeEvent{testEvent(20), testEvent(30)})
	if err == nil {
		t.Fatalf("publish(20, 30) error = nil, want a fence rejection")
	}

	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		t.Fatalf("StreamInfo(): %v", err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stream messages = %d, want 2 (event 10 and the foreign message)", info.State.Msgs)
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
	cfg := testConfig(t, url,
		"--stream-replicas=3", "--kv-replicas=3", "--publish-async-max-pending=16",
		"--event-count-max=25", "--publish-ack-timeout=2s")

	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect(): %v", err)
	}
	defer conn.Close()

	// stepDown forces the stream to elect a new leader, retrying while an election is in progress.
	stepDown := func() {
		eventually(t, 10*time.Second, "a stream leader change", func() bool {
			resp, err := conn.Request("$JS.API.STREAM.LEADER.STEPDOWN."+cfg.eventStream, nil, time.Second)
			return err == nil && strings.Contains(string(resp.Data), `"success":true`)
		})
	}

	// Release events in chunks and change the stream leader after each one, while the previous
	// chunks are still being published. The publisher must recover by itself, in process.
	source := newFakeSource()
	_, result := startRun(t, cfg, source)
	all := testEvents(1000, 10)
	for chunk := range 10 {
		source.add(all[chunk*100 : (chunk+1)*100]...)
		time.Sleep(20 * time.Millisecond)
		stepDown()
	}

	awaitStream(t, js, cfg.eventStream, timestampsOf(all))
	select {
	case err := <-result:
		t.Fatalf("run stopped during leader changes: %v", err)
	default:
	}
}
