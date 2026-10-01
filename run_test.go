package cdcnats

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRun_PublishesEventsAndCheckpoints(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	_, result := startRun(t, cfg, newFakeSource(testEvent(10), testEvent(20), testEvent(30)))

	eventually(t, 5*time.Second, "progress to reach the last event", func() bool {
		kv, err := js.KeyValue(cfg.progressBucket)
		if err != nil {
			return false
		}
		entry, err := kv.Get(cfg.progressKey())
		if err != nil {
			return false
		}
		var progress progressRecord
		return json.Unmarshal(entry.Value(), &progress) == nil && progress.Timestamp == 30
	})

	info, err := js.StreamInfo(cfg.eventStream)
	if err != nil {
		t.Fatalf("StreamInfo(): %v", err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("stream messages = %d, want 3", info.State.Msgs)
	}

	select {
	case err := <-result:
		t.Fatalf("run exited early: %v", err)
	default:
	}
}

func TestRun_StopsWhenLockIsLost(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	_, result := startRun(t, cfg, newFakeSource())
	eventually(t, 5*time.Second, "run to take the lock", func() bool { return lockOwner(t, js, cfg) != "" })

	kv, err := js.KeyValue(cfg.lockBucket)
	if err != nil {
		t.Fatalf("KeyValue(): %v", err)
	}
	if err := kv.Delete(cfg.lockKey()); err != nil {
		t.Fatalf("Delete(): %v", err)
	}
	if _, err := kv.Create(cfg.lockKey(), []byte(`{"owner":"other"}`)); err != nil {
		t.Fatalf("Create(): %v", err)
	}

	err = awaitResult(t, result, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "lost lock") {
		t.Fatalf("run() error = %v, want lost lock", err)
	}
	if owner := lockOwner(t, js, cfg); owner != "other" {
		t.Fatalf("lock owner after run exited = %q, want the new holder to keep it", owner)
	}
}

func TestRun_ShutsDownWhileTigerBeetleIsUnreachable(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	source := newFakeSource()
	source.unreachable = true
	cancel, result := startRun(t, cfg, source)
	select {
	case <-source.blocked:
	case <-time.After(5 * time.Second):
		t.Fatalf("run never queried TigerBeetle")
	}

	cancel()
	if err := awaitResult(t, result, 5*time.Second); err != nil {
		t.Fatalf("run() error = %v, want nil on shutdown", err)
	}
	if owner := lockOwner(t, js, cfg); owner != "" {
		t.Fatalf("lock owner after shutdown = %q, want released", owner)
	}
}

func TestRun_ShutsDownWhileWaitingForLock(t *testing.T) {
	t.Parallel()
	url := startJetStream(t)
	js := connectJetStream(t, url)
	cfg := testConfig(t, url)

	_, first := startRun(t, cfg, newFakeSource())
	eventually(t, 5*time.Second, "first run to take the lock", func() bool { return lockOwner(t, js, cfg) != "" })

	cancel, second := startRun(t, cfg, newFakeSource())
	time.Sleep(200 * time.Millisecond)
	cancel()
	if err := awaitResult(t, second, 5*time.Second); err != nil {
		t.Fatalf("waiting run() error = %v, want nil on shutdown", err)
	}

	select {
	case err := <-first:
		t.Fatalf("lock holder exited: %v", err)
	default:
	}
}
