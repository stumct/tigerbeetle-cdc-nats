package cdcnats

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestStreamConfigMismatches_None(t *testing.T) {
	t.Parallel()

	actual := nats.StreamConfig{
		Subjects:   []string{"tigerbeetle.cdc.>"},
		Storage:    nats.FileStorage,
		Replicas:   1,
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardOld,
		MaxBytes:   -1,
		MaxAge:     0,
		Duplicates: 2 * time.Minute,
	}

	expected := actual

	mismatches := streamConfigMismatches(actual, expected)
	if len(mismatches) != 0 {
		t.Fatalf("mismatches = %v, want empty", mismatches)
	}
}

func TestStreamConfigMismatches_DetectsDifferences(t *testing.T) {
	t.Parallel()

	actual := nats.StreamConfig{
		Subjects:   []string{"a.>"},
		Storage:    nats.MemoryStorage,
		Replicas:   1,
		Retention:  nats.LimitsPolicy,
		Discard:    nats.DiscardOld,
		MaxBytes:   1024,
		MaxAge:     time.Minute,
		Duplicates: time.Minute,
	}

	expected := nats.StreamConfig{
		Subjects:   []string{"b.>"},
		Storage:    nats.FileStorage,
		Replicas:   3,
		Retention:  nats.WorkQueuePolicy,
		Discard:    nats.DiscardNew,
		MaxBytes:   2048,
		MaxAge:     2 * time.Minute,
		Duplicates: 2 * time.Minute,
	}

	mismatches := streamConfigMismatches(actual, expected)
	if len(mismatches) == 0 {
		t.Fatalf("mismatches = empty, want non-empty")
	}
}

func TestStreamConfigMismatches_DetectsMessageCountLimits(t *testing.T) {
	t.Parallel()

	expected := *desiredEventStreamConfig(config{
		eventStream:   "TB_CDC_EVENTS_7",
		subjectMode:   subjectModeStructured,
		subjectPrefix: "tigerbeetle.cdc",
		streamStorage: nats.FileStorage,
	})

	// JetStream reports "no limit" as -1 or 0; neither is a mismatch.
	unlimited := expected
	unlimited.MaxMsgs, unlimited.MaxMsgsPerSubject = 0, 0
	if mismatches := streamConfigMismatches(unlimited, expected); len(mismatches) != 0 {
		t.Fatalf("mismatches = %v, want none", mismatches)
	}

	limited := expected
	limited.MaxMsgs, limited.MaxMsgsPerSubject = 1000, 1
	mismatches := strings.Join(streamConfigMismatches(limited, expected), "; ")
	if !strings.Contains(mismatches, "max_msgs=1000") || !strings.Contains(mismatches, "max_msgs_per_subject=1") {
		t.Fatalf("mismatches = %q, want max_msgs and max_msgs_per_subject", mismatches)
	}
}

func TestEnsureKV_RejectsBucketThatDoesNotStoreWrites(t *testing.T) {
	t.Parallel()
	js := connectJetStream(t, startJetStream(t))

	// A bucket's backing stream created by hand with interest retention: with no consumers it
	// acknowledges writes without storing them.
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:              "KV_LOCKS",
		Subjects:          []string{"$KV.LOCKS.>"},
		Retention:         nats.InterestPolicy,
		MaxMsgsPerSubject: 1,
		AllowRollup:       true,
		DenyDelete:        true,
	}); err != nil {
		t.Fatalf("AddStream(): %v", err)
	}

	_, err := ensureKV(js, nats.KeyValueConfig{Bucket: "LOCKS", History: 1, Storage: nats.FileStorage, Replicas: 1}, true)
	if err == nil || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("ensureKV() error = %v, want retention mismatch", err)
	}
}
