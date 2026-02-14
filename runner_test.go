package cdcnats

import (
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
