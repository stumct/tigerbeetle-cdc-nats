package cdcnats

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestParseConfig_DefaultClusterScopedResources(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--cluster-id=42",
		"--addresses=127.0.0.1:3000",
	}, "test-version")
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}

	if got, want := cfg.eventStream, "TB_CDC_EVENTS_42"; got != want {
		t.Fatalf("event stream = %q, want %q", got, want)
	}

	if got, want := cfg.progressBucket, "TB_CDC_PROGRESS_42"; got != want {
		t.Fatalf("progress bucket = %q, want %q", got, want)
	}

	if got, want := cfg.lockBucket, "TB_CDC_LOCK_42"; got != want {
		t.Fatalf("lock bucket = %q, want %q", got, want)
	}

	if got, want := cfg.subjectForEvent(7, "single_phase"), "tigerbeetle.cdc.7.single_phase"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}

	if got, want := cfg.publishMode, publishModeAsync; got != want {
		t.Fatalf("publish mode = %q, want %q", got, want)
	}

	if got, want := cfg.streamStorage, nats.FileStorage; got != want {
		t.Fatalf("stream storage = %v, want %v", got, want)
	}

	if got, want := cfg.kvStorage, nats.FileStorage; got != want {
		t.Fatalf("kv storage = %v, want %v", got, want)
	}
}

func TestParseConfig_ExplicitResourceNamesAndModes(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--cluster-id=99",
		"--addresses=127.0.0.1:3000",
		"--stream=my_stream",
		"--progress-bucket=my_progress",
		"--lock-bucket=my_lock",
		"--subject-mode=single",
		"--subject=my.subject",
		"--stream-storage=memory",
		"--kv-storage=memory",
		"--publish-mode=sync",
	}, "test-version")
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}

	if got, want := cfg.eventStream, "my_stream"; got != want {
		t.Fatalf("event stream = %q, want %q", got, want)
	}

	if got, want := cfg.progressBucket, "my_progress"; got != want {
		t.Fatalf("progress bucket = %q, want %q", got, want)
	}

	if got, want := cfg.lockBucket, "my_lock"; got != want {
		t.Fatalf("lock bucket = %q, want %q", got, want)
	}

	if got, want := cfg.subjectForEvent(7, "single_phase"), "my.subject"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}

	if got, want := cfg.streamStorage, nats.MemoryStorage; got != want {
		t.Fatalf("stream storage = %v, want %v", got, want)
	}

	if got, want := cfg.kvStorage, nats.MemoryStorage; got != want {
		t.Fatalf("kv storage = %v, want %v", got, want)
	}

	if got, want := cfg.publishMode, publishModeSync; got != want {
		t.Fatalf("publish mode = %q, want %q", got, want)
	}
}

func TestParseConfig_InvalidPublishMode(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--cluster-id=42",
		"--addresses=127.0.0.1:3000",
		"--publish-mode=fast",
	}, "test-version")
	if err == nil {
		t.Fatalf("parseConfig() error = nil, want non-nil")
	}
}
