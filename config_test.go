package cdcnats

import (
	"strings"
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

	if got, want := cfg.subjectForEvent(7, "single_phase"), "tigerbeetle.cdc.42.7.single_phase"; got != want {
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

func TestParseConfig_RejectsInvalidCombinations(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"dedupe window longer than max age", []string{"--stream-max-age=1m"}, "--dedupe-window"},
		{"dedupe window too short", []string{"--dedupe-window=10ms"}, "--dedupe-window"},
		{"wildcard subject prefix", []string{"--subject-prefix=tb.*"}, "--subject-prefix"},
		{"empty subject token", []string{"--subject-prefix=tb..cdc"}, "--subject-prefix"},
		{"wildcard single subject", []string{"--subject-mode=single", "--subject=tb.>"}, "--subject"},
		{"dotted stream name", []string{"--stream=tb.events"}, "--stream"},
		{"dotted bucket name", []string{"--progress-bucket=tb.progress"}, "--progress-bucket"},
		{"shared buckets", []string{"--progress-bucket=SAME", "--lock-bucket=SAME"}, "must differ"},
		{"timestamp beyond TigerBeetle range", []string{"--timestamp-last=9223372036854775807"}, "--timestamp-last"},
		{"creds and nkey together", []string{"--nats-creds=a.creds", "--nats-nkey=b.nk"}, "--nats-creds"},
		{"client cert without key", []string{"--nats-tls-cert=client.pem"}, "--nats-tls-key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			args := append([]string{"--cluster-id=42", "--addresses=127.0.0.1:3000"}, tc.args...)
			_, err := parseConfig(args, "test-version")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("parseConfig(%v) error = %v, want it to mention %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestValidateLiteralSubject_AllowsWildcardCharactersInsideTokens(t *testing.T) {
	t.Parallel()

	for _, subject := range []string{"foo*bar", "a.b>c.d", "tigerbeetle.cdc"} {
		if err := validateLiteralSubject(subject); err != nil {
			t.Errorf("validateLiteralSubject(%q) error = %v, want nil", subject, err)
		}
	}
}

func TestConnectNATS_KeepsCredentialsOutOfURLErrors(t *testing.T) {
	t.Parallel()

	for _, natsURL := range []string{"nats://alice:s3cret@host:badport", "nats://s3cret@host:badport"} {
		_, err := connectNATS(config{natsURL: natsURL})
		if err == nil || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("connectNATS(%q) error = %v, want an error without the credential", natsURL, err)
		}
	}
}

func TestParseConfig_AcceptsDedupeWindowWithinMaxAge(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--cluster-id=42",
		"--addresses=127.0.0.1:3000",
		"--stream-max-age=1m",
		"--dedupe-window=30s",
		"--nats-tls-cert=client.pem",
		"--nats-tls-key=client.key",
	}, "test-version")
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
}

func TestRedactURLs(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]string{
		"nats://127.0.0.1:4222":                          "nats://127.0.0.1:4222",
		"nats://alice:s3cret@host:4222":                  "nats://[redacted]@host:4222",
		"tls://token@host:4222,nats://bob:pw@other:4222": "tls://[redacted]@host:4222,nats://[redacted]@other:4222",
		"user:p@ss@host:4222":                            "[redacted]@host:4222",
	} {
		if got := redactURLs(input); got != want {
			t.Errorf("redactURLs(%q) = %q, want %q", input, got, want)
		}
	}
}
