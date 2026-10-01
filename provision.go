package cdcnats

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
)

// desiredEventStreamConfig is the event stream configuration the publisher requires. Message-count
// limits are unlimited: with DiscardOld, a count limit would silently drop events.
func desiredEventStreamConfig(cfg config) *nats.StreamConfig {
	return &nats.StreamConfig{
		Name:              cfg.eventStream,
		Subjects:          cfg.eventStreamSubjects(),
		Retention:         nats.LimitsPolicy,
		Storage:           cfg.streamStorage,
		Replicas:          cfg.streamReplicas,
		Discard:           nats.DiscardOld,
		Duplicates:        cfg.dedupeWindow,
		MaxAge:            cfg.streamMaxAge,
		MaxBytes:          normalizeUnlimited(cfg.streamMaxBytes),
		MaxMsgs:           -1,
		MaxMsgsPerSubject: -1,
	}
}

func desiredProgressKVConfig(cfg config) nats.KeyValueConfig {
	return nats.KeyValueConfig{
		Bucket:      cfg.progressBucket,
		Description: "TigerBeetle CDC progress",
		History:     1,
		Storage:     cfg.kvStorage,
		Replicas:    cfg.kvReplicas,
	}
}

func desiredLockKVConfig(cfg config) nats.KeyValueConfig {
	return nats.KeyValueConfig{
		Bucket:      cfg.lockBucket,
		Description: "TigerBeetle CDC lock",
		History:     1,
		Storage:     cfg.kvStorage,
		Replicas:    cfg.kvReplicas,
		TTL:         cfg.lockTTL,
	}
}

// ensureEventStream creates the event stream if it is missing (when provisioning is enabled) and
// checks that an existing stream matches the required configuration, updating it if
// --stream-update is set.
func ensureEventStream(js nats.JetStreamContext, cfg config) error {
	desired := desiredEventStreamConfig(cfg)

	info, err := js.StreamInfo(desired.Name)
	if errors.Is(err, nats.ErrStreamNotFound) {
		if !cfg.provision {
			return fmt.Errorf("stream %q not found and --provision=false", desired.Name)
		}

		_, createErr := js.AddStream(desired)
		if createErr == nil {
			log.Printf("created stream %q", desired.Name)
			return nil
		}

		// Another instance may have created the stream first. Check what it created.
		info, err = js.StreamInfo(desired.Name)
		if err != nil {
			return fmt.Errorf("create stream %q: %w", desired.Name, createErr)
		}
	}
	if err != nil {
		return fmt.Errorf("lookup stream %q: %w", desired.Name, err)
	}

	mismatches := streamConfigMismatches(info.Config, *desired)
	if len(mismatches) == 0 {
		return nil
	}

	if cfg.provision && cfg.streamUpdate {
		if _, err := js.UpdateStream(desired); err != nil {
			return fmt.Errorf("update stream %q: %w", desired.Name, err)
		}
		log.Printf("updated stream %q to expected CDC configuration", desired.Name)
		return nil
	}

	advice := "rerun with --stream-update (and --provision=true) to apply the expected stream config"
	if !cfg.provision {
		advice = "enable --provision=true and --stream-update, or update the stream manually"
	}

	return fmt.Errorf(
		"stream %q config mismatch: %s; %s",
		desired.Name,
		strings.Join(mismatches, "; "),
		advice,
	)
}

func ensureKV(
	js nats.JetStreamContext,
	desired nats.KeyValueConfig,
	provision bool,
) (nats.KeyValue, error) {
	kv, err := js.KeyValue(desired.Bucket)
	if err == nil {
		if err := validateKVConfig(js, kv, desired); err != nil {
			return nil, err
		}
		return kv, nil
	}

	if !errors.Is(err, nats.ErrBucketNotFound) {
		return nil, fmt.Errorf("lookup kv bucket %q: %w", desired.Bucket, err)
	}

	if !provision {
		return nil, fmt.Errorf("kv bucket %q not found and --provision=false", desired.Bucket)
	}

	kv, err = js.CreateKeyValue(&desired)
	if err != nil {
		if kv, lookupErr := js.KeyValue(desired.Bucket); lookupErr == nil {
			if err := validateKVConfig(js, kv, desired); err != nil {
				return nil, err
			}
			return kv, nil
		}
		return nil, fmt.Errorf("create kv bucket %q: %w", desired.Bucket, err)
	}

	log.Printf("created kv bucket %q", desired.Bucket)
	return kv, nil
}

func validateKVConfig(js nats.JetStreamContext, kv nats.KeyValue, desired nats.KeyValueConfig) error {
	status, err := kv.Status()
	if err != nil {
		return fmt.Errorf("read kv bucket status for %q: %w", desired.Bucket, err)
	}

	mismatches := make([]string, 0, 4)

	if status.History() != int64(desired.History) {
		mismatches = append(mismatches, fmt.Sprintf("history=%d (expected %d)", status.History(), desired.History))
	}

	if status.TTL() != desired.TTL {
		mismatches = append(mismatches, fmt.Sprintf("ttl=%s (expected %s)", status.TTL(), desired.TTL))
	}

	if streamInfo, err := js.StreamInfo(kvStreamName(desired.Bucket)); err == nil {
		// Other retention policies can acknowledge writes without storing them.
		if streamInfo.Config.Retention != nats.LimitsPolicy {
			mismatches = append(mismatches, fmt.Sprintf("retention=%v (expected %v)", streamInfo.Config.Retention, nats.LimitsPolicy))
		}

		if streamInfo.Config.Storage != desired.Storage {
			mismatches = append(
				mismatches,
				fmt.Sprintf(
					"storage=%s (expected %s)",
					storageTypeLabel(streamInfo.Config.Storage),
					storageTypeLabel(desired.Storage),
				),
			)
		}

		if streamInfo.Config.Replicas != desired.Replicas {
			mismatches = append(
				mismatches,
				fmt.Sprintf("replicas=%d (expected %d)", streamInfo.Config.Replicas, desired.Replicas),
			)
		}
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		mismatches = append(mismatches, fmt.Sprintf("unable to inspect kv stream replicas: %v", err))
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("kv bucket %q config mismatch: %s", desired.Bucket, strings.Join(mismatches, "; "))
	}

	return nil
}

func streamConfigMismatches(actual nats.StreamConfig, expected nats.StreamConfig) []string {
	mismatches := make([]string, 0, 8)

	if !stringSlicesEqual(actual.Subjects, expected.Subjects) {
		mismatches = append(
			mismatches,
			fmt.Sprintf("subjects=%v (expected %v)", actual.Subjects, expected.Subjects),
		)
	}

	if actual.Storage != expected.Storage {
		mismatches = append(
			mismatches,
			fmt.Sprintf("storage=%s (expected %s)", storageTypeLabel(actual.Storage), storageTypeLabel(expected.Storage)),
		)
	}

	if actual.Replicas != expected.Replicas {
		mismatches = append(mismatches, fmt.Sprintf("replicas=%d (expected %d)", actual.Replicas, expected.Replicas))
	}

	if actual.Retention != expected.Retention {
		mismatches = append(mismatches, fmt.Sprintf("retention=%v (expected %v)", actual.Retention, expected.Retention))
	}

	if actual.Discard != expected.Discard {
		mismatches = append(mismatches, fmt.Sprintf("discard=%v (expected %v)", actual.Discard, expected.Discard))
	}

	if normalizeUnlimited(actual.MaxBytes) != normalizeUnlimited(expected.MaxBytes) {
		mismatches = append(
			mismatches,
			fmt.Sprintf("max_bytes=%d (expected %d)", actual.MaxBytes, expected.MaxBytes),
		)
	}

	if normalizeUnlimited(actual.MaxMsgs) != normalizeUnlimited(expected.MaxMsgs) {
		mismatches = append(mismatches, fmt.Sprintf("max_msgs=%d (expected %d)", actual.MaxMsgs, expected.MaxMsgs))
	}

	if normalizeUnlimited(actual.MaxMsgsPerSubject) != normalizeUnlimited(expected.MaxMsgsPerSubject) {
		mismatches = append(
			mismatches,
			fmt.Sprintf("max_msgs_per_subject=%d (expected %d)", actual.MaxMsgsPerSubject, expected.MaxMsgsPerSubject),
		)
	}

	if actual.MaxAge != expected.MaxAge {
		mismatches = append(mismatches, fmt.Sprintf("max_age=%s (expected %s)", actual.MaxAge, expected.MaxAge))
	}

	if actual.Duplicates != expected.Duplicates {
		mismatches = append(
			mismatches,
			fmt.Sprintf("duplicate_window=%s (expected %s)", actual.Duplicates, expected.Duplicates),
		)
	}

	return mismatches
}

// normalizeUnlimited maps JetStream's two spellings of "no limit" (0 and -1) to -1.
func normalizeUnlimited(value int64) int64 {
	if value == 0 || value == -1 {
		return -1
	}
	return value
}

func kvStreamName(bucket string) string {
	return "KV_" + bucket
}

func stringSlicesEqual(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
