package cdcnats

import (
	"flag"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tigerbeetle/tigerbeetle-go/pkg/types"
)

type subjectMode string

const (
	subjectModeStructured subjectMode = "structured"
	subjectModeSingle     subjectMode = "single"
)

type publishMode string

const (
	publishModeSync  publishMode = "sync"
	publishModeAsync publishMode = "async"
)

const (
	defaultNATSURL                   = "nats://127.0.0.1:4222"
	defaultEventStreamBase           = "TB_CDC_EVENTS"
	defaultProgressBucketBase        = "TB_CDC_PROGRESS"
	defaultLockBucketBase            = "TB_CDC_LOCK"
	defaultSubjectPrefix             = "tigerbeetle.cdc"
	defaultSingleSubject             = "tigerbeetle.cdc"
	defaultLockTTL                   = 30 * time.Second
	defaultLockRefreshInterval       = 10 * time.Second
	defaultDedupeWindow              = 2 * time.Minute
	defaultEventCountMax             = uint32(4096)
	defaultIdleInterval              = time.Second
	defaultProvision                 = true
	defaultStreamReplicas            = 1
	defaultKVReplicas                = 1
	defaultPublishMode               = publishModeAsync
	defaultPublishAckTimeout         = 30 * time.Second
	defaultPublishAsyncPending       = 4096
	maxJetStreamReplicaCount         = 5
	defaultStreamMaxBytes      int64 = -1
	// minDedupeWindow is the smallest duplicate window JetStream accepts.
	minDedupeWindow = 100 * time.Millisecond
	// maxJetStreamNameLength is the longest stream name JetStream accepts.
	maxJetStreamNameLength = 255
	// maxTigerBeetleTimestamp is the largest TigerBeetle timestamp. Timestamps are u63: the top bit is
	// reserved.
	maxTigerBeetleTimestamp uint64 = math.MaxInt64
)

// bucketNamePattern is the KV bucket naming rule enforced by nats.go.
var bucketNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type optionalUint64Flag struct {
	value uint64
	set   bool
}

func (f *optionalUint64Flag) String() string {
	if !f.set {
		return ""
	}
	return strconv.FormatUint(f.value, 10)
}

func (f *optionalUint64Flag) Set(raw string) error {
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fmt.Errorf("must be an unsigned 64-bit integer: %w", err)
	}
	f.value = value
	f.set = true
	return nil
}

type optionalUint32Flag struct {
	value uint32
	set   bool
}

func (f *optionalUint32Flag) String() string {
	if !f.set {
		return ""
	}
	return strconv.FormatUint(uint64(f.value), 10)
}

func (f *optionalUint32Flag) Set(raw string) error {
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return fmt.Errorf("must be an unsigned 32-bit integer: %w", err)
	}
	f.value = uint32(value)
	f.set = true
	return nil
}

type config struct {
	clusterID        types.Uint128
	clusterIDDecimal string
	addresses        []string

	natsURL string
	// Optional NATS authentication and TLS files. nats.go reads them when connecting.
	natsCredsFile   string
	natsNKeyFile    string
	natsTLSCAFile   string
	natsTLSCertFile string
	natsTLSKeyFile  string

	eventStream    string
	streamReplicas int
	streamStorage  nats.StorageType
	streamMaxAge   time.Duration
	streamMaxBytes int64
	dedupeWindow   time.Duration
	provision      bool
	streamUpdate   bool

	progressBucket string
	lockBucket     string
	kvReplicas     int
	kvStorage      nats.StorageType

	lockTTL     time.Duration
	lockRefresh time.Duration

	subjectMode   subjectMode
	subjectPrefix string
	singleSubject string

	publishMode            publishMode
	publishAckTimeout      time.Duration
	publishAsyncMaxPending int

	eventCountMax          uint32
	idleInterval           time.Duration
	requestsPerSecondLimit uint32
	timestampLast          *uint64

	version string
}

// maxInFlight is how many published messages may await acknowledgement at once.
func (c config) maxInFlight() int {
	if c.publishMode == publishModeSync {
		return 1
	}
	return c.publishAsyncMaxPending
}

func (c config) progressKey() string {
	return "progress." + c.clusterIDDecimal
}

func (c config) lockKey() string {
	return "lock." + c.clusterIDDecimal
}

func (c config) eventStreamSubjects() []string {
	switch c.subjectMode {
	case subjectModeStructured:
		return []string{c.subjectPrefix + ".>"}
	case subjectModeSingle:
		return []string{c.singleSubject}
	default:
		panic("invalid subject mode")
	}
}

func (c config) subjectForEvent(ledger uint32, eventType string) string {
	if c.subjectMode == subjectModeSingle {
		return c.singleSubject
	}
	return fmt.Sprintf("%s.%d.%s", c.subjectPrefix, ledger, eventType)
}

func parseConfig(args []string, version string) (config, error) {
	fs := flag.NewFlagSet("tb-cdc-nats", flag.ContinueOnError)

	var cfg config
	var clusterRaw string
	var addressesRaw string
	var subjectModeRaw string
	var publishModeRaw string
	var streamStorageRaw string
	var kvStorageRaw string
	var idleIntervalMS uint
	var eventCountMax uint
	var timestampLast optionalUint64Flag
	var requestsPerSecondLimit optionalUint32Flag

	fs.StringVar(&clusterRaw, "cluster", "", "TigerBeetle cluster ID (u128 decimal)")
	fs.StringVar(&clusterRaw, "cluster-id", "", "TigerBeetle cluster ID (u128 decimal)")
	fs.StringVar(&addressesRaw, "addresses", "", "TigerBeetle replica addresses (comma-separated)")

	fs.StringVar(&cfg.natsURL, "nats-url", defaultNATSURL, "NATS server URL (comma-separated for multiple servers)")
	fs.StringVar(&cfg.natsCredsFile, "nats-creds", "", "NATS credentials file (user JWT and NKey seed)")
	fs.StringVar(&cfg.natsNKeyFile, "nats-nkey", "", "NATS NKey seed file")
	fs.StringVar(&cfg.natsTLSCAFile, "nats-tls-ca", "", "CA certificate file used to verify the NATS server (enables TLS)")
	fs.StringVar(&cfg.natsTLSCertFile, "nats-tls-cert", "", "Client certificate file for NATS mutual TLS (requires --nats-tls-key)")
	fs.StringVar(&cfg.natsTLSKeyFile, "nats-tls-key", "", "Client private key file for NATS mutual TLS (requires --nats-tls-cert)")

	fs.StringVar(&cfg.eventStream, "stream", "", "JetStream stream for CDC events (default: TB_CDC_EVENTS_<cluster>)")
	fs.IntVar(&cfg.streamReplicas, "stream-replicas", defaultStreamReplicas, "JetStream stream replica count (1-5)")
	fs.StringVar(&streamStorageRaw, "stream-storage", "file", "JetStream stream storage: file or memory")
	fs.DurationVar(&cfg.streamMaxAge, "stream-max-age", 0, "JetStream stream retention max age (0 = unlimited)")
	fs.Int64Var(&cfg.streamMaxBytes, "stream-max-bytes", defaultStreamMaxBytes, "JetStream stream max bytes (-1 = unlimited)")
	fs.DurationVar(&cfg.dedupeWindow, "dedupe-window", defaultDedupeWindow, "JetStream de-duplication window")
	fs.BoolVar(&cfg.provision, "provision", defaultProvision, "Create missing JetStream stream/KV buckets")
	fs.BoolVar(&cfg.streamUpdate, "stream-update", false, "Update existing stream config when mismatched (requires --provision=true)")

	fs.StringVar(&cfg.progressBucket, "progress-bucket", "", "KV bucket for CDC progress (default: TB_CDC_PROGRESS_<cluster>)")
	fs.StringVar(&cfg.lockBucket, "lock-bucket", "", "KV bucket for CDC lock (default: TB_CDC_LOCK_<cluster>)")
	fs.IntVar(&cfg.kvReplicas, "kv-replicas", defaultKVReplicas, "KV replica count (1-5)")
	fs.StringVar(&kvStorageRaw, "kv-storage", "file", "KV storage: file or memory")
	fs.DurationVar(&cfg.lockTTL, "lock-ttl", defaultLockTTL, "Lock key TTL")
	fs.DurationVar(&cfg.lockRefresh, "lock-refresh", defaultLockRefreshInterval, "Lock refresh interval")

	fs.StringVar(&subjectModeRaw, "subject-mode", string(subjectModeStructured), "Subject mode: structured or single")
	fs.StringVar(&cfg.subjectPrefix, "subject-prefix", defaultSubjectPrefix, "Prefix for structured subjects")
	fs.StringVar(&cfg.singleSubject, "subject", defaultSingleSubject, "Subject used when --subject-mode=single")

	fs.StringVar(&publishModeRaw, "publish-mode", string(defaultPublishMode), "Publish mode: async or sync")
	fs.IntVar(&cfg.publishAsyncMaxPending, "publish-async-max-pending", defaultPublishAsyncPending, "Max unacknowledged messages in async mode")
	fs.DurationVar(&cfg.publishAckTimeout, "publish-ack-timeout", defaultPublishAckTimeout, "Timeout for each message's JetStream acknowledgement, from when it is sent")

	fs.UintVar(&eventCountMax, "event-count-max", uint(defaultEventCountMax), "Max change events per TigerBeetle request")
	// Kept so existing deployments that pass it still start. Progress now comes from the event stream.
	fs.Uint("progress-every-events", 0, "Deprecated: has no effect")
	fs.UintVar(&idleIntervalMS, "idle-interval-ms", uint(defaultIdleInterval/time.Millisecond), "Polling interval when no events are available")
	fs.Var(&requestsPerSecondLimit, "requests-per-second-limit", "Rate-limit for get_change_events requests")
	fs.Var(&timestampLast, "timestamp-last", "Publish only events after this timestamp, when the stream has no position to continue from")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	if clusterRaw == "" {
		return config{}, fmt.Errorf("--cluster-id is required")
	}

	clusterID, clusterIDDecimal, err := parseUint128Decimal(clusterRaw)
	if err != nil {
		return config{}, fmt.Errorf("invalid --cluster-id: %w", err)
	}

	addresses, err := parseAddresses(addressesRaw)
	if err != nil {
		return config{}, fmt.Errorf("invalid --addresses: %w", err)
	}

	if eventCountMax == 0 || eventCountMax > math.MaxUint32 {
		return config{}, fmt.Errorf("--event-count-max must be in [1, %d]", uint(math.MaxUint32))
	}

	if idleIntervalMS == 0 {
		return config{}, fmt.Errorf("--idle-interval-ms must be greater than zero")
	}

	if cfg.streamReplicas < 1 || cfg.streamReplicas > maxJetStreamReplicaCount {
		return config{}, fmt.Errorf("--stream-replicas must be in [1, %d]", maxJetStreamReplicaCount)
	}

	if cfg.kvReplicas < 1 || cfg.kvReplicas > maxJetStreamReplicaCount {
		return config{}, fmt.Errorf("--kv-replicas must be in [1, %d]", maxJetStreamReplicaCount)
	}

	if cfg.streamMaxAge < 0 {
		return config{}, fmt.Errorf("--stream-max-age must be non-negative")
	}

	if cfg.streamMaxBytes == 0 {
		cfg.streamMaxBytes = -1
	}
	if cfg.streamMaxBytes < -1 {
		return config{}, fmt.Errorf("--stream-max-bytes must be -1 (unlimited) or a positive integer")
	}

	if cfg.lockTTL <= 0 {
		return config{}, fmt.Errorf("--lock-ttl must be greater than zero")
	}

	if cfg.lockRefresh <= 0 {
		return config{}, fmt.Errorf("--lock-refresh must be greater than zero")
	}

	if cfg.lockRefresh >= cfg.lockTTL {
		return config{}, fmt.Errorf("--lock-refresh must be less than --lock-ttl")
	}

	if cfg.dedupeWindow < minDedupeWindow {
		return config{}, fmt.Errorf("--dedupe-window must be at least %s", minDedupeWindow)
	}

	if cfg.streamMaxAge > 0 && cfg.dedupeWindow > cfg.streamMaxAge {
		return config{}, fmt.Errorf(
			"--dedupe-window (%s) must not exceed --stream-max-age (%s)",
			cfg.dedupeWindow,
			cfg.streamMaxAge,
		)
	}

	if cfg.publishAckTimeout <= 0 {
		return config{}, fmt.Errorf("--publish-ack-timeout must be greater than zero")
	}

	if cfg.publishAsyncMaxPending <= 0 {
		return config{}, fmt.Errorf("--publish-async-max-pending must be greater than zero")
	}

	if !cfg.provision && cfg.streamUpdate {
		return config{}, fmt.Errorf("--stream-update requires --provision=true")
	}

	mode := subjectMode(strings.ToLower(strings.TrimSpace(subjectModeRaw)))
	switch mode {
	case subjectModeStructured:
		cfg.subjectPrefix = strings.TrimSuffix(strings.TrimSpace(cfg.subjectPrefix), ".")
		if cfg.subjectPrefix == "" {
			return config{}, fmt.Errorf("--subject-prefix cannot be empty in structured mode")
		}
		if err := validateLiteralSubject(cfg.subjectPrefix); err != nil {
			return config{}, fmt.Errorf("invalid --subject-prefix: %w", err)
		}
	case subjectModeSingle:
		cfg.singleSubject = strings.TrimSpace(cfg.singleSubject)
		if cfg.singleSubject == "" {
			return config{}, fmt.Errorf("--subject cannot be empty in single mode")
		}
		if err := validateLiteralSubject(cfg.singleSubject); err != nil {
			return config{}, fmt.Errorf("invalid --subject: %w", err)
		}
	default:
		return config{}, fmt.Errorf("--subject-mode must be one of: structured, single")
	}

	parsedPublishMode := publishMode(strings.ToLower(strings.TrimSpace(publishModeRaw)))
	switch parsedPublishMode {
	case publishModeAsync, publishModeSync:
	default:
		return config{}, fmt.Errorf("--publish-mode must be one of: async, sync")
	}

	streamStorage, err := parseStorageType(streamStorageRaw)
	if err != nil {
		return config{}, fmt.Errorf("invalid --stream-storage: %w", err)
	}

	kvStorage, err := parseStorageType(kvStorageRaw)
	if err != nil {
		return config{}, fmt.Errorf("invalid --kv-storage: %w", err)
	}

	if cfg.natsURL == "" {
		return config{}, fmt.Errorf("--nats-url cannot be empty")
	}

	if cfg.natsCredsFile != "" && cfg.natsNKeyFile != "" {
		return config{}, fmt.Errorf("--nats-creds and --nats-nkey cannot be used together")
	}

	if (cfg.natsTLSCertFile == "") != (cfg.natsTLSKeyFile == "") {
		return config{}, fmt.Errorf("--nats-tls-cert and --nats-tls-key must be set together")
	}

	cfg.eventStream = strings.TrimSpace(cfg.eventStream)
	if cfg.eventStream == "" {
		cfg.eventStream = clusterScopedResourceName(defaultEventStreamBase, clusterIDDecimal)
	}

	cfg.progressBucket = strings.TrimSpace(cfg.progressBucket)
	if cfg.progressBucket == "" {
		cfg.progressBucket = clusterScopedResourceName(defaultProgressBucketBase, clusterIDDecimal)
	}

	cfg.lockBucket = strings.TrimSpace(cfg.lockBucket)
	if cfg.lockBucket == "" {
		cfg.lockBucket = clusterScopedResourceName(defaultLockBucketBase, clusterIDDecimal)
	}

	if err := validateStreamName(cfg.eventStream); err != nil {
		return config{}, fmt.Errorf("invalid --stream: %w", err)
	}

	if err := validateBucketName(cfg.progressBucket); err != nil {
		return config{}, fmt.Errorf("invalid --progress-bucket: %w", err)
	}

	if err := validateBucketName(cfg.lockBucket); err != nil {
		return config{}, fmt.Errorf("invalid --lock-bucket: %w", err)
	}

	if cfg.progressBucket == cfg.lockBucket {
		return config{}, fmt.Errorf("--progress-bucket and --lock-bucket must differ")
	}

	if requestsPerSecondLimit.set && requestsPerSecondLimit.value == 0 {
		return config{}, fmt.Errorf("--requests-per-second-limit must not be zero")
	}

	cfg.clusterID = clusterID
	cfg.clusterIDDecimal = clusterIDDecimal
	cfg.addresses = addresses
	cfg.subjectMode = mode
	cfg.publishMode = parsedPublishMode
	cfg.streamStorage = streamStorage
	cfg.kvStorage = kvStorage
	cfg.eventCountMax = uint32(eventCountMax)
	cfg.idleInterval = time.Duration(idleIntervalMS) * time.Millisecond
	cfg.version = version

	if requestsPerSecondLimit.set {
		cfg.requestsPerSecondLimit = requestsPerSecondLimit.value
	}

	if timestampLast.set {
		if timestampLast.value >= maxTigerBeetleTimestamp {
			return config{}, fmt.Errorf("--timestamp-last must be less than %d (the largest TigerBeetle timestamp)", maxTigerBeetleTimestamp)
		}
		value := timestampLast.value
		cfg.timestampLast = &value
	}

	return cfg, nil
}

func parseAddresses(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("must not be empty")
	}

	parts := strings.Split(raw, ",")
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		address := strings.TrimSpace(part)
		if address == "" {
			return nil, fmt.Errorf("contains an empty address")
		}
		addresses = append(addresses, address)
	}

	return addresses, nil
}

func parseUint128Decimal(raw string) (types.Uint128, string, error) {
	bigValue := new(big.Int)
	if _, ok := bigValue.SetString(raw, 10); !ok {
		return types.Uint128{}, "", fmt.Errorf("must be a base-10 unsigned integer")
	}

	if bigValue.Sign() < 0 {
		return types.Uint128{}, "", fmt.Errorf("must not be negative")
	}

	if bigValue.BitLen() > 128 {
		return types.Uint128{}, "", fmt.Errorf("must fit in 128 bits")
	}

	return types.BigIntToUint128(*bigValue), bigValue.String(), nil
}

func parseStorageType(raw string) (nats.StorageType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "file":
		return nats.FileStorage, nil
	case "memory", "mem":
		return nats.MemoryStorage, nil
	default:
		return 0, fmt.Errorf("must be one of: file, memory")
	}
}

func storageTypeLabel(storage nats.StorageType) string {
	switch storage {
	case nats.FileStorage:
		return "file"
	case nats.MemoryStorage:
		return "memory"
	default:
		return fmt.Sprintf("storage(%d)", storage)
	}
}

func clusterScopedResourceName(base string, clusterID string) string {
	return base + "_" + clusterID
}

// validateLiteralSubject checks that subject is a NATS subject without wildcards: dot-separated,
// non-empty tokens with no whitespace, none of which is '*' or '>'. Those characters are wildcards
// only as a whole token, so "foo*bar" is a valid literal token.
func validateLiteralSubject(subject string) error {
	if strings.ContainsAny(subject, " \t\r\n\f") {
		return fmt.Errorf("%q must not contain whitespace", subject)
	}
	for _, token := range strings.Split(subject, ".") {
		switch token {
		case "":
			return fmt.Errorf("%q has an empty token", subject)
		case "*", ">":
			return fmt.Errorf("%q must not contain wildcard tokens ('*', '>')", subject)
		}
	}
	return nil
}

// validateStreamName applies JetStream's stream naming rules, so a bad name fails before connecting.
func validateStreamName(name string) error {
	if len(name) > maxJetStreamNameLength {
		return fmt.Errorf("%q is longer than %d characters", name, maxJetStreamNameLength)
	}
	if strings.ContainsAny(name, " \t\r\n\f.*>/\\") {
		return fmt.Errorf("%q must not contain whitespace, '.', '*', '>', '/' or '\\'", name)
	}
	return nil
}

// validateBucketName applies the KV bucket naming rules, including the length of the backing stream.
func validateBucketName(name string) error {
	if !bucketNamePattern.MatchString(name) {
		return fmt.Errorf("%q may only contain letters, digits, '_' and '-'", name)
	}
	return validateStreamName(kvStreamName(name))
}
