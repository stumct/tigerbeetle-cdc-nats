# tigerbeetle-cdc-nats

Standalone TigerBeetle CDC publisher for NATS JetStream.

It mirrors `tigerbeetle amqp` semantics while taking advantage of JetStream:

- Polls TigerBeetle `GetChangeEvents` with `timestamp_min = last_timestamp + 1`
- Publishes JSON events with portable-number encoding compatibility
- Waits for JetStream publish acknowledgements before writing progress
- Stores progress and lock state in JetStream KV (stateless runner)
- Uses deterministic `Nats-Msg-Id` (`<cluster>/<timestamp>`) for de-duplication

## Delivery semantics

- Delivery is **at-least-once**.
- Progress advances only after event publish acknowledgements complete.
- If the process crashes after event ack but before progress write, events can be replayed.
- JetStream can suppress replay duplicates inside `--dedupe-window` due to deterministic `Nats-Msg-Id`.
- Consumers should still be idempotent using `<cluster>/<timestamp>` as a stable event key.

## Default resource model (cluster-scoped)

For one-cluster-per-stream deployments (recommended), default resource names are derived from `--cluster-id`:

- Stream: `TB_CDC_EVENTS_<cluster>`
- Progress KV bucket: `TB_CDC_PROGRESS_<cluster>`
- Lock KV bucket: `TB_CDC_LOCK_<cluster>`

Default structured event subject:

`tigerbeetle.cdc.<ledger>.<event_type>`

Headers include:

- `event_type`
- `ledger`
- `transfer_code`
- `debit_account_code`
- `credit_account_code`

## Install options

### 1) Go install (recommended)

Short binary name:

```bash
go install github.com/tigerbeetle/tigerbeetle-cdc-nats/cmd/tb-cdc-nats@latest
```

Compatibility binary name:

```bash
go install github.com/tigerbeetle/tigerbeetle-cdc-nats/cmd/tigerbeetle-cdc-nats@latest
```

### 2) Prebuilt release binaries

Download from GitHub Releases and install manually:

- `tb-cdc-nats_<version>_linux_amd64.tar.gz`
- `tb-cdc-nats_<version>_darwin_amd64.tar.gz`
- `tb-cdc-nats_<version>_darwin_arm64.tar.gz`

Each archive includes both executable names:

- `tb-cdc-nats`
- `tigerbeetle-cdc-nats`

Release checksums are published in `SHA256SUMS.txt`.

Example install (Linux amd64):

```bash
VERSION=v0.1.0
ASSET="tb-cdc-nats_${VERSION}_linux_amd64.tar.gz"
BASE_URL="https://github.com/tigerbeetle/tigerbeetle-cdc-nats/releases/download/${VERSION}"

curl -L -O "${BASE_URL}/${ASSET}"
curl -L -O "${BASE_URL}/SHA256SUMS.txt"
grep " ${ASSET}$" SHA256SUMS.txt | sha256sum -c -
tar -xzf "${ASSET}"
install -m 0755 tb-cdc-nats /usr/local/bin/tb-cdc-nats
```

### 3) Docker image

Published to GHCR on tags:

```bash
docker run --rm ghcr.io/<owner>/tigerbeetle-cdc-nats:latest --help
```

For runtime usage, pass your normal flags and networking configuration (for example, `--nats-url` and `--addresses`) to the container entrypoint.

## Quick start

```bash
go run ./cmd/tb-cdc-nats \
  --cluster-id=0 \
  --addresses=127.0.0.1:3000 \
  --nats-url=nats://127.0.0.1:4222
```

## Core flags

TigerBeetle source:

- `--cluster-id`: TigerBeetle cluster ID (u128 decimal)
- `--addresses`: comma-separated TigerBeetle replica addresses
- `--event-count-max`: max events per `GetChangeEvents` request
- `--idle-interval-ms`: poll interval while idle
- `--requests-per-second-limit`: throttle only `GetChangeEvents` requests
- `--timestamp-last`: override stored progress on startup

JetStream provisioning and retention:

- `--provision`: create missing stream/KV buckets (default: true)
- `--stream-update`: update mismatched stream config (requires `--provision=true`)
- `--stream`: override stream name
- `--stream-replicas`: stream replica count
- `--stream-storage`: `file` or `memory`
- `--stream-max-age`: retention age (`0` = unlimited)
- `--stream-max-bytes`: retention bytes (`-1` = unlimited)
- `--dedupe-window`: JetStream de-duplication window
- `--progress-bucket`: override progress KV bucket name
- `--lock-bucket`: override lock KV bucket name
- `--kv-replicas`: KV replica count
- `--kv-storage`: `file` or `memory`
- `--lock-ttl`: lock entry TTL
- `--lock-refresh`: lock refresh interval

Publishing behavior:

- `--publish-mode`: `async` (default) or `sync`
- `--publish-async-max-pending`: max in-flight async publish requests
- `--publish-ack-timeout`: publish acknowledgement timeout
- `--progress-every-events`: checkpoint progress every N published events (`0` = once per fetched batch)

Subject routing:

- `--subject-mode=structured` (default) with `--subject-prefix`
- `--subject-mode=single` with `--subject`

## Operational notes

- Lock acquisition failure includes lock holder metadata (`owner`, `host`, `pid`, `version`, `updated_at`) when available.
- The lock bucket must have TTL enabled, and the progress bucket must have TTL disabled.
- Stream and KV configuration mismatches fail fast with actionable error messages.

## Testing

Unit tests:

```bash
go test ./...
```

Integration test (local binaries):

```bash
TB_CDC_INTEGRATION=1 go test -run TestIntegration_CDCResumeWithJetStreamState -v
```

Optional binary overrides:

- `NATS_SERVER_BIN=/path/to/nats-server`
- `TIGERBEETLE_BIN=/path/to/tigerbeetle`

For externally managed test services, the integration test also supports:

- `TB_CDC_EXTERNAL_SERVICES=1`
- `TB_CDC_EXTERNAL_NATS_URL`
- `TB_CDC_EXTERNAL_CLUSTER_ID`
- `TB_CDC_EXTERNAL_ADDRESSES`

Containerized integration test:

```bash
./scripts/integration-test-containers.sh
```

Optional container test overrides:

- `TB_CDC_NATS_PORT` (default: `14222`)
- `TB_CDC_TIGERBEETLE_PORT` (default: `13000`)
- `TB_CDC_CLUSTER_ID` (default: `0`)
- `TB_CDC_TIGERBEETLE_TAG` (default: `latest`)

## CI

GitHub Actions workflow `/.github/workflows/tests.yml` runs:

- unit tests (`go test ./...`)
- containerized integration tests (`./scripts/integration-test-containers.sh`)

Tag-based workflow `/.github/workflows/release.yml` publishes:

- release binaries + `SHA256SUMS.txt`
- multi-arch Docker image to `ghcr.io/<owner>/tigerbeetle-cdc-nats`

## License

Apache-2.0. See `LICENSE`.
