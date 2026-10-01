# tigerbeetle-cdc-nats

Standalone TigerBeetle CDC publisher for NATS JetStream.

It mirrors `tigerbeetle amqp` semantics while taking advantage of JetStream:

- Polls TigerBeetle `GetChangeEvents` with `timestamp_min = last_timestamp + 1`
- Publishes JSON events with portable-number encoding compatibility
- Appends each event only directly after the one before it, so the stream has no gaps, duplicates or reordering
- Resumes from the last event in the stream; keeps a fallback checkpoint and the single-writer lock in JetStream KV (stateless runner)
- Sets a deterministic `Nats-Msg-Id` (`<cluster>/<timestamp>`) on every event

## Getting started in 5 minutes

1) Make sure these are running:

- TigerBeetle replicas reachable by `--addresses` (example: `127.0.0.1:3000`)
- NATS with JetStream enabled (example: `nats://127.0.0.1:4222`)

2) Install the CLI from GitHub Releases:

```bash
VERSION=v0.1.2
OS=linux   # linux or darwin
ARCH=amd64 # amd64 or arm64
ASSET="tb-cdc-nats_${VERSION}_${OS}_${ARCH}.tar.gz"
BASE_URL="https://github.com/stumct/tigerbeetle-cdc-nats/releases/download/${VERSION}"

curl -L -O "${BASE_URL}/${ASSET}"
curl -L -O "${BASE_URL}/SHA256SUMS.txt"
grep " ${ASSET}$" SHA256SUMS.txt | sha256sum -c -
tar -xzf "${ASSET}"
sudo install -m 0755 tb-cdc-nats /usr/local/bin/tb-cdc-nats
```

If your system does not have `sha256sum` (for example, macOS), use `shasum -a 256` for verification.

3) Start the publisher:

```bash
tb-cdc-nats \
  --cluster-id=0 \
  --addresses=127.0.0.1:3000 \
  --nats-url=nats://127.0.0.1:4222
```

The first run auto-provisions the JetStream stream and KV buckets (unless `--provision=false`).

4) Optional: verify messages are arriving:

```bash
nats --server nats://127.0.0.1:4222 sub 'tigerbeetle.cdc.>'
```

## Delivery semantics

The event stream is an ordered copy of the cluster's change events:

- Events appear in TigerBeetle timestamp order, each at most once, with no gaps.
- Every message carries `Nats-Expected-Last-Sequence`, the stream sequence of the event before it, and publishing always starts from a position read back from the stream. JetStream stores a message only if the stream ends at that sequence. So a lost or rejected message, a stream leader change mid-batch, a late message from an earlier attempt, or a second instance of the publisher cannot leave a gap or reorder events.
- After a failure, and on every start, publishing resumes after the message at the stream's last sequence, read from the stream leader. Crashing between publishing and checkpointing therefore doesn't republish anything, however long the restart takes.
- Lost responses, leader elections, reconnects and rejected messages are retried in-process with backoff. Failures that need an operator, such as a message over the stream's size limit or a sealed stream, stop the publisher.
- The KV progress checkpoint records the last event's timestamp and stream sequence, and when the stream was created. It is a fallback, used only when retention has removed every event from the stream, and only if it matches that stream's last sequence and creation time. Otherwise the publisher stops and asks for `--timestamp-last`.
- Consumers get at-least-once delivery from their JetStream consumer. Use `Nats-Msg-Id` (`<cluster>/<timestamp>`) or the stream sequence as the idempotency key.

Requirements:

- One stream per TigerBeetle cluster, with the publisher as its only writer. A foreign message that lands while a batch is in flight can take the position the next event expects and let it through, skipping the event before it. Enforce this with NATS permissions: allow only the publisher's user to publish to the stream's subjects (for example `tigerbeetle.cdc.<cluster>.>`). The publisher also refuses to resume if the stream's last message isn't one of its events.
- nats-server 2.14 or newer for replicated streams in `async` mode. Older servers ignore a failed write while applying a replicated message, so a pipelined message could be stored in its place. When connected to an older server, the publisher publishes one message at a time. It only sees the server it's connected to, so a cluster running mixed versions, for example mid-upgrade from 2.12, must use `--publish-mode=sync` until every server runs 2.14 or later.
- Durable JetStream storage. NATS acknowledges a write before flushing it to disk, so on a single server a crash or power cut can lose acknowledged events. The publisher republishes them, but consumers may already have seen them. For financial data, use `--stream-replicas=3` on a NATS cluster, or `sync_interval: always` on a single server.
- If the stream is deleted and recreated, the publisher refuses to resume into it, because earlier events would be missing. Start it with `--timestamp-last=0` to republish everything, or a later timestamp to start there.

## Default resource model (cluster-scoped)

Each TigerBeetle cluster gets its own stream. Default names and subjects include `--cluster-id`, so several clusters can share one NATS account:

- Stream: `TB_CDC_EVENTS_<cluster>`
- Progress KV bucket: `TB_CDC_PROGRESS_<cluster>`
- Lock KV bucket: `TB_CDC_LOCK_<cluster>`
- Structured subject: `tigerbeetle.cdc.<cluster>.<ledger>.<event_type>`
- Single-mode subject: `tigerbeetle.cdc.<cluster>`

Headers include:

- `event_type`
- `ledger`
- `transfer_code`
- `debit_account_code`
- `credit_account_code`

## Upgrading from v0.1.x

- Default subjects now include the cluster ID (`tigerbeetle.cdc.<cluster>.<ledger>.<event_type>`). An existing stream fails the startup config check. To keep the old subjects, pass `--subject-prefix=tigerbeetle.cdc` (or `--subject=tigerbeetle.cdc` in single mode). To move to the new ones, run once with `--stream-update` and update consumers' subject filters.
- Publishing resumes from the stream's last event, not the KV checkpoint. No migration is needed.
- `--timestamp-last` only moves the start forward and is safe to leave set. `--progress-every-events` is accepted but has no effect.
- A second instance now waits for the lock instead of exiting.
- An existing stream with `max_msgs` or `max_msgs_per_subject` limits fails the config check, because those limits silently drop events. Run with `--stream-update` to clear them.
- Nothing else may publish to the event stream.

## Install options

### 1) Prebuilt release binaries (recommended)

Download from GitHub Releases and install manually:

- `tb-cdc-nats_<version>_linux_amd64.tar.gz`
- `tb-cdc-nats_<version>_linux_arm64.tar.gz`
- `tb-cdc-nats_<version>_darwin_amd64.tar.gz`
- `tb-cdc-nats_<version>_darwin_arm64.tar.gz`

Each archive includes both executable names:

- `tb-cdc-nats`
- `tigerbeetle-cdc-nats`

Release checksums are published in `SHA256SUMS.txt`.

Example install (Linux amd64):

```bash
VERSION=v0.1.2
ASSET="tb-cdc-nats_${VERSION}_linux_amd64.tar.gz"
BASE_URL="https://github.com/stumct/tigerbeetle-cdc-nats/releases/download/${VERSION}"

curl -L -O "${BASE_URL}/${ASSET}"
curl -L -O "${BASE_URL}/SHA256SUMS.txt"
grep " ${ASSET}$" SHA256SUMS.txt | sha256sum -c -
tar -xzf "${ASSET}"
sudo install -m 0755 tb-cdc-nats /usr/local/bin/tb-cdc-nats
```

### 2) Docker image

Published to GHCR on tags:

```bash
docker run --rm ghcr.io/stumct/tigerbeetle-cdc-nats:latest --help
```

For runtime usage, pass your normal flags and networking configuration (for example, `--nats-url` and `--addresses`) to the container entrypoint.

Example:

```bash
docker run --rm --network host ghcr.io/stumct/tigerbeetle-cdc-nats:latest \
  --cluster-id=0 \
  --addresses=127.0.0.1:3000 \
  --nats-url=nats://127.0.0.1:4222
```

For Docker Desktop (macOS/Windows), use `host.docker.internal` instead of `127.0.0.1`, or run on a shared Docker network.

### 3) Build from source (development)

Requires Go 1.26 or newer and cgo (the TigerBeetle client links a native library). The `toolchain`
line in `go.mod` selects the patched Go release used for builds.

```bash
go build -o tb-cdc-nats ./cmd/tb-cdc-nats
./tb-cdc-nats --help
```

### 4) Go install (latest tag)

```bash
go install github.com/stumct/tigerbeetle-cdc-nats/cmd/tb-cdc-nats@latest
```

## TigerBeetle compatibility

The publisher uses `tigerbeetle-go` v0.16.72. A TigerBeetle cluster accepts clients from a range of
releases that ends at its own release, so the client is kept at the oldest release that supports
change events rather than the newest:

- Supported clusters: TigerBeetle 0.16.72 and newer (CI tests 0.16.72 and 0.17.9).
- Only raise the client version when you also require clusters to run at least that release.

## Core flags

TigerBeetle source:

- `--cluster-id`: TigerBeetle cluster ID (u128 decimal)
- `--addresses`: comma-separated TigerBeetle replica addresses
- `--event-count-max`: max events per `GetChangeEvents` request
- `--idle-interval-ms`: poll interval while idle
- `--requests-per-second-limit`: throttle only `GetChangeEvents` requests
- `--timestamp-last`: publish only events after this timestamp, when the stream has no position to continue from: it's new or recreated, or retention emptied it and the checkpoint doesn't match. Otherwise it's ignored, so it's safe to leave set. To skip ahead in an existing stream, start a new stream.

NATS connection:

- `--nats-url`: NATS server URL, or a comma-separated list. Logs never show credentials embedded in the URL.
- `--nats-creds`: credentials file (user JWT and NKey seed), for decentralized auth or Synadia Cloud
- `--nats-nkey`: NKey seed file (cannot be combined with `--nats-creds`)
- `--nats-tls-ca`: CA file used to verify the server; setting it enables TLS
- `--nats-tls-cert` / `--nats-tls-key`: client certificate and key for mutual TLS

JetStream provisioning and retention:

- `--provision`: create missing stream/KV buckets (default: true)
- `--stream-update`: update mismatched stream config (requires `--provision=true`). The update is applied only once this instance holds the lock, so a standby never changes the stream under the active publisher.
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

- `--publish-mode`: `async` (default) pipelines up to `--publish-async-max-pending` unacknowledged messages; `sync` waits for each acknowledgement
- `--publish-async-max-pending`: max unacknowledged messages in async mode
- `--publish-ack-timeout`: time allowed for each message's acknowledgement. After a transient failure the publisher backs off (0.5s doubling to 30s) and resumes from the stream instead of exiting.
- `--progress-every-events`: deprecated, has no effect

Subject routing:

- `--subject-mode=structured` (default) with `--subject-prefix`
- `--subject-mode=single` with `--subject`

## Metrics

`--metrics-addr=:9464` serves Prometheus metrics at `/metrics` (disabled by default):

| Metric | Meaning |
|---|---|
| `tb_cdc_lock_held` | 1 while this instance holds the lock and publishes |
| `tb_cdc_events_published_total` | events published |
| `tb_cdc_publish_failures_total` | failed publishes or resumes, each retried from the stream |
| `tb_cdc_last_event_timestamp_seconds` | TigerBeetle timestamp of the last published event |
| `tb_cdc_last_poll_timestamp_seconds` | time of the last successful TigerBeetle query |
| `tb_cdc_caught_up` | 1 if the last query found no new events |
| `tb_cdc_build_info{version}` | always 1 |

Suggested alerts (scope `tb_cdc_lock_held` to one cluster's publishers, for example with a `job` label):

- No publisher: `(sum(tb_cdc_lock_held) or vector(0)) == 0` for 2 minutes. The `or vector(0)` keeps it firing when every publisher is down.
- Stalled: `tb_cdc_lock_held == 1 and time() - tb_cdc_last_poll_timestamp_seconds > 60`. This covers TigerBeetle being unreachable, including from the moment the lock was taken.
- Falling behind: `tb_cdc_lock_held == 1 and tb_cdc_caught_up == 0 and time() - tb_cdc_last_event_timestamp_seconds > 300` for 5 minutes. Standbys don't hold the lock, so they never match.
- Repeated failures: `increase(tb_cdc_publish_failures_total[10m]) > 5`.

The endpoint has no authentication. Bind it to a private address.

## Operational notes

- One instance publishes at a time. Others wait for the lock and log the holder (`owner`, `host`, `pid`, `version`, `updated_at`), so you can run a hot standby.
- A standby takes over once the holder releases the lock on shutdown, or within about `--lock-ttl` after the holder dies.
- A holder retries failed lock renewals until the lock is close to expiring. If it loses the lock, it stops publishing, exits non-zero, and leaves the new holder's lock in place.
- `SIGINT`/`SIGTERM` stop the publisher, even while TigerBeetle is unreachable, release the lock and exit 0. A second signal exits immediately.
- The lock bucket must have TTL enabled, and the progress bucket must have TTL disabled.
- Stream and KV configuration mismatches fail fast with actionable error messages. This includes message-count limits on the event stream and non-`limits` retention on the KV buckets, both of which can lose data silently.
- Flags are validated before connecting: subjects must be literal (no `*` or `>` tokens, no empty tokens), stream and bucket names must follow JetStream's naming rules, and `--dedupe-window` must not exceed a non-zero `--stream-max-age`.

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
- `TB_CDC_TIGERBEETLE_TAG` (default: `0.17.9`)

## CI

GitHub Actions workflow `/.github/workflows/tests.yml` runs on pull requests, pushes to `main` and weekly:

- `go vet` and unit tests with the race detector
- `golangci-lint`
- `govulncheck` (the weekly run flags newly published Go vulnerabilities)
- containerized integration tests against TigerBeetle 0.16.72 and 0.17.9
- a Docker image build that checks the binary starts in the runtime image

Tag-based workflow `/.github/workflows/release.yml` reruns the tests, then publishes:

- release binaries for linux and darwin (amd64 and arm64) + `SHA256SUMS.txt`
- multi-arch Docker image to `ghcr.io/stumct/tigerbeetle-cdc-nats`

Tags containing `-` (for example `v0.2.0-rc.1`) are published as pre-releases and do not move `latest`.

## License

Apache-2.0. See `LICENSE`.
