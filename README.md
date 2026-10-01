# tigerbeetle-cdc-nats

Standalone TigerBeetle CDC publisher for NATS JetStream.

It mirrors `tigerbeetle amqp` semantics while taking advantage of JetStream:

- Polls TigerBeetle `GetChangeEvents` with `timestamp_min = last_timestamp + 1`
- Publishes JSON events with portable-number encoding compatibility
- Waits for JetStream publish acknowledgements before writing progress
- Stores progress and lock state in JetStream KV (stateless runner)
- Uses deterministic `Nats-Msg-Id` (`<cluster>/<timestamp>`) for de-duplication

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
- `--timestamp-last`: override stored progress on startup

NATS connection:

- `--nats-url`: NATS server URL, or a comma-separated list. Logs never show credentials embedded in the URL.
- `--nats-creds`: credentials file (user JWT and NKey seed), for decentralized auth or Synadia Cloud
- `--nats-nkey`: NKey seed file (cannot be combined with `--nats-creds`)
- `--nats-tls-ca`: CA file used to verify the server; setting it enables TLS
- `--nats-tls-cert` / `--nats-tls-key`: client certificate and key for mutual TLS

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
