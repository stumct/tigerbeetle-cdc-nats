#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${TB_CDC_COMPOSE_FILE:-${ROOT_DIR}/docker-compose.integration.yml}"
PROJECT_NAME="${TB_CDC_COMPOSE_PROJECT:-tb-cdc-int-$RANDOM}"

export TB_CDC_CLUSTER_ID="${TB_CDC_CLUSTER_ID:-0}"
export TB_CDC_NATS_PORT="${TB_CDC_NATS_PORT:-14222}"
export TB_CDC_TIGERBEETLE_PORT="${TB_CDC_TIGERBEETLE_PORT:-13000}"
export TB_CDC_TIGERBEETLE_TAG="${TB_CDC_TIGERBEETLE_TAG:-latest}"

compose() {
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT

cd "${ROOT_DIR}"

cleanup
compose run --rm tigerbeetle-format
compose up -d nats tigerbeetle

TB_CDC_INTEGRATION=1 \
TB_CDC_EXTERNAL_SERVICES=1 \
TB_CDC_EXTERNAL_NATS_URL="nats://127.0.0.1:${TB_CDC_NATS_PORT}" \
TB_CDC_EXTERNAL_CLUSTER_ID="${TB_CDC_CLUSTER_ID}" \
TB_CDC_EXTERNAL_ADDRESSES="127.0.0.1:${TB_CDC_TIGERBEETLE_PORT}" \
go test "$@" -run TestIntegration_CDCResumeWithJetStreamState -v ./...
