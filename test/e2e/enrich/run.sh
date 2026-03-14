#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-esbl-enrich-e2e}"
ES_PORT="${ES_PORT:-9200}"
ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT}}"
KEEP_ENV="${KEEP_ENV:-0}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ "${KEEP_ENV}" == "1" ]]; then
    echo "KEEP_ENV=1, leaving Docker resources running"
    return
  fi

  docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans
}

run_loader() {
  docker compose -f "${COMPOSE_FILE}" run --rm es-bulk-loader "$@"
}

require_command docker
require_command curl
require_command jq

trap cleanup EXIT

export COMPOSE_PROJECT_NAME ES_PORT ES_URL

echo "Starting Elasticsearch 9"
docker compose -f "${COMPOSE_FILE}" up -d elasticsearch

echo "Building the loader image"
docker compose -f "${COMPOSE_FILE}" build es-bulk-loader

echo "Creating pipelines, indices, and enrich policy"
"${SCRIPT_DIR}/setup.sh"

echo "Bulk loading source data and executing enrich policy"
run_loader \
  -url "http://elasticsearch:9200" \
  -index e2e-source-index \
  -data /fixtures/index1-data.json \
  -batch 2 \
  -add \
  -enrich=e2e-source-policy

echo "Creating target resources after enrich index execution"
"${SCRIPT_DIR}/create-target.sh"

echo "Bulk loading target data through the enrich pipeline"
run_loader \
  -url "http://elasticsearch:9200" \
  -index e2e-target-index \
  -data /fixtures/index2-data.json \
  -batch 2 \
  -add

echo "Verifying calculated and enriched fields"
"${SCRIPT_DIR}/verify.sh"
