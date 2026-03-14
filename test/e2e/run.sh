#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-esbl-enrich-e2e}"
ES_PORT="${ES_PORT:-9200}"
ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT}}"
LOADER_ES_URL="${LOADER_ES_URL:-http://elasticsearch:9200}"
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

wait_for_elasticsearch() {
  local attempts=60
  local i

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${ES_URL}/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null; then
      return 0
    fi
    sleep 2
  done

  echo "Elasticsearch did not become ready at ${ES_URL}" >&2
  return 1
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

echo "Waiting for Elasticsearch"
wait_for_elasticsearch

echo "Creating source index, policies, pipelines, and data through es-bulk-loader"
run_loader \
  -url "${LOADER_ES_URL}" \
  -index e2e-source-index \
  -settings /fixtures/index1-settings.json \
  -mappings /fixtures/index1-mappings.json \
  -pipelines /fixtures/index1-pipelines.json \
  -policies /fixtures/index1-policies.json \
  -data /fixtures/index1-data.json \
  -batch 2 \
  -delete \
  -enrich

echo "Creating target index, pipelines, and data through es-bulk-loader"
run_loader \
  -url "${LOADER_ES_URL}" \
  -index e2e-target-index \
  -settings /fixtures/index2-settings.json \
  -mappings /fixtures/index2-mappings.json \
  -pipelines /fixtures/index2-pipelines.json \
  -data /fixtures/index2-data.json \
  -batch 2 \
  -delete

echo "Verifying calculated and enriched fields"
"${SCRIPT_DIR}/verify.sh"
