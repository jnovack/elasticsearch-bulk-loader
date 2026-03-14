#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"
ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT:-9200}}"

curl_json() {
  local method=$1
  local path=$2
  local body_file=$3

  curl -fsS -X "${method}" "${ES_URL}${path}" \
    -H "Content-Type: application/json" \
    --data-binary "@${body_file}" >/dev/null
}

echo "Creating target enrich pipeline and index"
curl_json PUT "/_ingest/pipeline/e2e-index2-enrich" "${FIXTURES_DIR}/index2-pipeline.json"
curl_json PUT "/e2e-target-index" "${FIXTURES_DIR}/index2-create.json"

echo "Target-side E2E resources created"
