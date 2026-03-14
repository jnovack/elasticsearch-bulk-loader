#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"
ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT:-9200}}"
INDEX2_PIPELINES_FILE="${FIXTURES_DIR}/index2-pipelines.json"

curl_json() {
  local method=$1
  local path=$2
  local body_file=$3

  curl -fsS -X "${method}" "${ES_URL}${path}" \
    -H "Content-Type: application/json" \
    --data-binary "@${body_file}" >/dev/null
}

create_pipelines_from_file() {
  local definitions_file=$1
  local pipeline_name

  while IFS= read -r pipeline_name; do
    [[ -n "${pipeline_name}" ]] || continue
    echo "Creating pipeline ${pipeline_name}"
    jq -c --arg name "${pipeline_name}" '.[$name]' "${definitions_file}" |
      curl -fsS -X PUT "${ES_URL}/_ingest/pipeline/${pipeline_name}" \
        -H "Content-Type: application/json" \
        --data-binary @- >/dev/null
  done < <(jq -r 'keys[]' "${definitions_file}")
}

delete_pipelines_from_file() {
  local definitions_file=$1
  local pipeline_name

  while IFS= read -r pipeline_name; do
    [[ -n "${pipeline_name}" ]] || continue
    curl -fsS -X DELETE "${ES_URL}/_ingest/pipeline/${pipeline_name}" >/dev/null || true
  done < <(jq -r 'keys[]' "${definitions_file}")
}

echo "Creating target enrich pipeline and index"
delete_pipelines_from_file "${INDEX2_PIPELINES_FILE}"
create_pipelines_from_file "${INDEX2_PIPELINES_FILE}"
curl_json PUT "/e2e-target-index" "${FIXTURES_DIR}/index2-create.json"

echo "Target-side E2E resources created"
