#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"
ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT:-9200}}"
INDEX1_PIPELINES_FILE="${FIXTURES_DIR}/index1-pipelines.json"
INDEX1_POLICIES_FILE="${FIXTURES_DIR}/index1-policies.json"
INDEX1_SETTINGS_FILE="${FIXTURES_DIR}/index1-settings.json"
INDEX1_MAPPINGS_FILE="${FIXTURES_DIR}/index1-mappings.json"

curl_json() {
  local method=$1
  local path=$2
  local body_file=${3:-}

  if [[ -n "${body_file}" ]]; then
    curl -fsS -X "${method}" "${ES_URL}${path}" \
      -H "Content-Type: application/json" \
      --data-binary "@${body_file}" >/dev/null
    return
  fi

  curl -fsS -X "${method}" "${ES_URL}${path}" >/dev/null
}

names_from_file() {
  local definitions_file=$1
  jq -r 'keys[]' "${definitions_file}"
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
  done < <(names_from_file "${definitions_file}")
}

delete_pipelines_from_file() {
  local definitions_file=$1
  local pipeline_name

  while IFS= read -r pipeline_name; do
    [[ -n "${pipeline_name}" ]] || continue
    curl -fsS -X DELETE "${ES_URL}/_ingest/pipeline/${pipeline_name}" >/dev/null || true
  done < <(names_from_file "${definitions_file}")
}

create_index() {
  local index_name=$1
  local settings_file=$2
  local mappings_file=$3

  jq -s '
    {
      settings: ((.[0].settings // .[0]) // {}),
      mappings: ((.[1].mappings // .[1]) // {})
    }
  ' "${settings_file}" "${mappings_file}" |
    curl -fsS -X PUT "${ES_URL}/${index_name}" \
      -H "Content-Type: application/json" \
      --data-binary @- >/dev/null
}

create_enrich_policies_from_file() {
  local definitions_file=$1
  local policy_name

  while IFS= read -r policy_name; do
    [[ -n "${policy_name}" ]] || continue
    echo "Creating enrich policy ${policy_name}"
    jq -c --arg name "${policy_name}" '.[$name]' "${definitions_file}" |
      curl -fsS -X PUT "${ES_URL}/_enrich/policy/${policy_name}" \
        -H "Content-Type: application/json" \
        --data-binary @- >/dev/null
  done < <(names_from_file "${definitions_file}")
}

delete_enrich_policies_from_file() {
  local definitions_file=$1
  local policy_name

  while IFS= read -r policy_name; do
    [[ -n "${policy_name}" ]] || continue
    curl -fsS -X DELETE "${ES_URL}/_enrich/policy/${policy_name}" >/dev/null || true
  done < <(names_from_file "${definitions_file}")
}

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
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

wait_for_index() {
  local index_name=$1
  local attempts=30
  local i

  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${ES_URL}/${index_name}" >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "Index ${index_name} did not become visible at ${ES_URL}" >&2
  return 1
}

require_command curl
require_command jq

echo "Waiting for Elasticsearch at ${ES_URL}"
wait_for_elasticsearch

echo "Resetting E2E resources"
curl -fsS -X DELETE "${ES_URL}/e2e-target-index,e2e-source-index" >/dev/null || true
delete_pipelines_from_file "${INDEX1_PIPELINES_FILE}"
delete_enrich_policies_from_file "${INDEX1_POLICIES_FILE}"
