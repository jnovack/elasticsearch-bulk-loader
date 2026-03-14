#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
FIXTURES_DIR="${SCRIPT_DIR}/fixtures"
ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT:-9200}}"

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

echo "Waiting for Elasticsearch at ${ES_URL}"
wait_for_elasticsearch

echo "Resetting E2E resources"
curl -fsS -X DELETE "${ES_URL}/e2e-target-index,e2e-source-index" >/dev/null || true
curl -fsS -X DELETE "${ES_URL}/_ingest/pipeline/e2e-index2-enrich" >/dev/null || true
curl -fsS -X DELETE "${ES_URL}/_ingest/pipeline/e2e-index1-calc" >/dev/null || true
curl -fsS -X DELETE "${ES_URL}/_enrich/policy/e2e-source-policy" >/dev/null || true

echo "Creating source calculation pipeline and index"
curl_json PUT "/_ingest/pipeline/e2e-index1-calc" "${FIXTURES_DIR}/index1-pipeline.json"
curl_json PUT "/e2e-source-index" "${FIXTURES_DIR}/index1-settings.json"
curl_json PUT "/e2e-source-index" "${FIXTURES_DIR}/index1-mappings.json"
wait_for_index "e2e-source-index"

echo "Creating enrich policy"
curl_json PUT "/_enrich/policy/e2e-source-policy" "${FIXTURES_DIR}/enrich-policy.json"
echo "Source-side E2E resources created"
