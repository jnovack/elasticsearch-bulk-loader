#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ES_URL:-http://127.0.0.1:${ES_PORT:-9200}}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command curl
require_command jq

assert_response() {
  local name=$1
  local response=$2
  local filter=$3

  if ! echo "${response}" | jq -e "${filter}" >/dev/null; then
    echo "${name} verification failed" >&2
    echo "Actual response:" >&2
    echo "${response}" | jq . >&2
    exit 1
  fi
}

echo "Refreshing source and target indices"
curl -fsS -X POST "${ES_URL}/e2e-source-index/_refresh" >/dev/null
curl -fsS -X POST "${ES_URL}/e2e-target-index/_refresh" >/dev/null

echo "Verifying source index painless calculation output"
source_response=$(curl -fsS "${ES_URL}/e2e-source-index/_search?size=10&sort=lookup_id:asc")
assert_response "source index" "${source_response}" '
  .hits.total.value == 2 and
  .hits.hits[0]._source.lookup_id == "A1" and
  .hits.hits[0]._source.calculated_value == 21 and
  .hits.hits[0]._source.source_label == "alpha-21" and
  .hits.hits[1]._source.lookup_id == "B2" and
  .hits.hits[1]._source.calculated_value == 20 and
  .hits.hits[1]._source.source_label == "beta-20"
'

echo "Verifying target index enrich output"
target_response=$(curl -fsS "${ES_URL}/e2e-target-index/_search?size=10&sort=lookup_id:asc")
assert_response "target index" "${target_response}" '
  .hits.total.value == 2 and
  .hits.hits[0]._source.lookup_id == "A1" and
  .hits.hits[0]._source.source_enrich.calculated_value == 21 and
  .hits.hits[0]._source.source_enrich.source_name == "alpha" and
  .hits.hits[0]._source.source_enrich.source_label == "alpha-21" and
  .hits.hits[1]._source.lookup_id == "B2" and
  .hits.hits[1]._source.source_enrich.calculated_value == 20 and
  .hits.hits[1]._source.source_enrich.source_name == "beta" and
  .hits.hits[1]._source.source_enrich.source_label == "beta-20"
'

echo "E2E verification succeeded"
