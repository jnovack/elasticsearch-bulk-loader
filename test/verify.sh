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
    echo "Expected: ${filter}" >&2
    echo "Actual response:" >&2
    echo "${response}" | jq . >&2
    exit 1
  fi
}

echo "Refreshing source and target indices"
curl -fsS -X POST "${ES_URL}/e2e-source-index/_refresh" >/dev/null
curl -fsS -X POST "${ES_URL}/e2e-target-index/_refresh" >/dev/null

echo "Verifying index.default_pipeline selection"
source_settings=$(curl -fsS "${ES_URL}/e2e-source-index/_settings")
assert_response "source settings" "${source_settings}" '."e2e-source-index".settings.index.default_pipeline == "e2e-index1-calc"'
target_settings=$(curl -fsS "${ES_URL}/e2e-target-index/_settings")
assert_response "target settings" "${target_settings}" '."e2e-target-index".settings.index.default_pipeline == "e2e-index2-enrich"'

echo "Verifying source index painless calculation output"
source_response=$(curl -fsS "${ES_URL}/e2e-source-index/_search?size=10&sort=lookup_id:asc")
assert_response "source index" "${source_response}" '.hits.total.value == 2'
assert_response "source index" "${source_response}" '.hits.hits[0]._id == "A1"'
assert_response "source index" "${source_response}" '.hits.hits[0]._source.lookup_id == "A1"'
assert_response "source index" "${source_response}" '.hits.hits[0]._source.lookup_id == "A1"'
assert_response "source index" "${source_response}" '.hits.hits[0]._source.calculated_value == 21'
assert_response "source index" "${source_response}" '.hits.hits[0]._source.source_label == "alpha-21"'
assert_response "source index" "${source_response}" '.hits.hits[0]._source.secondary_pipeline_ran == null'
assert_response "source index" "${source_response}" '.hits.hits[1]._id == "B2"'
assert_response "source index" "${source_response}" '.hits.hits[1]._source.lookup_id == "B2"'
assert_response "source index" "${source_response}" '.hits.hits[1]._source.calculated_value == 20'
assert_response "source index" "${source_response}" '.hits.hits[1]._source.source_label == "beta-20"'
assert_response "source index" "${source_response}" '.hits.hits[1]._source.secondary_pipeline_ran == null'

echo "Verifying target index enrich output"
target_response=$(curl -fsS "${ES_URL}/e2e-target-index/_search?size=10&sort=lookup_id:asc")
assert_response "target index" "${target_response}" '.hits.total.value == 2'
assert_response "target index" "${target_response}" '.hits.hits[0]._source.lookup_id == "A1"'
assert_response "target index" "${target_response}" '.hits.hits[0]._source.source_enrich.calculated_value == 21'
assert_response "target index" "${target_response}" '.hits.hits[0]._source.source_enrich.source_name == "alpha"'
assert_response "target index" "${target_response}" '.hits.hits[0]._source.source_enrich.source_label == "alpha-21"'
assert_response "target index" "${target_response}" '.hits.hits[0]._source.secondary_pipeline_ran == null'
assert_response "target index" "${target_response}" '.hits.hits[1]._source.lookup_id == "B2"'
assert_response "target index" "${target_response}" '.hits.hits[1]._source.source_enrich.calculated_value == 20'
assert_response "target index" "${target_response}" '.hits.hits[1]._source.source_enrich.source_name == "beta"'
assert_response "target index" "${target_response}" '.hits.hits[1]._source.source_enrich.source_label == "beta-20"'
assert_response "target index" "${target_response}" '.hits.hits[1]._source.secondary_pipeline_ran == null'

echo "E2E verification succeeded"
