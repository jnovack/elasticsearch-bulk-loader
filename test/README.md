# Enrich E2E Fixture

This fixture spins up Elasticsearch 9 and uses the `es-bulk-loader` container itself to create index settings, mappings, pipelines, policies, load source data, execute enrich, create the target index, load target data, and verify the final documents.

## Prerequisites

- Docker with `docker compose`
- `curl`
- `jq`

## Run

```bash
./test/run.sh
```

Set `KEEP_ENV=1` to leave Elasticsearch running for inspection after the test:

```bash
KEEP_ENV=1 ./test/run.sh
```

`run.sh` uses `ES_URL` for host-side health checks and verification, and `LOADER_ES_URL` for the `es-bulk-loader` container's internal connection to Elasticsearch. The defaults are `http://127.0.0.1:9200` and `http://elasticsearch:9200`.

## What The Fixture Proves

The fixture is meant to verify the loader's real lifecycle, not just isolated API calls.

It validates that:

- The first pipeline declared for each index becomes `index.default_pipeline` when the settings file leaves it unset.
- Settings and mappings are applied during index creation.
- Source documents in `e2e-source-index` have `calculated_value` and `source_label` populated by a painless ingest script.
- Enrich policies are created through the loader, not by helper setup scripts.
- The source index is refreshed before enrich execution.
- Target documents in `e2e-target-index` are enriched from `e2e-source-policy` and contain the calculated source fields.

## Fixture Layout

- [fixtures/index1-settings.json](fixtures/index1-settings.json), [fixtures/index1-mappings.json](fixtures/index1-mappings.json), [fixtures/index1-pipelines.json](fixtures/index1-pipelines.json), and [fixtures/index1-policies.json](fixtures/index1-policies.json) define the source index.
- [fixtures/index2-create.json](fixtures/index2-create.json), [fixtures/index2-settings.json](fixtures/index2-settings.json), [fixtures/index2-mappings.json](fixtures/index2-mappings.json), and [fixtures/index2-pipelines.json](fixtures/index2-pipelines.json) define the target index.
- [fixtures/index1-data.json](fixtures/index1-data.json) and [fixtures/index2-data.json](fixtures/index2-data.json) provide the source and target documents.

This split is intentional. The source index owns the enrich policy. The target index proves that enrich execution produced usable backing data.

## Manual Helpers

The shell helpers remain available for direct Elasticsearch-only setup and debugging:

- [setup.sh](setup.sh)
- [create-target.sh](create-target.sh)
