# Enrich E2E Fixture

This fixture spins up Elasticsearch 9 and uses the `es-bulk-loader` container itself to create index settings, mappings, pipelines, policies, load source data, execute enrich, create the target index, load target data, and verify the final documents.

## Prerequisites

- Docker with `docker compose`
- `curl`
- `jq`

## Run

```bash
./test/e2e/run.sh
```

Set `KEEP_ENV=1` to leave Elasticsearch running for inspection after the test:

```bash
KEEP_ENV=1 ./test/e2e/run.sh
```

`run.sh` uses `ES_URL` for host-side health checks and verification, and `LOADER_ES_URL` for the `es-bulk-loader` container's internal connection to Elasticsearch. The defaults are `http://127.0.0.1:9200` and `http://elasticsearch:9200`.

The fixture validates two things:

- Source documents in `e2e-source-index` have `calculated_value` and `source_label` populated by a painless ingest script.
- Target documents in `e2e-target-index` are enriched from `e2e-source-policy` and contain the calculated source fields.

## Manual Helpers

The shell helpers remain available for direct Elasticsearch-only setup and debugging:

- [setup.sh](setup.sh)
- [create-target.sh](create-target.sh)
