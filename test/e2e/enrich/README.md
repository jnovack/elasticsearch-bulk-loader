# Enrich E2E Fixture

This fixture spins up Elasticsearch 9, creates the source index and enrich policy, loads source data through the `es-bulk-loader` container, executes the enrich policy with `-enrich`, then creates the target index and enrich pipeline, loads target data, and verifies the final documents.

## Prerequisites

- Docker with `docker compose`
- `curl`
- `jq`

## Run

```bash
./test/e2e/enrich/run.sh
```

Set `KEEP_ENV=1` to leave Elasticsearch running for inspection after the test:

```bash
KEEP_ENV=1 ./test/e2e/enrich/run.sh
```

The fixture validates two things:

- Source documents in `e2e-source-index` have `calculated_value` and `source_label` populated by a painless ingest script.
- Target documents in `e2e-target-index` are enriched from `e2e-source-policy` and contain the calculated source fields.
