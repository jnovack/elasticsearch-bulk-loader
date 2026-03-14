# Pipelines

Elasticsearch ingest pipelines are created one pipeline at a time at `/_ingest/pipeline/<name>`.
This repository supports organizing multiple pipeline definitions for a single index in one local JSON file.

## File Shape

Use a top-level JSON object where each key is the pipeline name and each value is the exact Elasticsearch pipeline body for that pipeline.

```json
{
  "my-index-default": {
    "description": "Default pipeline for my index",
    "processors": [
      {
        "set": {
          "field": "ingested_by",
          "value": "es-bulk-loader"
        }
      }
    ]
  },
  "my-index-secondary": {
    "description": "Optional secondary pipeline",
    "processors": [
      {
        "remove": {
          "field": "debug"
        }
      }
    ]
  }
}
```

## How It Is Used

The E2E fixture keeps pipeline definitions per index:

- [index1-pipelines.json](../test/e2e/fixtures/index1-pipelines.json)
- [index2-pipelines.json](../test/e2e/fixtures/index2-pipelines.json)

The setup scripts iterate over the keys and issue one `PUT /_ingest/pipeline/<name>` request for each definition.

## Notes

- Consolidation is local-file organization only. Elasticsearch still requires one API call per pipeline.
- Keeping definitions per index is useful when one index owns several pipelines.
- Each value must already match the Elasticsearch ingest pipeline request body format.
