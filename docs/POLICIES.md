# Policies

Elasticsearch enrich policies are created one policy at a time at `/_enrich/policy/<name>`.
This repository supports organizing multiple policy definitions for a single index in one local JSON file.

## File Shape

Use a top-level JSON object where each key is the policy name and each value is the exact Elasticsearch enrich policy body for that policy.

```json
{
  "my-index-match-policy": {
    "match": {
      "indices": "my-source-index",
      "match_field": "lookup_id",
      "enrich_fields": [
        "name",
        "status"
      ]
    }
  },
  "my-index-range-policy": {
    "range": {
      "indices": "my-range-index",
      "match_field": "start",
      "enrich_fields": [
        "label"
      ]
    }
  }
}
```

## How It Is Used

The loader reads the file and issues one `PUT /_enrich/policy/<name>` request per entry.

The E2E fixture keeps source-index enrich policies in:

- [index1-policies.json](../test/fixtures/index1-policies.json)

The loader, not the shell fixtures, is the primary path under test.

## Execution Behavior

Creating an enrich policy is not the same as executing it.

- `-policies` creates or updates the declared policy definitions.
- `-enrich` executes enrich policies after the bulk load completes.
- Before execution, the loader refreshes the source index so the enrich backing index is built from visible documents.
- If `-enrich` is passed with no explicit value and `-policies` is also supplied, the loader executes the policies declared for that run.
- If `-enrich` is passed without `-policies`, the loader falls back to the enrich policies currently available in the cluster.
- Unknown policy names are warned and skipped.
- If the cluster does not expose enrich APIs, the loader warns and skips enrich create/delete/execute operations instead of aborting the whole run.

## Variable Expansion

Definition files are templated before they are parsed.

- `${INDEX}` expands to the current `-index` value.
- Other placeholders fall back to environment variables when present.

Example:

```json
{
  "my-policy": {
    "match": {
      "indices": "${INDEX}",
      "match_field": "lookup_id",
      "enrich_fields": ["name"]
    }
  }
}
```

## Notes

- Consolidation is local-file organization only. Elasticsearch still requires one API call per policy.
- Policy bodies must match the Elasticsearch enrich policy request format.
- `${INDEX}` is expanded by `es-bulk-loader` to the current `-index` value before the policy file is parsed.
- A created policy still needs to be executed before its enrich backing index exists.
