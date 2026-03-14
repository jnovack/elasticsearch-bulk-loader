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

The E2E fixture keeps source-index enrich policies in:

- [index1-policies.json](../test/e2e/fixtures/index1-policies.json)

The setup script iterates over the keys and issues one `PUT /_enrich/policy/<name>` request for each definition.

## Notes

- Consolidation is local-file organization only. Elasticsearch still requires one API call per policy.
- Policy bodies must match the Elasticsearch enrich policy request format.
- A created policy still needs to be executed before its enrich backing index exists.
