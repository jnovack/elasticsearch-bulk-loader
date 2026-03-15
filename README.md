# es-bulk-loader

A Go CLI for end-to-end loading of JSON documents into Elasticsearch-compatible clusters.

`es-bulk-loader` can manage index creation, settings, mappings, ingest pipelines, enrich policies,
document loading, and optional enrich execution as one repeatable CLI or CI workflow.

## Features

- Create a new index or work against an existing one with `-add`, `-delete`, and `-flush`
- Load index settings and mappings from JSON files during index creation
- Create or update one or more ingest pipelines from a keyed JSON definition file, using the first declared pipeline as the default when settings do not already define a default pipeline
- Create or update one or more enrich policies from a keyed JSON definition file
- Bulk load JSON array documents with configurable batch sizes
- Run all enrich policies or only a selected comma-separated subset with `-enrich`
- Refresh the source index before enrich execution so enrich backing indices are built from visible documents
- Warn and skip enrich operations cleanly when the cluster does not expose enrich APIs (e.g. OpenSearch)

## Quick Start

### Build

```bash
go build -o es-bulk-loader ./cmd/es-bulk-loader
```

### Run Example

#### Docker

```bash
docker run --rm jnovack/es-bulk-loader:latest \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -pipelines pipelines.json \
  -policies policies.json \
  -data data.json \
  -batch 500 \
  -delete \
  -enrich
```

#### Go

```bash
go run ./cmd/es-bulk-loader \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -pipelines pipelines.json \
  -policies policies.json \
  -data data.json \
  -batch 500 \
  -delete \
  -enrich
```

Sample config file: [examples/es-bulk-loader.conf](examples/es-bulk-loader.conf)

## Loader Workflow

When you give `es-bulk-loader` settings, mappings, pipelines, policies, and data, it treats that as one managed workflow:

1. Delete the current index and declared managed resources when `-delete` is set.
2. Flush only documents when `-flush` is set.
3. Create or update declared ingest pipelines.
4. Create the index when needed, applying settings and mappings.
5. Create or update declared enrich policies when appropriate for that run.
6. Bulk load the data file.
7. Refresh the source index and execute enrich policies when `-enrich` is requested.

That ordering matters. The loader is intentionally opinionated so CI runs and operator workflows stay predictable.

## Command-Line Flags

Settings can be loaded from a configuration file (e.g. `-config es-bulk-loader.conf`), the environment,
or from the command-line.

| Flag                 | Description                                                                  |
|----------------------|------------------------------------------------------------------------------|
| `-config`            | Path to configuration file with settings                                     |
| `-url`               | Endpoint URL (e.g., `http://localhost:9200`)                                 |
| `-insecureSkipVerify`| Skip TLS verification for HTTPS                                              |
| `-index`             | Target index name (**required**)                                             |
| `-data`              | Path to JSON array of documents to load (**required**)                       |
| `-settings`          | Optional path to JSON file with index settings                               |
| `-mappings`          | Optional path to JSON file with index mappings                               |
| `-pipelines`         | Optional path to JSON file with one or more ingest pipeline definitions      |
| `-policies`          | Optional path to JSON file with one or more enrich policy definitions        |
| `-batch`             | Number of documents per bulk insert (default: 1000)                          |
| `-add`               | Append to an existing index or create it if it doesn’t exist; with `-flush`, also update declared pipelines and policies |
| `-delete`            | Delete the index if it exists before recreating it (default: false)          |
| `-flush`             | Delete all documents from an existing index without deleting the index; with `-add`, also update declared pipelines and policies |
| `-nuke`              | With `-delete`, also delete ingest pipelines that reference declared enrich policies so managed resources can be fully reset |
| `-id`                | Field to use in the document to override _id (default: not set)              |
| `-enrich`            | Run enrich policies after the bulk insert; omit value for all or pass a comma-separated list |
| `-user` / `-pass`    | Username and password for Basic Auth                                         |
| `-apiKey`            | Elasticsearch API key                                                        |
| `-version`           | Print version and exit                                                       |

## Behavior Summary

| Index Exists  | Flags Set       | Action                                                                              |
|---------------|-----------------|-------------------------------------------------------------------------------------|
| ❌ No         | none or `-add`  | ✅ Create index (with optional settings/mappings), load data                        |
| ❌ No         | `-delete`       | ✅ Warn (nothing to delete), create index, load data                                |
| ❌ No         | `-flush`        | ✅ Warn (nothing to flush), create index, load data                                 |
| ❌ No         | `-add -delete`  | ✅ Create index, load data                                                          |
| ❌ No         | `-add -flush`   | ✅ Create index, load data                                                          |
| ✅ Yes        | `-add`          | ✅ Append data to existing index                                                    |
| ✅ Yes        | `-flush`        | ✅ Delete all documents, keep index settings/mappings/policies/pipelines unchanged, load data |
| ✅ Yes        | `-delete`       | ✅ Delete and recreate index, load data                                             |
| ✅ Yes        | `-add -delete`  | ✅ Delete and recreate index, load data                                             |
| ✅ Yes        | `-add -flush`   | ✅ Flush existing docs, keep settings/mappings, update declared pipelines/policies, then load data |
| ✅ Yes        | `-delete -nuke` | ✅ Delete and recreate index; also delete pipelines that reference declared enrich policies so policy cleanup can complete |
| ✅ Yes        | none            | ❌ **Fail** — requires explicit `-add`, `-flush`, or `-delete` to continue.         |

## Enrich Policies

Use `-enrich` after a bulk load when enrich policy backing indices need to be rebuilt.

When `-pipelines` and `-policies` are supplied, the loader imports those definitions as part of the run:

- `-delete` removes the current index plus the declared pipelines and policies before rebuilding everything from scratch.
- `-delete` fails loudly if a declared enrich policy is still referenced by another ingest pipeline. This is the safe default because those references may belong to another index workflow.
- `-delete -nuke` keeps deleting through that conflict by finding and deleting ingest pipelines that reference the declared enrich policy, then retrying the policy delete.
- `-flush` deletes only documents from the current index and preserves existing settings, mappings, pipelines, and policies as-is.
- `-flush -add` deletes only documents from the current index, preserves existing settings and mappings, and updates or creates the declared pipelines and policies before loading new data.
- `-add` updates or creates declared pipelines and policies, then appends documents.

Use `-nuke` carefully. Unlike the normal one-index workflow, it can remove ingest pipelines outside the current index's declared pipeline file if those pipelines reference a declared enrich policy you are deleting.

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -data data.json \
  -enrich
```

When `-enrich` is provided without an explicit policy list and `-policies` is also provided, the loader executes the policies declared for that run. If no policy file is provided, it falls back to all enrich policies currently available in the cluster.

Run only specific policies:

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -data data.json \
  -enrich=policy-a,policy-b
```

Unknown policy names are logged as warnings and skipped.
If the cluster does not expose the enrich APIs at all, the loader logs a warning and skips enrich policy create/delete/execute operations instead of aborting the whole load.

Definition file formats are documented in [docs/PIPELINES.md](docs/PIPELINES.md) and [docs/POLICIES.md](docs/POLICIES.md).

When `-pipelines` is supplied during index creation, the loader preserves the JSON key order from the pipeline file. If the settings file does not already define a default pipeline, the first declared pipeline becomes the index default pipeline automatically.

Definition files support variable expansion before they are parsed. `${INDEX}` is populated from the current `-index` value, and other placeholders fall back to environment variables when present.

## JSON Formats

### `data.json`

```json
[
  { "id": 1, "name": "Alice" },
  { "id": 2, "name": "Bob" }
]
```

### `settings.json` (optional)

```json
{
  "number_of_shards": 1,
  "number_of_replicas": 1
}
```

Wrapped settings are also accepted:

```json
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 1
  }
}
```

Nested `index` settings are also accepted and normalized:

```json
{
  "settings": {
    "index": {
      "number_of_shards": 2,
      "number_of_replicas": 1
    }
  }
}
```

Dotted `index.*` keys are also accepted:

```json
{
  "index.number_of_shards": 2,
  "index.number_of_replicas": 1,
  "index.default_pipeline": "my-default-pipeline"
}
```

All of these shapes are normalized into the create-index request body the loader sends to Elasticsearch.

### `mappings.json` (optional)

```json
{
  "properties": {
    "id":   { "type": "integer" },
    "name": { "type": "text" }
  }
}
```

Wrapped mappings are also accepted:

```json
{
  "mappings": {
    "properties": {
      "id": { "type": "integer" },
      "name": { "type": "text" }
    }
  }
}
```

## 🛡 Requirements

- Go 1.25+
- Elasticsearch 7.x, 8.x, or 9.x (tested with v9 client) or Opensearch (does not support enrich policies)

## 👥 License

MIT License © 2025-2026

## Disclaimer

This project is an independent, open-source tool and is not affiliated with, endorsed by, or
sponsored by Elastic. “Elasticsearch” and “Elastic” are trademarks of Elasticsearch B.V.
