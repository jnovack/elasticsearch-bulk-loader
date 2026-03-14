# es-bulk-loader

A Go CLI for end-to-end loading of JSON documents into Elasticsearch-compatible clusters.

`es-bulk-loader` is not just a thin bulk-insert wrapper. It can manage index creation, settings, mappings, ingest pipelines, enrich policies, document loading, and optional enrich execution as one repeatable CLI or CI workflow.

## Features

- Create a new index or work against an existing one with `-add`, `-delete`, and `-flush`
- Load index settings and mappings from JSON files during index creation
- Create or update one or more ingest pipelines from a keyed JSON definition file
- Create or update one or more enrich policies from a keyed JSON definition file
- Bulk load JSON array documents with configurable batch sizes
- Run all enrich policies or only a selected comma-separated subset with `-enrich`
- Refresh the source index before enrich execution so enrich backing indices are built from visible documents
- Support Basic Auth or API key authentication
- Ship with Docker-based end-to-end coverage for pipelines, painless processing, and enrich flow

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

## Command-Line Flags

Settings can be loaded from a configuration file (e.g. `-config es-bulk-loader.conf`), the environment,
or from the command-line.

| Flag                 | Description                                                                  |
|----------------------|------------------------------------------------------------------------------|
| `-config`            | Path to configuration file with settings                                     |
| `-url`               | Elasticsearch URL (e.g., `http://localhost:9200`)                            |
| `-insecureSkipVerify`| Skip TLS verification for HTTPS                                              |
| `-index`             | Target Elasticsearch index name (**required**)                               |
| `-data`              | Path to JSON array of documents to load (**required**)                       |
| `-settings`          | Optional path to JSON file with index settings                               |
| `-mappings`          | Optional path to JSON file with index mappings                               |
| `-pipelines`         | Optional path to JSON file with one or more ingest pipeline definitions      |
| `-policies`          | Optional path to JSON file with one or more enrich policy definitions        |
| `-batch`             | Number of documents per bulk insert (default: 1000)                          |
| `-add`               | Append to an existing index or create it if it doesn’t exist                 |
| `-delete`            | Delete the index if it exists before recreating it (default: false)          |
| `-flush`             | Delete all documents from an existing index without deleting the index       |
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
| ✅ Yes        | `-flush`        | ✅ Delete all documents, keep index settings/mappings/policies/pipelines, load data |
| ✅ Yes        | `-delete`       | ✅ Delete and recreate index, load data                                             |
| ✅ Yes        | `-add -delete`  | ✅ Delete and recreate index, load data                                             |
| ✅ Yes        | `-add -flush`   | ✅ Flush existing docs, then load data                                              |
| ✅ Yes        | none            | ❌ **Fail** — requires explicit `-add`, `-flush`, or `-delete` to continue.         |

## Enrich Policies

Use `-enrich` after a bulk load when enrich policy backing indices need to be rebuilt.

When `-pipelines` and `-policies` are supplied, the loader imports those definitions as part of the run:

- `-delete` removes the current index plus the declared pipelines and policies before rebuilding everything from scratch.
- `-flush` deletes only documents from the current index and preserves existing settings, mappings, pipelines, and policies.
- `-add` updates or creates declared pipelines and policies, then appends documents.

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -data data.json \
  -enrich
```

Run only specific policies:

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -index my-index \
  -data data.json \
  -enrich=policy-a,policy-b
```

Unknown policy names are logged as warnings and skipped.

Definition file formats are documented in [docs/PIPELINES.md](docs/PIPELINES.md) and [docs/POLICIES.md](docs/POLICIES.md).

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

### `settings.conf` (optional)

```ini
url=http://localhost:9200
insecureSkipVerify=true
index=e2e-source-index
settings=test/fixtures/index1-settings.json
mappings=test/fixtures/index1-mappings.json
pipelines=test/fixtures/index1-pipelines.json
policies=test/fixtures/index1-policies.json
data=test/fixtures/index1-data.json
delete=true
enrich=e2e-source-policy
```

## 🛡 Requirements

- Go 1.25+
- Elasticsearch 7.x, 8.x, or 9.x (tested with v9 client)
- Opensearch (does not support enrich policies)

## 👥 License

MIT License © 2025-2026

## Disclaimer

This project is an independent, open-source tool and is not affiliated with, endorsed by, or
sponsored by Elastic. “Elasticsearch” and “Elastic” are trademarks of Elasticsearch B.V.
