# Elasticsearch Bulk Loader (Go)

A fast and flexible Go CLI application for bulk loading JSON data into an Elasticsearch cluster.

It supports loading index settings, mappings, and data from external JSON files with full control over index lifecycle (create, add to, or delete/recreate).

## 📦 Features

- Support for Basic Auth and API Keys
- Supports optional (and smart) settings and mappings management for index creation

## 🚀 Quick Start

### 🔧 Build

```bash
go build -o es-loader main.go
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
  -data data.json \
  -batch 500 \
  -delete
```

#### Do It Yourself

```bash
go run cmd/es-bulk-loader/main.go \
  -url https://localhost:9200 \
  -insecureSkipVerify=true \
  -index my-index \
  -settings settings.json \
  -mappings mappings.json \
  -data data.json \
  -batch 500 \
  -delete
```

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
| `-pipelines`         | Optional path to JSON file containing one or more ingest pipeline definitions |
| `-policies`          | Optional path to JSON file containing one or more enrich policy definitions  |
| `-batch`             | Number of documents per bulk insert (default: 1000)                          |
| `-add`               | Append to an existing index or create it if it doesn’t exist                 |
| `-delete`            | Delete the index if it exists before recreating it (default: false)          |
| `-flush`             | Delete all documents from an existing index without deleting the index        |
| `-id`                | Field to use in the document to override _id (default: not set)              |
| `-enrich`            | Run enrich policies after the bulk insert; omit value for all or pass a comma-separated list |
| `-user` / `-pass`    | Username and password for Basic Auth                                         |
| `-apiKey`            | Elasticsearch API key                                                        |
| `-version`           | Print version and exit                                                       |

## Behavior Summary

| Index Exists | Flags Set       | Action                                                               |
|--------------|-----------------|----------------------------------------------------------------------|
| ❌ No         | none or `-add`  | ✅ Create index (with optional settings/mappings), load data         |
| ❌ No         | `-delete`       | ✅ Warn (nothing to delete), create index, load data                 |
| ❌ No         | `-flush`        | ✅ Warn (nothing to flush), create index, load data                  |
| ❌ No         | `-add -delete`  | ✅ Create index, load data                                           |
| ❌ No         | `-add -flush`   | ✅ Create index, load data                                           |
| ✅ Yes        | `-add`          | ✅ Append data to existing index                                     |
| ✅ Yes        | `-flush`        | ✅ Delete all documents, keep index settings/mappings/policies/pipelines, load data |
| ✅ Yes        | `-delete`       | ✅ Delete and recreate index, load data                              |
| ✅ Yes        | `-add -delete`  | ✅ Delete and recreate index, load data                              |
| ✅ Yes        | `-add -flush`   | ✅ Flush existing docs, then load data                               |
| ✅ Yes        | none            | ❌ **Fail** — requires explicit `-add`, `-flush`, or `-delete` to continue |

## JSON Formats

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

### `mappings.json` (optional)

```json
{
  "properties": {
    "id":   { "type": "integer" },
    "name": { "type": "text" }
  }
}
```

### `settings.conf` (optional)

```ini
url=http://localhost:9200
insecureSkipVerify=true
index=e2e-source-index
settings=test/e2e/fixtures/index1-settings.json
mappings=test/e2e/fixtures/index1-mappings.json
pipelines=test/e2e/fixtures/index1-pipelines.json
policies=test/e2e/fixtures/index1-policies.json
data=test/e2e/fixtures/index1-data.json
delete=true
```

## 🛡 Requirements

- Go 1.25+
- Elasticsearch 7.x, 8.x, or 9.x (tested with v9 client)

## 👥 License

MIT License © 2025
