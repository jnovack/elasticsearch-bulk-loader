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

| Flag                 | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| `-config`            | Path to configuration file with settings                                    |
| `-url`               | Elasticsearch URL (e.g., `http://localhost:9200`)                           |
| `-insecureSkipVerify`| Skip TLS verification for HTTPS                                             |
| `-index`             | Target Elasticsearch index name (**required**)                              |
| `-data`              | Path to JSON array of documents to load (**required**)                      |
| `-settings`          | Optional path to JSON file with index settings                              |
| `-mappings`          | Optional path to JSON file with index mappings                              |
| `-batch`             | Number of documents per bulk insert (default: 1000)                         |
| `-add`               | Append to an existing index or create it if it doesn’t exist                |
| `-delete`            | Delete the index if it exists before recreating it                          |

## Behavior Summary

| Index Exists | Flags Set       | Action                                                               |
|--------------|-----------------|----------------------------------------------------------------------|
| ❌ No         | none or `-add`  | ✅ Create index (with optional settings/mappings), load data         |
| ❌ No         | `-delete`       | ✅ Warn (nothing to delete), create index, load data                 |
| ❌ No         | `-add -delete`  | ✅ Create index, load data                                           |
| ✅ Yes        | `-add`          | ✅ Append data to existing index                                     |
| ✅ Yes        | `-delete`       | ✅ Delete and recreate index, load data                              |
| ✅ Yes        | `-add -delete`  | ✅ Delete and recreate index, load data                              |
| ✅ Yes        | none            | ❌ **Fail** — requires explicit `-add` or `-delete` to continue      |

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
url=https://localhost:9200
insecureSkipVerify=true
delete=true
```

## 🛡 Requirements

- Go 1.18+
- Elasticsearch 7.x, 8.x, or 9.x (tested with v9 client)

## 👥 License

MIT License © 2025
