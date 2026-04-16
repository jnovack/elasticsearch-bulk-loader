// Package loader implements the Elasticsearch lifecycle used by es-bulk-loader.
//
// Responsibilities:
//   - validate run options and choose data action modes,
//   - create/update managed resources (settings, mappings, pipelines, policies, transforms),
//   - load/replace/append source data in bulk,
//   - execute enrich policies and managed transform lifecycle steps.
//
// File layout:
//   - loader.go: runtime orchestration, API calls, option parsing helpers.
//   - main_test.go: unit coverage for flags, mapping logic, and execution ordering.
//   - pipeline_defaults_test.go: settings/pipeline normalization behavior tests.
//   - doc.go: package contract and lifecycle semantics.
//
// Non-obvious decisions:
//   - managed enrich policy names are content-hashed and rewritten in dependent
//     pipelines to make updates deterministic and garbage-collectable.
//   - transform definitions are filtered by logical source index so a single
//     transforms file can serve multiple loader runs.
//
// Failure modes:
//   - option validation and managed resource failures return typed sentinel errors,
//   - bulk item failures include per-item diagnostics and fail the run,
//   - enrich/transform lifecycle failures are treated as fatal when requested.
package loader
