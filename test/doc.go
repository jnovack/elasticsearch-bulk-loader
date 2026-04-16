//go:build e2e

// Package e2e contains Docker-backed integration scenarios for es-bulk-loader.
//
// Responsibilities:
//   - stand up disposable Elasticsearch fixtures,
//   - execute CLI flows across delete/flush/add/nuke/enrich/alias modes,
//   - assert final index, pipeline, policy, and document-state outcomes.
//
// File layout:
//   - e2e_test.go: scenario harness, fixture setup, and assertions.
//   - doc.go: package-level contract for build-tagged integration coverage.
package e2e
