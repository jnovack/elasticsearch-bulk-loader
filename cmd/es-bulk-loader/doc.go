// Package main wires CLI inputs to the loader runtime.
//
// Responsibilities:
//   - parse command flags into loader.Options,
//   - populate build metadata for startup diagnostics,
//   - configure console logging and level behavior,
//   - invoke pkg/loader and map fatal conditions to process exit codes.
//
// File layout:
//   - main.go: flag definitions, logger setup, command execution.
//   - main_test.go: CLI logging behavior tests.
//   - doc.go: package contract for command wiring.
//
// Failure modes:
//   - invalid flags/options fail fast with non-zero exit status,
//   - loader runtime errors are surfaced with operation context.
package main
