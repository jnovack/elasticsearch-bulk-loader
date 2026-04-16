package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

// TestNewConsoleLoggerIncludesTimestamp verifies behavior for the related scenario.
func TestNewConsoleLoggerIncludesTimestamp(t *testing.T) {
	var output bytes.Buffer
	logger := newConsoleLogger(&output)
	logger.Info().Msg("timestamp-check")

	logs := output.String()
	if !strings.Contains(logs, "timestamp-check") {
		t.Fatalf("expected log output to contain message, got: %s", logs)
	}
	if strings.Contains(logs, "<nil>") {
		t.Fatalf("expected timestamped console output without <nil>, got: %s", logs)
	}
	if !regexp.MustCompile(`\d{1,2}:\d{2}:\d{2}`).MatchString(logs) {
		t.Fatalf("expected HH:MM:SS timestamp in console output, got: %s", logs)
	}
}
