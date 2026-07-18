package sqlite_test

import (
	"path/filepath"
	"testing"
	"time"

	"contogether/logsys"
	"contogether/logsys/backends/sqlite"
)

func TestWriteReadClearRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.db")
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := []logsys.LogEntry{
		logsys.NewEntry(base, logsys.InfoLevel, "one", map[string]any{"n": 1.0}),
		logsys.NewEntry(base.Add(time.Minute), logsys.ErrorLevel, "two", nil),
	}
	for _, e := range entries {
		if err := store.Write(e); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	got, err := store.Read(logsys.DebugLevel, logsys.LogFilter{})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[0].Message() != "one" || got[1].Message() != "two" {
		t.Fatalf("unexpected entries: %+v", got)
	}
	if got[0].Fields()["n"] != 1.0 {
		t.Fatalf("expected field n=1, got %+v", got[0].Fields())
	}

	onlyErrors, err := store.Read(logsys.ErrorLevel, logsys.LogFilter{})
	if err != nil {
		t.Fatalf("Read(ErrorLevel) failed: %v", err)
	}
	if len(onlyErrors) != 1 || onlyErrors[0].Message() != "two" {
		t.Fatalf("Read(ErrorLevel) = %+v, want just \"two\"", onlyErrors)
	}

	if err := store.Clear(base.Add(30 * time.Second)); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}
	remaining, err := store.Read(logsys.DebugLevel, logsys.LogFilter{})
	if err != nil {
		t.Fatalf("Read after Clear failed: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Message() != "two" {
		t.Fatalf("after Clear, remaining = %+v, want just \"two\"", remaining)
	}
}
