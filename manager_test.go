package logsys_test

import (
	"sync"
	"testing"
	"time"

	"contogether/logsys"
	"contogether/logsys/backends/memory"
)

type countingHandler struct {
	mu    sync.Mutex
	count int
}

func (h *countingHandler) Handle(logsys.LogEntry) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
}

func (h *countingHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func TestWriteLogAndReadLogsLevelFiltering(t *testing.T) {
	store := memory.New()
	mgr := logsys.NewManager(store, store, store)

	handler := &countingHandler{}
	mgr.RegisterLogHandler(handler)

	writes := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	for _, level := range writes {
		if err := mgr.WriteLog(level, level+" entry"); err != nil {
			t.Fatalf("WriteLog(%s) failed: %v", level, err)
		}
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if got := handler.Count(); got != len(writes) {
		t.Fatalf("handler invocations = %d, want %d", got, len(writes))
	}

	entries, err := mgr.ReadLogs("WARN", logsys.LogFilter{})
	if err != nil {
		t.Fatalf("ReadLogs failed: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("ReadLogs(WARN) returned %d entries, want 2 (WARN, ERROR)", len(entries))
	}
	for _, e := range entries {
		if e.Level() != logsys.WarnLevel && e.Level() != logsys.ErrorLevel {
			t.Fatalf("unexpected level in filtered results: %s", e.Level())
		}
	}
}

func TestWriteLogAfterCloseFails(t *testing.T) {
	store := memory.New()
	mgr := logsys.NewManager(store, store, store)
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := mgr.WriteLog("INFO", "should not be accepted"); err != logsys.ErrClosed {
		t.Fatalf("WriteLog after Close = %v, want ErrClosed", err)
	}
}

// TestRegisterLogHandlerUnregister verifies the returned unregister
// func actually stops a handler from receiving further entries — this
// is what makes a WebSocket client's handler safe to tie to its
// connection lifetime instead of leaking for the life of the Manager.
func TestRegisterLogHandlerUnregister(t *testing.T) {
	store := memory.New()
	mgr := logsys.NewManager(store, store, store)

	kept := &countingHandler{}
	removed := &countingHandler{}
	mgr.RegisterLogHandler(kept)
	unregister := mgr.RegisterLogHandler(removed)

	if err := mgr.WriteLog("INFO", "first"); err != nil {
		t.Fatalf("WriteLog failed: %v", err)
	}

	// WriteLog only enqueues; wait for the async write-loop to actually
	// have delivered "first" to both handlers before unregistering one,
	// or unregister could race ahead of delivery.
	deadline := time.Now().Add(2 * time.Second)
	for removed.Count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if removed.Count() == 0 {
		t.Fatal("removed handler never saw the first entry")
	}

	unregister()

	if err := mgr.WriteLog("INFO", "second"); err != nil {
		t.Fatalf("WriteLog failed: %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if got := kept.Count(); got != 2 {
		t.Fatalf("kept handler saw %d entries, want 2 (never unregistered)", got)
	}
	if got := removed.Count(); got != 1 {
		t.Fatalf("removed handler saw %d entries, want 1 (unregistered after the first)", got)
	}
}

// TestConcurrentWriteAndHandlerRegistration exercises WriteLog and
// RegisterLogHandler from many goroutines at once; run with -race to
// verify the RWMutex-guarded close and handler slice are actually safe.
func TestConcurrentWriteAndHandlerRegistration(t *testing.T) {
	store := memory.New()
	mgr := logsys.NewManager(store, store, store)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_ = mgr.WriteLog("INFO", "concurrent")
		})
	}
	wg.Go(func() {
		mgr.RegisterLogHandler(&countingHandler{})
	})
	wg.Wait()

	if err := mgr.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestClearLogsBoundary(t *testing.T) {
	store := memory.New()
	cutoff := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	before := logsys.NewEntry(cutoff.Add(-time.Hour), logsys.InfoLevel, "before cutoff", nil)
	atCutoff := logsys.NewEntry(cutoff, logsys.InfoLevel, "at cutoff", nil)
	after := logsys.NewEntry(cutoff.Add(time.Hour), logsys.InfoLevel, "after cutoff", nil)

	for _, e := range []logsys.LogEntry{before, atCutoff, after} {
		if err := store.Write(e); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := store.Clear(cutoff); err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	remaining, err := store.Read(logsys.DebugLevel, logsys.LogFilter{})
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(remaining) != 2 {
		t.Fatalf("got %d remaining entries, want 2 (at cutoff, after cutoff)", len(remaining))
	}
	for _, e := range remaining {
		if e.Message() == "before cutoff" {
			t.Fatal("entry before cutoff should have been cleared")
		}
	}
}

// blockingWriter stalls every Write until release is closed, letting the
// DropNewest test force the queue to stay full deterministically.
type blockingWriter struct {
	release chan struct{}
}

func (w *blockingWriter) Write(logsys.LogEntry) error {
	<-w.release
	return nil
}

func (w *blockingWriter) Close() error { return nil }

func TestDropNewestPolicyDoesNotBlock(t *testing.T) {
	bw := &blockingWriter{release: make(chan struct{})}
	defer close(bw.release)

	store := memory.New()
	mgr := logsys.NewManager(bw, store, store,
		logsys.WithQueueSize(1),
		logsys.WithDropPolicy(logsys.DropNewest),
	)

	done := make(chan struct{})
	go func() {
		for range 20 {
			_ = mgr.WriteLog("INFO", "burst")
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("WriteLog blocked despite DropNewest policy")
	}

	if mgr.Dropped() == 0 {
		t.Fatal("expected at least one dropped entry when the writer stalls under DropNewest")
	}
}
