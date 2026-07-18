package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/ttfancy/logGO"
	"github.com/ttfancy/logGO/backends/memory"
	"github.com/ttfancy/logGO/internal/ingest"
)

type remoteEntryJSON struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// fakeContogether stands in for a real conTogether instance: GET /logs
// returns a fixed history, GET /ws/logs sends one live message once a
// client connects, so a test can exercise both halves of ingest.Run
// (backfill, then live tail) without a real conTogether process.
func fakeContogether(t *testing.T, history []remoteEntryJSON, live remoteEntryJSON, gotLiveMsg chan<- struct{}) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /logs", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(history)
	})
	mux.HandleFunc("GET /ws/logs", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("api_key") != "test-key" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		data, _ := json.Marshal(live)
		conn.Write(r.Context(), websocket.MessageText, data)
		close(gotLiveMsg)
		<-r.Context().Done()
	})
	return httptest.NewServer(mux)
}

func TestRunIngestsBackfillThenLiveEntry(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	history := []remoteEntryJSON{
		{Timestamp: base, Level: "INFO", Message: "historical entry", Fields: map[string]any{"path": "/healthz"}},
	}
	live := remoteEntryJSON{Timestamp: time.Now(), Level: "ERROR", Message: "live entry"}
	gotLiveMsg := make(chan struct{})

	srv := fakeContogether(t, history, live, gotLiveMsg)
	defer srv.Close()

	store := memory.New()
	manager := logGO.NewManager(store, store, store)
	defer manager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- ingest.Run(ctx, ingest.Options{BaseURL: srv.URL, APIKey: "test-key", Source: "conTogether"}, manager)
	}()

	select {
	case <-gotLiveMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("fake conTogether never received a /ws/logs connection")
	}

	// Give the live message a moment to actually be ingested (Run's read
	// loop + WriteEntry are async relative to this goroutine).
	deadline := time.Now().Add(3 * time.Second)
	var entries []logGO.LogEntry
	for time.Now().Before(deadline) {
		var err error
		entries, err = manager.ReadLogs("DEBUG", logGO.LogFilter{})
		if err != nil {
			t.Fatalf("ReadLogs failed: %v", err)
		}
		if len(entries) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx was canceled")
	}

	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2 (backfilled + live)", len(entries))
	}
	for _, e := range entries {
		if e.Fields()["source"] != "conTogether" {
			t.Fatalf("entry %+v missing source=conTogether tag", e)
		}
	}

	var historical, liveEntry logGO.LogEntry
	for _, e := range entries {
		if e.Message() == "historical entry" {
			historical = e
		}
		if e.Message() == "live entry" {
			liveEntry = e
		}
	}
	if historical == nil || liveEntry == nil {
		t.Fatalf("expected both a historical and a live entry, got %+v", entries)
	}
	if !historical.Timestamp().Equal(base) {
		t.Fatalf("historical entry timestamp = %v, want the original %v (not re-stamped)", historical.Timestamp(), base)
	}
	if historical.Fields()["path"] != "/healthz" {
		t.Fatalf("historical entry lost its original fields: %+v", historical.Fields())
	}
}

func TestRunReturnsPromptlyWhenBackfillAuthFails(t *testing.T) {
	history := []remoteEntryJSON{{Timestamp: time.Now(), Level: "INFO", Message: "should not be reachable"}}
	srv := fakeContogether(t, history, remoteEntryJSON{}, make(chan struct{}))
	defer srv.Close()

	store := memory.New()
	manager := logGO.NewManager(store, store, store)
	defer manager.Close()

	// Wrong key: backfill fails (logged as a WARN, not fatal), and the
	// live tail will also fail to authenticate — Run should keep
	// retrying (with backoff) rather than giving up, and must still
	// respect context cancellation promptly.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := ingest.Run(ctx, ingest.Options{BaseURL: srv.URL, APIKey: "wrong-key", Source: "conTogether"}, manager)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want context.DeadlineExceeded", err)
	}
}
