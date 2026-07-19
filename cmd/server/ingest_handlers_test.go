package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"

	"github.com/ttfancy/logGO"
	"github.com/ttfancy/logGO/backends/memory"
	ingestv1 "github.com/ttfancy/logGO/internal/genproto/ingest/v1"
)

func testManager(t *testing.T) *logGO.Manager {
	t.Helper()
	store := memory.New()
	mgr := logGO.NewManager(store, store, store)
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

func waitForEntry(t *testing.T, manager *logGO.Manager, message string) logGO.LogEntry {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := manager.ReadLogs("DEBUG", logGO.LogFilter{})
		if err != nil {
			t.Fatalf("ReadLogs failed: %v", err)
		}
		for _, e := range entries {
			if e.Message() == message {
				return e
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("entry %q never appeared", message)
	return nil
}

func TestHandleIngestRESTWritesTaggedEntry(t *testing.T) {
	manager := testManager(t)
	srv := httptest.NewServer(handleIngestREST(manager))
	defer srv.Close()

	body, _ := json.Marshal(ingestPayload{
		Source: "demo-rest-client", Level: "INFO", Message: "hello via rest",
		Fields: map[string]any{"n": 1.0},
	})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}

	e := waitForEntry(t, manager, "hello via rest")
	if e.Fields()["source"] != "demo-rest-client" {
		t.Fatalf("fields = %+v, want source=demo-rest-client", e.Fields())
	}
	if e.Fields()["n"] != 1.0 {
		t.Fatalf("fields = %+v, want n=1.0 preserved", e.Fields())
	}
}

func TestHandleIngestWSWritesTaggedEntry(t *testing.T) {
	manager := testManager(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws/ingest", handleIngestWS(manager))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wsURL := "ws" + srv.URL[len("http"):] + "/ws/ingest"
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.CloseNow()

	body, _ := json.Marshal(ingestPayload{Source: "demo-ws-client", Level: "INFO", Message: "hello via ws"})
	if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	e := waitForEntry(t, manager, "hello via ws")
	if e.Fields()["source"] != "demo-ws-client" {
		t.Fatalf("fields = %+v, want source=demo-ws-client", e.Fields())
	}
}

func TestIngestServiceHandlerWritesTaggedEntry(t *testing.T) {
	manager := testManager(t)
	handler := &ingestServiceHandler{manager: manager}

	req := connect.NewRequest(&ingestv1.IngestRequest{
		Source: "demo-grpc-client",
		Entry: &ingestv1.LogEntry{
			Level:   "INFO",
			Message: "hello via grpc",
			Fields: []*ingestv1.Field{
				{Key: "n", ValueJson: "42"},
				{Key: "nested", ValueJson: `{"a":1}`},
			},
		},
	})
	if _, err := handler.Ingest(context.Background(), req); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	e := waitForEntry(t, manager, "hello via grpc")
	if e.Fields()["source"] != "demo-grpc-client" {
		t.Fatalf("fields = %+v, want source=demo-grpc-client", e.Fields())
	}
	if e.Fields()["n"] != 42.0 {
		t.Fatalf("fields[n] = %v, want 42 (decoded from JSON)", e.Fields()["n"])
	}
	nested, ok := e.Fields()["nested"].(map[string]any)
	if !ok || nested["a"] != 1.0 {
		t.Fatalf("fields[nested] = %+v, want a nested object with a=1", e.Fields()["nested"])
	}
}

// TestIngestServiceHandlerPreservesExplicitTimestamp confirms a pushed
// gRPC entry's own timestamp survives, not just internal/ingest's
// pulled entries — the same WriteEntry-not-WriteLog reasoning applies
// here: a client pushing logs may be relaying something that already
// happened, not just logging live.
func TestIngestServiceHandlerPreservesExplicitTimestamp(t *testing.T) {
	manager := testManager(t)
	handler := &ingestServiceHandler{manager: manager}

	past := time.Now().Add(-time.Hour).Truncate(time.Second)
	req := connect.NewRequest(&ingestv1.IngestRequest{
		Source: "demo-grpc-client",
		Entry: &ingestv1.LogEntry{
			TimestampUnixNano: past.UnixNano(),
			Level:             "INFO",
			Message:           "backdated via grpc",
		},
	})
	if _, err := handler.Ingest(context.Background(), req); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	e := waitForEntry(t, manager, "backdated via grpc")
	if !e.Timestamp().Equal(past) {
		t.Fatalf("timestamp = %v, want the explicit %v", e.Timestamp(), past)
	}
}
