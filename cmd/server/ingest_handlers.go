package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"

	"github.com/ttfancy/logGO"
	ingestv1 "github.com/ttfancy/logGO/internal/genproto/ingest/v1"
)

// ingestPayload is the REST/WebSocket wire shape for a pushed entry —
// deliberately the same shape /entries already returns, so a client
// could in principle echo one straight back in. Timestamp is optional;
// omitting it (the common case for a client just logging as it happens)
// defaults to time.Now(), the same as logGO's own WriteLog.
type ingestPayload struct {
	Source    string         `json:"source"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
	Timestamp *time.Time     `json:"timestamp,omitempty"`
}

// ingest writes p into manager, tagging it with its source the same
// way a pulled (internal/ingest) entry is — the "source" field is what
// the UI's sidebar/filtering keys on, regardless of whether the entry
// arrived by logGO pulling from a registered instance or a client
// pushing it directly.
func ingest(manager *logGO.Manager, p ingestPayload) error {
	ts := time.Now()
	if p.Timestamp != nil {
		ts = *p.Timestamp
	}
	fields := p.Fields
	if fields == nil {
		fields = make(map[string]any, 1)
	}
	fields["source"] = p.Source
	return manager.WriteEntry(logGO.NewEntry(ts, logGO.Level(strings.ToUpper(p.Level)), p.Message, fields))
}

// handleIngestREST is push-ingestion protocol #1: a plain JSON POST,
// the simplest possible way for any HTTP client to ship logGO an entry.
func handleIngestREST(manager *logGO.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p ingestPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := ingest(manager, p); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

// handleIngestWS is push-ingestion protocol #2: a persistent WebSocket
// a client can keep open and send one JSON entry (the same shape as
// the REST payload) per message — no per-write HTTP overhead, useful
// for a client emitting a steady stream of entries.
func handleIngestWS(manager *logGO.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		// Deliberately NOT conn.CloseRead here: that puts a connection
		// into send-only mode (it discards incoming data frames itself,
		// only handling ping/pong control frames) — right for the
		// server-only-writes log tails elsewhere in this codebase, wrong
		// here, where the whole point is the server *reading* entries
		// the client sends.
		ctx := r.Context()

		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var p ingestPayload
			if json.Unmarshal(data, &p) == nil {
				_ = ingest(manager, p)
			}
			// A malformed message is dropped rather than closing the
			// connection over it — one bad line from a client shouldn't
			// end an otherwise-healthy long-lived stream.
		}
	}
}

// ingestServiceHandler is push-ingestion protocol #3: gRPC/Connect,
// implementing ingestv1connect.IngestServiceHandler. Structured and
// schema'd, unlike the JSON protocols above — the tradeoff a client
// gets for using it.
type ingestServiceHandler struct {
	manager *logGO.Manager
}

func (h *ingestServiceHandler) Ingest(_ context.Context, req *connect.Request[ingestv1.IngestRequest]) (*connect.Response[ingestv1.IngestResponse], error) {
	entry := req.Msg.GetEntry()
	fields := make(map[string]any, len(entry.GetFields()))
	for _, f := range entry.GetFields() {
		var v any
		if json.Unmarshal([]byte(f.GetValueJson()), &v) == nil {
			fields[f.GetKey()] = v
		} else {
			fields[f.GetKey()] = f.GetValueJson()
		}
	}
	ts := time.Now()
	if entry.GetTimestampUnixNano() != 0 {
		ts = time.Unix(0, entry.GetTimestampUnixNano())
	}
	if err := ingest(h.manager, ingestPayload{
		Source:    req.Msg.GetSource(),
		Level:     entry.GetLevel(),
		Message:   entry.GetMessage(),
		Fields:    fields,
		Timestamp: &ts,
	}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&ingestv1.IngestResponse{}), nil
}
