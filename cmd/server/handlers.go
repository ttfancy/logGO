package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/ttfancy/logGO"
	"github.com/ttfancy/logGO/internal/sources"
)

type entryJSON struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

func toEntryJSON(e logGO.LogEntry) entryJSON {
	return entryJSON{Timestamp: e.Timestamp(), Level: string(e.Level()), Message: e.Message(), Fields: e.Fields()}
}

// entrySourceID reads the "source" field ingest.go tags every entry
// with (the source's ID, not its display name — see internal/sources'
// doc comment on why) — "" for an entry logGO wrote about itself, which
// never goes through ingestion.
func entrySourceID(e logGO.LogEntry) string {
	v, _ := e.Fields()["source"].(string)
	return v
}

// maxEntries caps both /entries and the initial ReadLogs behind
// /ws/entries at the most recent N — a long-running instance
// accumulates entries without bound, and rendering however many
// thousand have ever been seen isn't useful or fast.
const maxEntries = 500

func handleListEntries(manager *logGO.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		level := r.URL.Query().Get("level")
		if level == "" {
			level = "DEBUG"
		}
		sourceID := r.URL.Query().Get("source_id")

		entries, err := manager.ReadLogs(level, logGO.LogFilter{Contains: r.URL.Query().Get("contains")})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if sourceID != "" {
			filtered := entries[:0:0]
			for _, e := range entries {
				if entrySourceID(e) == sourceID {
					filtered = append(filtered, e)
				}
			}
			entries = filtered
		}
		if len(entries) > maxEntries {
			entries = entries[len(entries)-maxEntries:]
		}

		out := make([]entryJSON, len(entries))
		for i, e := range entries {
			out[i] = toEntryJSON(e)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

// handleWatchEntries upgrades to a WebSocket and pushes every new
// entry as it's written (built on Manager.RegisterLogHandler — the
// same extension point conTogether's own WebSocket log tail uses),
// filtered by the same level/contains/source_id query params /entries
// accepts. It also sends the current backlog first (capped at
// maxEntries) so a client doesn't start from a blank slate.
func handleWatchEntries(manager *logGO.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		level := strings.ToUpper(r.URL.Query().Get("level"))
		if level == "" {
			level = "DEBUG"
		}
		contains := r.URL.Query().Get("contains")
		sourceID := r.URL.Query().Get("source_id")

		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx := conn.CloseRead(context.Background())

		backlog, err := manager.ReadLogs(level, logGO.LogFilter{Contains: contains})
		if err == nil {
			if sourceID != "" {
				filtered := backlog[:0:0]
				for _, e := range backlog {
					if entrySourceID(e) == sourceID {
						filtered = append(filtered, e)
					}
				}
				backlog = filtered
			}
			if len(backlog) > maxEntries {
				backlog = backlog[len(backlog)-maxEntries:]
			}
			for _, e := range backlog {
				if writeEntry(ctx, conn, e) != nil {
					return
				}
			}
		}

		live := make(chan logGO.LogEntry, 64)
		unregister := manager.RegisterLogHandler(logGO.LogHandlerFunc(func(e logGO.LogEntry) {
			if !logGO.LevelAtLeast(e.Level(), logGO.Level(level)) {
				return
			}
			if contains != "" && !strings.Contains(e.Message(), contains) {
				return
			}
			if sourceID != "" && entrySourceID(e) != sourceID {
				return
			}
			select {
			case live <- e:
			default:
				// A slow/stuck client must not block the shared write
				// loop every other write goes through — drop for this
				// client instead, same reasoning as conTogether's own
				// app-log WebSocket tail.
			}
		}))
		defer unregister()

		for {
			select {
			case <-ctx.Done():
				return
			case e := <-live:
				if writeEntry(ctx, conn, e) != nil {
					return
				}
			}
		}
	}
}

func writeEntry(ctx context.Context, conn *websocket.Conn, e logGO.LogEntry) error {
	data, err := json.Marshal(toEntryJSON(e))
	if err != nil {
		return nil // skip a single bad entry rather than drop the connection over it
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, data)
}

func handleListSources(registry *sources.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(registry.List())
	}
}

// addSourceRequest covers both directions: Kind == "push" only reads
// Name/Protocol, anything else (including the empty default, for
// back-compat with clients that predate push sources) only reads
// Name/BaseURL/APIKey.
type addSourceRequest struct {
	Name     string `json:"name"`
	Kind     string `json:"kind,omitempty"`
	BaseURL  string `json:"base_url,omitempty"`
	APIKey   string `json:"api_key,omitempty"`
	Protocol string `json:"protocol,omitempty"`
}

func handleAddSource(registry *sources.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req addSourceRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid request body: %s", err), http.StatusBadRequest)
			return
		}
		var (
			added sources.Source
			err   error
		)
		if req.Kind == sources.KindPush {
			added, err = registry.AddPush(req.Name, req.Protocol)
		} else {
			added, err = registry.Add(req.Name, req.BaseURL, req.APIKey)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(added)
	}
}

func handleRemoveSource(registry *sources.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if err := registry.Remove(id); err != nil {
			if errors.Is(err, sources.ErrNotFound) {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
