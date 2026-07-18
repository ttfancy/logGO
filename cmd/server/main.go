// Command server runs logGO as its own log-aggregation service: it
// connects to a configured conTogether instance as a client of its
// existing log-reading endpoints (see internal/ingest), stores what it
// collects in a logGO.Manager of its own, and serves a small UI + JSON
// API over it. This is the "own UI page, own integration" logGO is
// meant to demonstrate — a real, independent project, not a library
// conTogether reaches into.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ttfancy/logGO"
	logfile "github.com/ttfancy/logGO/backends/file"
	"github.com/ttfancy/logGO/internal/ingest"
	"github.com/ttfancy/logGO/internal/webui"
)

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	store, err := logfile.Open(cfg.LogFilePath)
	if err != nil {
		log.Fatalf("open log file: %v", err)
	}
	manager := logGO.NewManager(store, store, store)
	defer manager.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		err := ingest.Run(ctx, ingest.Options{
			BaseURL: cfg.ContogetherURL,
			APIKey:  cfg.ContogetherAPIKey,
			Source:  cfg.SourceName,
		}, manager)
		if err != nil && ctx.Err() == nil {
			_ = manager.WriteLog("ERROR", "ingestion stopped unexpectedly", logGO.F("error", err.Error()))
		}
	}()

	uiHandler, err := webui.Handler()
	if err != nil {
		log.Fatalf("load UI: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /", uiHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /entries", func(w http.ResponseWriter, r *http.Request) {
		level := r.URL.Query().Get("level")
		if level == "" {
			level = "DEBUG"
		}
		entries, err := manager.ReadLogs(level, logGO.LogFilter{Contains: r.URL.Query().Get("contains")})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// A long-running instance accumulates entries without bound
		// (ReadLogs returns everything matching, oldest first); capping
		// to the most recent N here is what keeps the UI table (and this
		// response) from growing unboundedly instead of silently
		// rendering however many thousand entries have ever been seen.
		const maxEntries = 500
		if len(entries) > maxEntries {
			entries = entries[len(entries)-maxEntries:]
		}
		out := make([]entryJSON, len(entries))
		for i, e := range entries {
			out[i] = entryJSON{Timestamp: e.Timestamp(), Level: string(e.Level()), Message: e.Message(), Fields: e.Fields()}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()
	_ = manager.WriteLog("INFO", "logGO server listening", logGO.F("addr", srv.Addr), logGO.F("ingesting_from", cfg.ContogetherURL))

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = manager.WriteLog("ERROR", "http server shutdown error", logGO.F("error", err.Error()))
	}
}

type entryJSON struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}
