// Command server runs logGO as its own log-aggregation service, Dozzle
// style: any number of remote instances can be added and removed at
// runtime (see internal/sources), each ingested independently as a
// client of its existing log-reading endpoints (internal/ingest), with
// everything collected stored in one logGO.Manager of its own and
// served over a small UI + JSON/WebSocket API. This is the "own UI
// page, own integration" logGO is meant to demonstrate — a real,
// independent project, not a library conTogether reaches into.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	logfile "github.com/ttfancy/logGO/backends/file"

	"github.com/ttfancy/logGO"
	"github.com/ttfancy/logGO/internal/sources"
	"github.com/ttfancy/logGO/internal/webui"
)

func main() {
	cfg := loadConfig()

	store, err := logfile.Open(cfg.LogFilePath)
	if err != nil {
		log.Fatalf("open log file: %v", err)
	}
	manager := logGO.NewManager(store, store, store)
	defer manager.Close()

	registry, err := sources.NewRegistry(cfg.SourcesFilePath, manager)
	if err != nil {
		log.Fatalf("load sources: %v", err)
	}
	registerConfiguredSource(cfg, registry, manager)

	uiHandler, err := webui.Handler()
	if err != nil {
		log.Fatalf("load UI: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /", uiHandler)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /entries", handleListEntries(manager))
	mux.HandleFunc("GET /ws/entries", handleWatchEntries(manager))
	mux.HandleFunc("GET /sources", handleListSources(registry))
	mux.HandleFunc("POST /sources", handleAddSource(registry))
	mux.HandleFunc("DELETE /sources/{id}", handleRemoveSource(registry))

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()
	_ = manager.WriteLog("INFO", "logGO server listening", logGO.F("addr", srv.Addr))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = manager.WriteLog("ERROR", "http server shutdown error", logGO.F("error", err.Error()))
	}
}

// registerConfiguredSource is the backward-compatible on-ramp: if
// CONTOGETHER_URL/CONTOGETHER_API_KEY are set (the only way to point
// logGO anywhere in the previous single-source version), it's
// auto-registered as an ordinary source at boot — unless a source
// pointing at the same URL is already persisted from a previous run,
// so restarting doesn't pile up duplicate registrations of the same
// instance.
func registerConfiguredSource(cfg *config, registry *sources.Registry, manager *logGO.Manager) {
	if cfg.ContogetherURL == "" || cfg.ContogetherAPIKey == "" {
		return
	}
	for _, s := range registry.List() {
		if s.BaseURL == cfg.ContogetherURL {
			return
		}
	}
	if _, err := registry.Add(cfg.SourceName, cfg.ContogetherURL, cfg.ContogetherAPIKey); err != nil {
		_ = manager.WriteLog("ERROR", "failed to auto-register CONTOGETHER_URL as a source", logGO.F("error", err.Error()))
	}
}
