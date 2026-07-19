package sources_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ttfancy/logGO"
	"github.com/ttfancy/logGO/backends/memory"
	"github.com/ttfancy/logGO/internal/sources"
)

// fakeInstance stands in for a remote conTogether-like instance: it just
// needs to answer GET /logs and GET /ws/logs well enough for
// internal/ingest.Run's backfill+tail loop to succeed without erroring
// (that loop's own correctness is covered in internal/ingest's own
// tests) — this package's tests are about the registry's lifecycle
// (add/list/remove/persist), not re-proving ingestion itself.
func fakeInstance(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /logs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	})
	mux.HandleFunc("GET /ws/logs", func(w http.ResponseWriter, r *http.Request) {
		// Deliberately never upgrades — the registry's own tests don't
		// need a real live tail, just for Add to not error out.
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func testManager(t *testing.T) *logGO.Manager {
	t.Helper()
	store := memory.New()
	mgr := logGO.NewManager(store, store, store)
	t.Cleanup(func() { mgr.Close() })
	return mgr
}

func TestAddListsRedactedAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	registry, err := sources.NewRegistry(path, testManager(t))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}

	instance := fakeInstance(t)
	added, err := registry.Add("staging", instance.URL, "secret-key")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if added.APIKey != "" {
		t.Fatalf("Add's response leaked the API key: %+v", added)
	}
	if added.Name != "staging" || added.BaseURL != instance.URL {
		t.Fatalf("unexpected source: %+v", added)
	}

	list := registry.List()
	if len(list) != 1 || list[0].ID != added.ID {
		t.Fatalf("List = %+v, want exactly the just-added source", list)
	}
	if list[0].APIKey != "" {
		t.Fatalf("List leaked the API key: %+v", list[0])
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected sources.json to be written: %v", err)
	}
	var onDisk []sources.Source
	if err := json.Unmarshal(data, &onDisk); err != nil {
		t.Fatalf("sources.json is not valid JSON: %v", err)
	}
	if len(onDisk) != 1 || onDisk[0].APIKey != "secret-key" {
		t.Fatalf("expected the real API key persisted to disk (not redacted there), got %+v", onDisk)
	}
}

func TestAddRejectsMissingFields(t *testing.T) {
	registry, err := sources.NewRegistry(filepath.Join(t.TempDir(), "sources.json"), testManager(t))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	if _, err := registry.Add("", "http://x", "key"); err == nil {
		t.Fatal("expected Add with an empty name to fail")
	}
	if _, err := registry.Add("name", "", "key"); err == nil {
		t.Fatal("expected Add with an empty base URL to fail")
	}
	if _, err := registry.Add("name", "http://x", ""); err == nil {
		t.Fatal("expected Add with an empty API key to fail")
	}
}

func TestRemoveStopsIngestionAndUpdatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	registry, err := sources.NewRegistry(path, testManager(t))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	instance := fakeInstance(t)
	added, err := registry.Add("staging", instance.URL, "secret-key")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if err := registry.Remove(added.ID); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if list := registry.List(); len(list) != 0 {
		t.Fatalf("List after Remove = %+v, want empty", list)
	}
	if name := registry.NameForID(added.ID); name != "" {
		t.Fatalf("NameForID after Remove = %q, want empty", name)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading sources.json failed: %v", err)
	}
	var onDisk []sources.Source
	if err := json.Unmarshal(data, &onDisk); err != nil {
		t.Fatalf("sources.json is not valid JSON: %v", err)
	}
	if len(onDisk) != 0 {
		t.Fatalf("expected sources.json to reflect the removal, got %+v", onDisk)
	}
}

func TestRemoveUnknownIDReturnsErrNotFound(t *testing.T) {
	registry, err := sources.NewRegistry(filepath.Join(t.TempDir(), "sources.json"), testManager(t))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	if err := registry.Remove("does-not-exist"); err != sources.ErrNotFound {
		t.Fatalf("Remove(unknown) = %v, want ErrNotFound", err)
	}
}

// TestNewRegistryReloadsPersistedSources is what actually proves
// persistence survives a restart: a fresh Registry pointed at the same
// file a previous one saved to must come back up already ingesting.
func TestNewRegistryReloadsPersistedSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	instance := fakeInstance(t)

	first, err := sources.NewRegistry(path, testManager(t))
	if err != nil {
		t.Fatalf("NewRegistry failed: %v", err)
	}
	added, err := first.Add("staging", instance.URL, "secret-key")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	second, err := sources.NewRegistry(path, testManager(t))
	if err != nil {
		t.Fatalf("second NewRegistry (reload) failed: %v", err)
	}
	list := second.List()
	if len(list) != 1 || list[0].ID != added.ID || list[0].Name != "staging" {
		t.Fatalf("reloaded registry's List = %+v, want the persisted source", list)
	}

	deadline := time.Now().Add(2 * time.Second)
	for second.NameForID(added.ID) == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if name := second.NameForID(added.ID); name != "staging" {
		t.Fatalf("NameForID after reload = %q, want %q", name, "staging")
	}
}
