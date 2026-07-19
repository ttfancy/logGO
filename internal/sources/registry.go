// Package sources is logGO's multi-service registry: rather than one
// static conTogether instance configured at boot, logGO can ingest from
// any number of remote instances added and removed at runtime, each
// with its own independent internal/ingest.Run goroutine, persisted to
// a small JSON file so they survive a restart.
package sources

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/ttfancy/logGO"
	"github.com/ttfancy/logGO/internal/ingest"
)

var ErrNotFound = errors.New("source not found")

// KindPull and KindPush are the two directions a Source can represent.
// A pull source is logGO actively connecting out to fetch someone
// else's logs (the original, and still default, design); a push source
// is the inverse — a name and an expected protocol registered up front
// so a client that pushes to /ingest, /ws/ingest, or the gRPC
// IngestService with a matching "source" ID shows up under a friendly
// name (and protocol badge) instead of just its raw tag.
const (
	KindPull = "pull"
	KindPush = "push"
)

// pushProtocols are the connection styles a push source can declare —
// descriptive only (shown in the UI so whoever configures the other
// service knows which endpoint/wire format to use); push ingestion
// itself doesn't enforce that a pushed entry's source actually used the
// declared protocol.
var pushProtocols = map[string]bool{"rest": true, "websocket": true, "grpc": true}

// Source is one registered source of logs, either pulled from (Kind ==
// KindPull, the default) or pushed to (Kind == KindPush). Every entry
// ingested or pushed under it is tagged with its ID (not Name — two
// sources could share a display name, but IDs are unique), so
// /entries and /ws/entries can filter to exactly one source
// unambiguously.
type Source struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Kind is "" for anything saved before this field existed —
	// treated as KindPull throughout (see (Source).kind), so an
	// existing sources.json from before push sources existed doesn't
	// need migrating.
	Kind    string `json:"kind,omitempty"`
	BaseURL string `json:"base_url,omitempty"`
	// APIKey is never sent back out over the registry's own API (see
	// List/Add's redacted copies) — only ever read from disk or from an
	// incoming Add request, never echoed.
	APIKey string `json:"api_key,omitempty"`
	// Protocol is push-only: "rest", "websocket", or "grpc".
	Protocol string `json:"protocol,omitempty"`
}

func (s Source) kind() string {
	if s.Kind == "" {
		return KindPull
	}
	return s.Kind
}

type running struct {
	Source
	cancel context.CancelFunc
	done   chan struct{}
}

// Registry manages the set of currently-ingesting sources.
type Registry struct {
	mu      sync.Mutex
	byID    map[string]*running
	manager *logGO.Manager
	path    string
}

// NewRegistry loads any previously-saved sources from path (a JSON
// file; missing is not an error, just an empty registry) and starts
// ingesting from each of them immediately.
func NewRegistry(path string, manager *logGO.Manager) (*Registry, error) {
	r := &Registry{byID: make(map[string]*running), manager: manager, path: path}
	saved, err := loadSources(path)
	if err != nil {
		return nil, err
	}
	for _, s := range saved {
		r.start(s)
	}
	return r, nil
}

func loadSources(path string) ([]Source, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Source
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return out, nil
}

// save persists the current source list — including API keys, unlike
// every in-memory response this package hands back over its own API —
// to disk with owner-only permissions, since it's the one place those
// keys live in cleartext. Caller must hold r.mu.
func (r *Registry) save() error {
	sources := make([]Source, 0, len(r.byID))
	for _, rs := range r.byID {
		sources = append(sources, rs.Source)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Name < sources[j].Name })

	data, err := json.MarshalIndent(sources, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.path, data, 0o600)
}

// start launches s's ingestion goroutine — unless s is a push source,
// which has nothing to connect out to (the other service connects to
// logGO, not the reverse), so it just occupies a registry slot with a
// no-op cancel/already-closed done. Caller must hold r.mu.
func (r *Registry) start(s Source) {
	if s.kind() == KindPush {
		done := make(chan struct{})
		close(done)
		r.byID[s.ID] = &running{Source: s, cancel: func() {}, done: done}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	r.byID[s.ID] = &running{Source: s, cancel: cancel, done: done}
	go func() {
		defer close(done)
		// ingest.Run only ever returns once ctx is canceled (its own
		// retry/backoff loop absorbs every recoverable failure) — so
		// this goroutine's lifetime is exactly the source's registered
		// lifetime, ended by Remove calling cancel().
		_ = ingest.Run(ctx, ingest.Options{BaseURL: s.BaseURL, APIKey: s.APIKey, Source: s.ID}, r.manager)
	}()
}

// Add registers a new pull source and starts ingesting from it
// immediately.
func (r *Registry) Add(name, baseURL, apiKey string) (Source, error) {
	if name == "" || baseURL == "" || apiKey == "" {
		return Source{}, fmt.Errorf("name, base_url, and api_key are all required")
	}
	id, err := randomID()
	if err != nil {
		return Source{}, err
	}
	s := Source{ID: id, Name: name, Kind: KindPull, BaseURL: baseURL, APIKey: apiKey}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.start(s)
	if err := r.save(); err != nil {
		if rs, ok := r.byID[id]; ok {
			delete(r.byID, id)
			rs.cancel()
		}
		return Source{}, fmt.Errorf("save sources: %w", err)
	}
	return redacted(s), nil
}

// AddPush registers a named push source: a client pushes entries to
// logGO's own /ingest, /ws/ingest, or gRPC IngestService, tagging them
// with the returned ID as "source" — the inverse of Add, where logGO
// is the one connecting out. protocol must be "rest", "websocket", or
// "grpc" (which endpoint the pushing client is expected to use; shown
// back to the caller so a UI can display connection instructions, not
// enforced against what actually arrives).
func (r *Registry) AddPush(name, protocol string) (Source, error) {
	if name == "" {
		return Source{}, fmt.Errorf("name is required")
	}
	if !pushProtocols[protocol] {
		return Source{}, fmt.Errorf("protocol must be one of rest, websocket, grpc")
	}
	id, err := randomID()
	if err != nil {
		return Source{}, err
	}
	s := Source{ID: id, Name: name, Kind: KindPush, Protocol: protocol}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.start(s)
	if err := r.save(); err != nil {
		delete(r.byID, id)
		return Source{}, fmt.Errorf("save sources: %w", err)
	}
	return redacted(s), nil
}

// Remove stops a source's ingestion and forgets it — its
// already-ingested entries stay in the Manager (removing a source isn't
// the same as deleting its history), only future ingestion stops.
func (r *Registry) Remove(id string) error {
	r.mu.Lock()
	rs, ok := r.byID[id]
	if !ok {
		r.mu.Unlock()
		return ErrNotFound
	}
	delete(r.byID, id)
	err := r.save()
	r.mu.Unlock()

	rs.cancel()
	<-rs.done
	return err
}

// List returns every registered source, API keys redacted, sorted by
// name.
func (r *Registry) List() []Source {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Source, 0, len(r.byID))
	for _, rs := range r.byID {
		out = append(out, redacted(rs.Source))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// NameForID returns the display name for a source ID (as tagged on
// ingested entries), or "" if it's not currently registered — e.g. an
// entry ingested from a source that's since been removed.
func (r *Registry) NameForID(id string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rs, ok := r.byID[id]; ok {
		return rs.Name
	}
	return ""
}

func redacted(s Source) Source {
	s.APIKey = ""
	return s
}

func randomID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
