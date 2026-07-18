// Package ingest is logGO's integration with a remote conTogether
// instance: it connects as a plain HTTP/WebSocket *client* of
// conTogether's already-existing log-reading endpoints (GET /logs,
// GET /ws/logs) and feeds what it collects into logGO's own Manager.
// This is deliberately the only relationship between the two projects
// — logGO never imports conTogether's code, and conTogether never
// imports logGO's; they integrate over the network, the same way any
// external log aggregator (Loki, Graylog, ...) would.
package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/ttfancy/logGO"
)

// Options configures a connection to one conTogether instance.
type Options struct {
	// BaseURL is conTogether's own address, e.g. "http://localhost:8080".
	BaseURL string
	// APIKey authenticates against conTogether's existing auth — the
	// same key you'd use to call its API directly.
	APIKey string
	// Source tags every ingested entry (as a field) with where it came
	// from, so logGO's UI can distinguish it from entries logGO writes
	// about itself, or from any other instance it's ever ingested.
	Source string
}

type remoteEntry struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields"`
}

// Run connects to the configured conTogether instance and feeds every
// log entry it can reach into manager: first its existing history (via
// GET /logs), then everything new as it happens (via the /ws/logs live
// tail). It blocks until ctx is canceled, reconnecting the WebSocket
// with backoff — and re-backfilling from the last entry actually
// ingested — if the connection drops.
func Run(ctx context.Context, opts Options, manager *logGO.Manager) error {
	lastSeen, err := backfill(ctx, opts, manager, time.Time{})
	if err != nil {
		_ = manager.WriteLog("WARN", "initial backfill from conTogether failed",
			logGO.F("error", err.Error()), logGO.F("source", opts.Source))
	}

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		seen, tailErr := tail(ctx, opts, manager, lastSeen)
		if seen.After(lastSeen) {
			lastSeen = seen
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		_ = manager.WriteLog("WARN", "conTogether live tail disconnected, retrying",
			logGO.F("error", errString(tailErr)), logGO.F("source", opts.Source))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}

		// Fill whatever gap opened up while disconnected before resuming
		// the live tail.
		if seen, err := backfill(ctx, opts, manager, lastSeen); err == nil && seen.After(lastSeen) {
			lastSeen = seen
		}
		backoff = time.Second // a successful reconnect resets it
	}
}

func errString(err error) string {
	if err == nil {
		return "connection closed"
	}
	return err.Error()
}

// backfill pulls every entry conTogether has after since (exclusive
// isn't guaranteed at the boundary — the /logs endpoint's `since` is
// inclusive, so the entry timestamped exactly at `since` may be
// re-ingested once; harmless duplication, not worth the complexity of
// exact-boundary tracking here) and returns the newest timestamp seen.
func backfill(ctx context.Context, opts Options, manager *logGO.Manager, since time.Time) (time.Time, error) {
	u, err := url.Parse(strings.TrimSuffix(opts.BaseURL, "/") + "/logs")
	if err != nil {
		return since, fmt.Errorf("parse base URL: %w", err)
	}
	if !since.IsZero() {
		q := u.Query()
		q.Set("since", since.UTC().Format(time.RFC3339Nano))
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return since, err
	}
	req.Header.Set("X-API-Key", opts.APIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return since, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return since, fmt.Errorf("GET /logs: %s: %s", resp.Status, body)
	}

	var entries []remoteEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return since, fmt.Errorf("decode /logs response: %w", err)
	}

	newest := since
	for _, re := range entries {
		ingest(manager, opts.Source, re)
		if re.Timestamp.After(newest) {
			newest = re.Timestamp
		}
	}
	return newest, nil
}

// tail live-tails conTogether's /ws/logs until the connection drops or
// ctx is canceled, ingesting every entry as it arrives. It returns the
// newest timestamp actually ingested, so the caller can resume a
// backfill from exactly that point after a reconnect.
func tail(ctx context.Context, opts Options, manager *logGO.Manager, lastSeen time.Time) (time.Time, error) {
	wsURL, err := toWebSocketURL(opts.BaseURL, opts.APIKey)
	if err != nil {
		return lastSeen, err
	}

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return lastSeen, fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.CloseNow()

	newest := lastSeen
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return newest, err
		}
		var re remoteEntry
		if err := json.Unmarshal(data, &re); err != nil {
			continue // skip a malformed message rather than drop the connection over it
		}
		ingest(manager, opts.Source, re)
		if re.Timestamp.After(newest) {
			newest = re.Timestamp
		}
	}
}

func ingest(manager *logGO.Manager, source string, re remoteEntry) {
	fields := re.Fields
	if fields == nil {
		fields = make(map[string]any, 1)
	}
	fields["source"] = source
	_ = manager.WriteEntry(logGO.NewEntry(re.Timestamp, logGO.Level(re.Level), re.Message, fields))
}

// toWebSocketURL turns conTogether's HTTP base URL into its /ws/logs
// WebSocket URL, with the API key as a query param — browsers can't set
// custom headers on a WS handshake, and conTogether's own auth
// (internal/wsstream) already expects it there for exactly that reason,
// so this client authenticates the same way.
func toWebSocketURL(baseURL, apiKey string) (string, error) {
	u, err := url.Parse(strings.TrimSuffix(baseURL, "/") + "/ws/logs")
	if err != nil {
		return "", fmt.Errorf("parse base URL: %w", err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	q := u.Query()
	q.Set("api_key", apiKey)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
