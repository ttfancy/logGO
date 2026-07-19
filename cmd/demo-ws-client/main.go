// Command demo-ws-client pushes entries into logGO over a persistent
// WebSocket (/ws/ingest) — one connection kept open, one JSON message
// per entry, instead of a new HTTP request each time. One of three demo
// clients (rest, ws, grpc) showing logGO accepting entries over
// different protocols side by side — see the root README.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type entry struct {
	Source  string         `json:"source"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

func main() {
	loggoURL := stringEnv("LOGGO_URL", "http://localhost:9090")
	source := stringEnv("SOURCE_NAME", "demo-ws-client")
	interval := durationEnv("INTERVAL", 3*time.Second)
	wsURL := toWebSocketURL(loggoURL) + "/ws/ingest"

	ctx := context.Background()
	conn := dialWithRetry(ctx, wsURL)
	defer conn.CloseNow()

	var n int
	for {
		n++
		e := entry{
			Source:  source,
			Level:   "INFO",
			Message: "hello from the WebSocket demo client",
			Fields:  map[string]any{"protocol": "ws", "n": n},
		}
		body, _ := json.Marshal(e)
		if err := conn.Write(ctx, websocket.MessageText, body); err != nil {
			log.Printf("write failed, reconnecting: %v", err)
			conn.CloseNow()
			conn = dialWithRetry(ctx, wsURL)
			continue
		}
		log.Printf("pushed entry #%d via WebSocket", n)
		time.Sleep(interval)
	}
}

func dialWithRetry(ctx context.Context, wsURL string) *websocket.Conn {
	backoff := time.Second
	for {
		conn, _, err := websocket.Dial(ctx, wsURL, nil)
		if err == nil {
			log.Printf("connected to %s", wsURL)
			return conn
		}
		log.Printf("dial failed, retrying in %s: %v", backoff, err)
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func toWebSocketURL(httpURL string) string {
	if strings.HasPrefix(httpURL, "https://") {
		return "wss://" + strings.TrimPrefix(httpURL, "https://")
	}
	return "ws://" + strings.TrimPrefix(httpURL, "http://")
}

func stringEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func durationEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
