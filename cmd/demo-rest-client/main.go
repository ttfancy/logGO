// Command demo-rest-client is the simplest possible way to push entries
// into logGO: a plain JSON POST to /ingest, repeated on a timer. One of
// three demo clients (rest, ws, grpc) showing logGO accepting entries
// over different protocols side by side — see the root README.
package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"
)

type entry struct {
	Source  string         `json:"source"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

func main() {
	loggoURL := stringEnv("LOGGO_URL", "http://localhost:9090")
	source := stringEnv("SOURCE_NAME", "demo-rest-client")
	interval := durationEnv("INTERVAL", 3*time.Second)

	var n int
	for {
		n++
		e := entry{
			Source:  source,
			Level:   "INFO",
			Message: "hello from the REST demo client",
			Fields:  map[string]any{"protocol": "rest", "n": n},
		}
		if err := push(loggoURL, e); err != nil {
			log.Printf("push failed: %v", err)
		} else {
			log.Printf("pushed entry #%d via REST", n)
		}
		time.Sleep(interval)
	}
}

func push(loggoURL string, e entry) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}
	resp, err := http.Post(loggoURL+"/ingest", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		log.Printf("unexpected status: %s", resp.Status)
	}
	return nil
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
