// Command demo-grpc-client pushes entries into logGO over real gRPC
// (not Connect's own JSON protocol — connect.WithGRPC() forces the
// actual gRPC wire format, over cleartext HTTP/2 via h2c since there's
// no TLS in this demo setup). One of three demo clients (rest, ws,
// grpc) showing logGO accepting entries over different protocols side
// by side — see the root README.
package main

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"

	ingestv1 "github.com/ttfancy/logGO/internal/genproto/ingest/v1"
	"github.com/ttfancy/logGO/internal/genproto/ingest/v1/ingestv1connect"
)

func main() {
	loggoURL := stringEnv("LOGGO_URL", "http://localhost:9090")
	source := stringEnv("SOURCE_NAME", "demo-grpc-client")
	interval := durationEnv("INTERVAL", 3*time.Second)

	// A plain http.Client speaks HTTP/1.1; real gRPC needs HTTP/2. There's
	// no TLS in this demo, so h2c (cleartext HTTP/2) is what makes a real
	// gRPC client work against the server's h2c.NewHandler wrapping —
	// see cmd/server/main.go.
	httpClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
	client := ingestv1connect.NewIngestServiceClient(httpClient, loggoURL, connect.WithGRPC())

	var n int
	for {
		n++
		req := connect.NewRequest(&ingestv1.IngestRequest{
			Source: source,
			Entry: &ingestv1.LogEntry{
				TimestampUnixNano: time.Now().UnixNano(),
				Level:             "INFO",
				Message:           "hello from the gRPC demo client",
				Fields: []*ingestv1.Field{
					{Key: "protocol", ValueJson: `"grpc"`},
					{Key: "n", ValueJson: strconv.Itoa(n)},
				},
			},
		})
		if _, err := client.Ingest(context.Background(), req); err != nil {
			log.Printf("Ingest failed: %v", err)
		} else {
			log.Printf("pushed entry #%d via gRPC", n)
		}
		time.Sleep(interval)
	}
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
