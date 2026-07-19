#!/usr/bin/env bash
# Runs all three push-ingestion demo clients (REST, WebSocket, gRPC)
# against a running logGO instance, registering each as a named push
# source first so they show up under a friendly name and protocol
# badge instead of a raw source string. Ctrl+C stops the clients and
# removes their registrations, leaving logGO exactly as it was.
set -euo pipefail

LOGGO_URL="${LOGGO_URL:-http://localhost:9090}"
INTERVAL="${INTERVAL:-2s}"

if ! curl -sf "$LOGGO_URL/healthz" > /dev/null; then
  echo "logGO isn't reachable at $LOGGO_URL — start it first: go run ./cmd/server" >&2
  exit 1
fi

register() {
  local name=$1 protocol=$2
  curl -sf -X POST "$LOGGO_URL/sources" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"$name\",\"kind\":\"push\",\"protocol\":\"$protocol\"}" \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4
}

echo "Registering demo push sources on $LOGGO_URL..."
REST_ID=$(register demo-rest-client rest)
WS_ID=$(register demo-ws-client websocket)
GRPC_ID=$(register demo-grpc-client grpc)
echo "  demo-rest-client -> $REST_ID"
echo "  demo-ws-client   -> $WS_ID"
echo "  demo-grpc-client -> $GRPC_ID"

cd "$(dirname "$0")/.."

echo "Building demo clients..."
BIN_DIR=$(mktemp -d)
go build -o "$BIN_DIR/demo-rest" ./cmd/demo-rest-client
go build -o "$BIN_DIR/demo-ws" ./cmd/demo-ws-client
go build -o "$BIN_DIR/demo-grpc" ./cmd/demo-grpc-client

PIDS=()
CLEANED_UP=0
cleanup() {
  [ "$CLEANED_UP" = 1 ] && return
  CLEANED_UP=1
  echo
  echo "Stopping demo clients and removing their registrations..."
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for id in "$REST_ID" "$WS_ID" "$GRPC_ID"; do
    curl -sf -X DELETE "$LOGGO_URL/sources/$id" > /dev/null || true
  done
  rm -rf "$BIN_DIR"
}
trap cleanup EXIT INT TERM

LOGGO_URL="$LOGGO_URL" SOURCE_NAME="$REST_ID" INTERVAL="$INTERVAL" "$BIN_DIR/demo-rest" &
PIDS+=($!)
LOGGO_URL="$LOGGO_URL" SOURCE_NAME="$WS_ID" INTERVAL="$INTERVAL" "$BIN_DIR/demo-ws" &
PIDS+=($!)
LOGGO_URL="$LOGGO_URL" SOURCE_NAME="$GRPC_ID" INTERVAL="$INTERVAL" "$BIN_DIR/demo-grpc" &
PIDS+=($!)

echo
echo "Demo clients running — open $LOGGO_URL to watch them arrive live."
echo "Press Ctrl+C to stop and clean up."
wait
