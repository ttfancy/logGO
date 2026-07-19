# logGO

A small, dependency-injected logging system: asynchronous writes, pluggable
storage, level filtering, and an extension point for things like remote log
aggregation — built around five interfaces (`LogEntry`, `LogWriter`,
`LogReader`, `LogClearer`, `LogHandler`) rather than one concrete logger
type.

See [`docs/diagrams/`](docs/diagrams/) for PlantUML diagrams of the structure
and call flows below: core interfaces/structure
([`01`](docs/diagrams/01-logGO-structure.puml)), the write
([`02`](docs/diagrams/02-logGO-write-sequence.puml)) and read
([`03`](docs/diagrams/03-logGO-read-sequence.puml)) paths, and the standalone
server's component layout ([`04`](docs/diagrams/04-server-components.puml)),
pull-ingestion sequence ([`05`](docs/diagrams/05-ingestion-sequence.puml)),
and push-ingestion sequence
([`06`](docs/diagrams/06-push-ingestion-sequence.puml)) — see "Standalone
server" below.

## Install

```
go get github.com/ttfancy/logGO
```

Originally developed as part of [conTogether](https://github.com/ttfancy/conTogether)
(its container management API's logging middleware), split out into its own
module so it can be versioned and imported independently.

## Interfaces

| Interface | Responsibility |
|---|---|
| `LogEntry` | Data structure of one log record (timestamp, level, message, fields) |
| `LogWriter` | Persist a single entry |
| `LogReader` | Query entries back out, filtered by minimum level + `LogFilter` |
| `LogClearer` | Purge entries older than a cutoff |
| `LogHandler` | Notified of every entry as it's written (extension point) |

`Manager` composes a `LogWriter` + `LogReader` + `LogClearer` (usually the
same backend instance implementing all three, but they're independent
interfaces — a backend that only supports append+read isn't forced to
implement pruning) and adds two things on top: asynchronous writing and
handler fan-out.

## Backends

Three implementations of the storage interfaces, under `backends/`:

- **`memory`** — in-process, `sync.RWMutex`-guarded slice. Used in tests and the runnable example.
- **`file`** — append-only JSON-lines file. Read seeks to the start, scans, then restores the append position.
- **`sqlite`** — a real database backend, using `modernc.org/sqlite` (pure Go, no cgo) so the module builds without a C toolchain.

Swapping which backend `Manager` uses is a one-line change at the call site — nothing in `Manager` itself changes.

## Design choices worth knowing

- **`WriteLog` is asynchronous.** It builds a `LogEntry`, pushes it onto a
  buffered channel, and returns — a background goroutine (`run`) drains the
  channel into the writer and then fans the entry out to every registered
  handler. Callers never block on I/O.
- **Backpressure is a choice, not an accident.** `Manager` supports `Block`
  (default: back off the caller until there's room) or `DropNewest` (discard
  and count via `Dropped()`) via `WithDropPolicy`. A silently-unbounded queue
  would let a slow writer grow memory without limit; blocking is the safe
  default, `DropNewest` is there for callers who'd rather lose a log line
  than slow down the request path.
- **`Close` is race-free by construction, not by convention.** Closing a Go
  channel while other goroutines might still be sending on it is a classic
  panic waiting to happen. `Manager` guards this with an `RWMutex`:
  `WriteLog` holds `RLock` for its entire check-then-send, and `Close` takes
  `Lock` — which can't succeed until every in-flight `WriteLog` has released
  its `RLock` — before closing the channel. This means `Close` is provably
  safe to call concurrently with in-flight writes, not just "safe in
  practice." See `manager_test.go`'s concurrent test, run under `-race`.
- **Entries are immutable values, not pooled.** An earlier design pooled
  `LogEntry` structs via `sync.Pool` to cut allocations, then reset and
  reused them after each write. That's unsafe here: entries fan out to an
  arbitrary number of `LogHandler`s, and nothing stops a handler from
  retaining the pointer past its `Handle` call — reusing the underlying
  struct after that would corrupt whatever the handler kept. Instead, the
  `[進階] Low GC Pressure` requirement is addressed where the actual
  allocation hot spot is: the `file` backend pools `*bytes.Buffer` for JSON
  encoding, which has no such aliasing risk (the buffer's contents are fully
  copied into the file before the buffer is returned to the pool).
- **Structured JSON output** comes for free from the same encoding path —
  `file` and `sqlite` both serialize `Fields()` as a JSON object.
- **Level filtering is "at least", not "exactly."** `ReadLogs("WARN", ...)`
  returns WARN and ERROR entries, not just WARN — this is what makes
  level-based filtering actually useful for log review.

## Usage

```go
store := memory.New() // or file.Open("app.log"), or sqlite.Open("app.db")
manager := logGO.NewManager(store, store, store)

manager.RegisterLogHandler(logGO.LogHandlerFunc(func(e logGO.LogEntry) {
    fmt.Printf("[%s] %s\n", e.Level(), e.Message())
}))

manager.WriteLog("INFO", "server started", logGO.F("port", 8080))
manager.WriteLog("ERROR", "failed to connect to database")

manager.Close() // flush pending async writes

entries, _ := manager.ReadLogs("ERROR", logGO.LogFilter{})
```

See `example_test.go` for a runnable, testable version of this (`go doc -all . `
shows it as `Example`).

## Standalone server

`cmd/server` runs logGO as its own independent service, with its own
Dozzle-style UI — the only relationship to
[conTogether](https://github.com/ttfancy/conTogether) (or any other instance
you point it at) is that it connects as a plain HTTP/WebSocket *client* of
its already-existing log-reading endpoints (`GET /logs`, `GET /ws/logs`).
Neither project imports the other's code.

```bash
go run ./cmd/server
```

Open http://localhost:9090 (override with `PORT`) and click **+ Add
service** — no env vars required to get started; sources are managed at
runtime, not fixed at boot.

### Building

```bash
# The server binary
go build -o loggo-server ./cmd/server

# The three demo push clients (see "Push ingestion" below)
go build -o demo-rest ./cmd/demo-rest-client
go build -o demo-ws ./cmd/demo-ws-client
go build -o demo-grpc ./cmd/demo-grpc-client

# Or install any of them onto $GOPATH/bin
go install ./cmd/server

# Docker: build the server image directly...
docker build -t loggo .
docker run -p 9090:9090 loggo

# ...or build/run everything (server + all three demo clients) at once
docker compose up --build
```

`Dockerfile` builds the server; `cmd/Dockerfile.demo-client` is a shared,
parameterized build for the three demo clients (see `docker-compose.yml`
for how each is built with a different `CLIENT` build arg).

### Multi-source registry

Any number of sources can be registered at once (`internal/sources`), each
independent, removed via the UI or `DELETE /sources/{id}` (stops future
ingestion; already-collected history stays), and persisted to a small JSON
file (`SOURCES_FILE`, default `sources.json`) so they survive a restart. The
sidebar lists every registered source — click one to filter the log view to
just it (over a WebSocket, genuinely real-time, not polling), or stay on
"All sources" to see everything interleaved.

A source is one of two kinds, chosen in the UI's "+ Add service" form (or via
`kind` in `POST /sources`):

- **Pull** (`kind: "pull"`, the original design) — logGO connects *out* to
  `{"name":"...","base_url":"...","api_key":"..."}`, the same as before push
  sources existed; a saved `sources.json` from before this field existed has
  no `kind` at all and is still treated as pull.
- **Push** (`kind: "push"`, `{"name":"...","protocol":"rest"|"websocket"|"grpc"}`)
  — the inverse: logGO doesn't connect anywhere, a client pushes to it. Adding
  one just reserves a name and an ID; the UI then shows a copy-pasteable
  snippet (curl/wscat/grpcurl) for the chosen protocol, using that ID as the
  entry's `source` so it shows up under the registered name instead of a raw
  string. See "Push ingestion" below for the actual wire formats.

For each **pull** source (see
[`docs/diagrams/05-ingestion-sequence.puml`](docs/diagrams/05-ingestion-sequence.puml)
for the full sequence):

1. **Backfill** — `GET /logs` once at registration, pulling everything that
   instance already has.
2. **Live tail** — dials `GET /ws/logs` and ingests every new entry as it's
   written, for as long as the connection holds.
3. **Reconnect** — if the WebSocket drops, retries with exponential backoff
   (capped at 30s) and re-backfills from the last entry actually ingested
   first, so a brief disconnect doesn't lose anything in the gap.

Every ingested entry is stored via `Manager.WriteEntry` (not `WriteLog`) and
tagged with the source's ID (not its display name — two sources could share
a name, IDs are unique) via a `source` field — `WriteEntry` enqueues an
already-built `LogEntry` as-is, preserving its *original* timestamp, where
`WriteLog` would stamp it with `time.Now()`. For an aggregator, using
ingestion time instead of the original event time would misrepresent when
things actually happened — see `manager_test.go`'s
`TestWriteEntryPreservesOriginalTimestamp`.

### Push ingestion

Registering a push source above (or skipping registration entirely — see
below) is how a client tells logGO it'll be *pushing* entries, over
whichever of three protocols suits it. See `cmd/demo-rest-client`,
`cmd/demo-ws-client`, and `cmd/demo-grpc-client` for one minimal, complete
example of each, and `docker-compose.yml` for all three running against a
containerized logGO at once.

| Protocol | Endpoint | Shape |
|---|---|---|
| REST | `POST /ingest` | One JSON body per request: `{"source","level","message","fields","timestamp"}` (`timestamp` optional, RFC3339, defaults to now) |
| WebSocket | `GET /ws/ingest` | Same JSON shape as REST, one text message per entry, over a connection the client keeps open |
| gRPC | `IngestService/Ingest` (`proto/ingest/v1/ingest.proto`) | Real gRPC wire format (not just Connect's own JSON-ish protocol) over cleartext HTTP/2 (h2c) — no TLS needed for local/demo use |

All three write through the same `ingest()` helper (`cmd/server/ingest_handlers.go`)
into the one shared `Manager`, tagged with the pushed `source` field exactly
like a pulled entry — the UI's sidebar and filtering don't distinguish
between "logGO pulled this" and "a client pushed this."

Registering a push source first is a convenience, not a requirement —
`source` in a pushed entry is never validated against the registry. Push
without registering and it shows up under its raw `source` string; register
first and it shows up under the friendly name (and protocol badge) instead,
and you get the connection snippet.

One concrete example per protocol, against a server running on
`localhost:9090`:

```bash
# REST — one request per entry
curl -X POST http://localhost:9090/ingest \
  -H 'Content-Type: application/json' \
  -d '{"source":"my-service","level":"INFO","message":"hello"}'

# WebSocket — one connection, one JSON message per entry (wscat)
wscat -c ws://localhost:9090/ws/ingest
> {"source":"my-service","level":"INFO","message":"hello"}

# gRPC — real gRPC wire format over h2c (grpcurl, plaintext)
grpcurl -plaintext -d '{"source":"my-service","entry":{"level":"INFO","message":"hello"}}' \
  localhost:9090 ingest.v1.IngestService/Ingest
```

Or run the demo clients, which do this on a timer:

```bash
# REST
docker compose up loggo demo-rest-client

# all three protocols at once
docker compose up
```

### API

| Method | Path | Notes |
|---|---|---|
| GET | `/entries?level=&contains=&source_id=` | Historical query, capped at the 500 most recent matches |
| GET | `/ws/entries?level=&contains=&source_id=` | Backlog, then real-time push — what the UI actually uses |
| GET | `/sources` | List registered sources (API keys never included) |
| POST | `/sources` | Add one — `{"name","base_url","api_key"}` |
| DELETE | `/sources/{id}` | Stop ingesting from it (history stays) |
| POST | `/ingest` | Push one entry via REST — see "Push ingestion" |
| GET | `/ws/ingest` | Push entries via a persistent WebSocket — see "Push ingestion" |
| — | `IngestService/Ingest` (gRPC) | Push one entry via gRPC — see "Push ingestion" |

The UI itself (`internal/webui`) is a single static HTML page — no frontend
build step, deliberately, to match a "small" project.

### Configuration

| Env var | Default | Meaning |
|---|---|---|
| `PORT` | `9090` | logGO's own HTTP listen port |
| `LOG_FILE_PATH` | `logGO.log` | Where logGO persists its own (ingested + self-written) entries |
| `SOURCES_FILE` | `sources.json` | Where registered sources are persisted |
| `CONTOGETHER_URL` / `CONTOGETHER_API_KEY` | — | Optional: auto-registers as an ordinary source at boot (the old single-source way of pointing logGO somewhere) — everything past that first registration works exactly the same as a source added through the UI |
| `SOURCE_NAME` | `conTogether` | Display name for the source above, if set |

## Tests

`go test ./... -race` from the repo root covers:

- Level filtering end to end through `Manager`
- `WriteLog` after `Close` returning `ErrClosed`
- `WriteEntry` preserving an ingested entry's original timestamp instead of
  re-stamping it
- A genuine concurrency test: many goroutines calling `WriteLog` and
  `RegisterLogHandler` at once, run under `-race`
- `ClearLogs` boundary semantics (an entry timestamped exactly at the
  cutoff is kept, not cleared)
- The `DropNewest` policy under a deliberately stalled writer
- Round-trip write/read/clear against both the `file` and `sqlite` backends
- `internal/ingest`, against a fake conTogether (`httptest`): backfill then
  live tail, tagging, and that a disconnect/auth failure retries with
  backoff instead of giving up, while still respecting context cancellation
  promptly

## Known limitations

- The default queue size (1024) and `Block` policy are reasonable general
  defaults, not tuned for any particular throughput target — override via
  `WithQueueSize`/`WithDropPolicy` if needed.
- `sqlite.Store` caps the connection pool at 1 (`SetMaxOpenConns(1)`) to
  avoid "database is locked" errors, since SQLite serializes writers at the
  file level anyway.
