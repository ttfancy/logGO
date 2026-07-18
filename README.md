# logsys

A small, dependency-injected logging system: asynchronous writes, pluggable
storage, level filtering, and an extension point for things like remote log
aggregation — built around four interfaces rather than one concrete logger
type.

See [`../docs/diagrams/01-logsys-structure.puml`](../docs/diagrams/01-logsys-structure.puml),
[`02-logsys-write-sequence.puml`](../docs/diagrams/02-logsys-write-sequence.puml) and
[`03-logsys-read-sequence.puml`](../docs/diagrams/03-logsys-read-sequence.puml) for diagrams of
the structure and both call flows below.

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
manager := logsys.NewManager(store, store, store)

manager.RegisterLogHandler(logsys.LogHandlerFunc(func(e logsys.LogEntry) {
    fmt.Printf("[%s] %s\n", e.Level(), e.Message())
}))

manager.WriteLog("INFO", "server started", logsys.F("port", 8080))
manager.WriteLog("ERROR", "failed to connect to database")

manager.Close() // flush pending async writes

entries, _ := manager.ReadLogs("ERROR", logsys.LogFilter{})
```

See `example_test.go` for a runnable, testable version of this (`go doc -all . `
shows it as `Example`).

## Tests

`go test ./... -race` from the repo root covers:

- Level filtering end to end through `Manager`
- `WriteLog` after `Close` returning `ErrClosed`
- A genuine concurrency test: many goroutines calling `WriteLog` and
  `RegisterLogHandler` at once, run under `-race`
- `ClearLogs` boundary semantics (an entry timestamped exactly at the
  cutoff is kept, not cleared)
- The `DropNewest` policy under a deliberately stalled writer
- Round-trip write/read/clear against both the `file` and `sqlite` backends

## Known limitations

- The default queue size (1024) and `Block` policy are reasonable general
  defaults, not tuned for any particular throughput target — override via
  `WithQueueSize`/`WithDropPolicy` if needed.
- `sqlite.Store` caps the connection pool at 1 (`SetMaxOpenConns(1)`) to
  avoid "database is locked" errors, since SQLite serializes writers at the
  file level anyway.
