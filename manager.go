package logsys

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

const defaultQueueSize = 1024

// ErrClosed is returned by WriteLog once the Manager has been closed.
var ErrClosed = errors.New("logsys: manager is closed")

// DropPolicy controls what WriteLog does when the async write queue is
// full.
type DropPolicy int

const (
	// Block waits for room in the queue. Never loses an entry, but a
	// stalled writer applies backpressure to every caller.
	Block DropPolicy = iota
	// DropNewest discards the incoming entry instead of blocking; see
	// Manager.Dropped for a running count.
	DropNewest
)

// Option configures a Manager at construction time.
type Option func(*Manager)

// WithQueueSize overrides the default async write queue buffer size.
func WithQueueSize(n int) Option {
	return func(m *Manager) { m.queue = make(chan LogEntry, n) }
}

// WithDropPolicy overrides the default (Block) policy.
func WithDropPolicy(p DropPolicy) Option {
	return func(m *Manager) { m.dropPolicy = p }
}

// Manager is the LogManager core class: it composes a LogWriter,
// LogReader and LogClearer (any combination of interfaces satisfying
// each may be distinct instances or, commonly, one backend implementing
// all three) and adds asynchronous writing plus a handler extension
// point on top.
type Manager struct {
	writer  LogWriter
	reader  LogReader
	clearer LogClearer

	queue      chan LogEntry
	dropPolicy DropPolicy
	dropped    atomic.Int64

	// stateMu guards `closed` and, critically, makes closing `queue`
	// race-free despite WriteLog being called from many goroutines:
	// WriteLog holds RLock for the whole check-then-send, Close takes
	// Lock (which waits out every in-flight RLock holder) before it
	// closes the channel. See WriteLog/Close for the paired halves.
	stateMu sync.RWMutex
	closed  bool

	handlersMu  sync.RWMutex
	handlers    []registeredHandler
	nextHandler int64

	wg sync.WaitGroup
}

type registeredHandler struct {
	id      int64
	handler LogHandler
}

// NewManager wires a writer, reader and clearer together and starts the
// background write loop. All three parameters are plain interfaces, so
// callers inject whichever backend(s) they like (see logsys/backends) —
// this is the dependency-injection seam the container-api's logging
// middleware plugs into.
func NewManager(writer LogWriter, reader LogReader, clearer LogClearer, opts ...Option) *Manager {
	m := &Manager{
		writer:  writer,
		reader:  reader,
		clearer: clearer,
		queue:   make(chan LogEntry, defaultQueueSize),
	}
	for _, opt := range opts {
		opt(m)
	}
	m.wg.Add(1)
	go m.run()
	return m
}

// WriteLog builds a LogEntry and enqueues it for asynchronous writing;
// it returns before the entry reaches the writer or any handler.
func (m *Manager) WriteLog(level string, message string, fields ...Field) error {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	if m.closed {
		return ErrClosed
	}

	var fieldMap map[string]any
	if len(fields) > 0 {
		fieldMap = make(map[string]any, len(fields))
		for _, f := range fields {
			fieldMap[f.Key] = f.Value
		}
	}
	e := &entry{ts: time.Now(), level: parseLevel(level), message: message, fields: fieldMap}

	if m.dropPolicy == DropNewest {
		select {
		case m.queue <- e:
		default:
			m.dropped.Add(1)
		}
		return nil
	}
	m.queue <- e
	return nil
}

func (m *Manager) run() {
	defer m.wg.Done()
	for e := range m.queue {
		if err := m.writer.Write(e); err != nil {
			// A broken sink must not take the process down; the write
			// is simply lost. Callers who need write-error visibility
			// can register a LogHandler and check for it out-of-band,
			// or wrap LogWriter with retry/alerting logic of their own.
			continue
		}
		m.handlersMu.RLock()
		for _, rh := range m.handlers {
			rh.handler.Handle(e)
		}
		m.handlersMu.RUnlock()
	}
}

// ReadLogs returns stored entries at or above level that also satisfy
// filter.
func (m *Manager) ReadLogs(level string, filter LogFilter) ([]LogEntry, error) {
	return m.reader.Read(parseLevel(level), filter)
}

// ClearLogs removes stored entries timestamped before the given time.
func (m *Manager) ClearLogs(before time.Time) error {
	return m.clearer.Clear(before)
}

// RegisterLogHandler adds a handler invoked for every entry as it is
// written, e.g. to bridge into remote aggregation, alerting, or a live
// WebSocket tail (see container-api's internal/wsstream). It returns an
// unregister function that removes the handler; callers that never need
// to stop receiving entries (the common case) can simply ignore it —
// existing code that predates unregister support still compiles
// unchanged, since Go permits discarding a return value. Callers that
// DO need it — anything tied to a connection's lifetime, like a
// WebSocket client — must call it on disconnect, or the handler (and
// whatever it's holding, e.g. a dead connection) leaks for the life of
// the Manager.
func (m *Manager) RegisterLogHandler(handler LogHandler) (unregister func()) {
	m.handlersMu.Lock()
	id := m.nextHandler
	m.nextHandler++
	m.handlers = append(m.handlers, registeredHandler{id: id, handler: handler})
	m.handlersMu.Unlock()

	return func() {
		m.handlersMu.Lock()
		defer m.handlersMu.Unlock()
		for i, rh := range m.handlers {
			if rh.id == id {
				m.handlers = append(m.handlers[:i], m.handlers[i+1:]...)
				return
			}
		}
	}
}

// Dropped returns how many entries have been discarded under the
// DropNewest policy.
func (m *Manager) Dropped() int64 { return m.dropped.Load() }

// Close stops accepting new entries, waits for the queue to drain, and
// closes the underlying writer. It is safe to call exactly once, e.g.
// as the last step of graceful shutdown (see container-api's shutdown
// sequence: stop HTTP, drain jobs, then Close the logger).
func (m *Manager) Close() error {
	m.stateMu.Lock()
	m.closed = true
	close(m.queue) // safe: Lock() waited out every WriteLog holding RLock
	m.stateMu.Unlock()

	m.wg.Wait()
	return m.writer.Close()
}
