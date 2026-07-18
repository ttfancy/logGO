// Package logsys is a small, dependency-injected logging system built
// around four interfaces: LogWriter, LogReader, LogClearer and
// LogHandler. Manager wires a writer/reader/clearer together, writes
// asynchronously, and fans entries out to registered handlers.
package logsys

import (
	"strings"
	"time"
)

// Level identifies the severity of a LogEntry. Levels are ordered
// DebugLevel < InfoLevel < WarnLevel < ErrorLevel; ReadLogs treats the
// requested level as a minimum, not an exact match.
type Level string

const (
	DebugLevel Level = "DEBUG"
	InfoLevel  Level = "INFO"
	WarnLevel  Level = "WARN"
	ErrorLevel Level = "ERROR"
)

var levelRank = map[Level]int{
	DebugLevel: 0,
	InfoLevel:  1,
	WarnLevel:  2,
	ErrorLevel: 3,
}

// parseLevel normalizes a caller-supplied level string, defaulting to
// InfoLevel for anything unrecognized rather than rejecting the write.
func parseLevel(s string) Level {
	l := Level(strings.ToUpper(strings.TrimSpace(s)))
	if _, ok := levelRank[l]; !ok {
		return InfoLevel
	}
	return l
}

// LevelAtLeast reports whether l is at least as severe as min.
func LevelAtLeast(l, min Level) bool {
	return levelRank[l] >= levelRank[min]
}

// Field is a structured key/value pair attached to a log entry, used for
// the JSON structured-logging output.
type Field struct {
	Key   string
	Value any
}

// F builds a Field; a small ergonomic helper for WriteLog call sites,
// e.g. mgr.WriteLog("INFO", "listening", logsys.F("port", 8080)).
func F(key string, value any) Field {
	return Field{Key: key, Value: value}
}

// LogEntry defines the data structure of a single log record. It is
// deliberately an immutable value: WriteLog builds one that is safe for
// concurrent readers (the async writer goroutine and every registered
// handler) to observe without additional locking.
type LogEntry interface {
	Timestamp() time.Time
	Level() Level
	Message() string
	Fields() map[string]any
}

type entry struct {
	ts      time.Time
	level   Level
	message string
	fields  map[string]any
}

func (e *entry) Timestamp() time.Time   { return e.ts }
func (e *entry) Level() Level           { return e.level }
func (e *entry) Message() string        { return e.message }
func (e *entry) Fields() map[string]any { return e.fields }

// NewEntry constructs a LogEntry from stored fields. Backend
// implementations (file, sqlite, ...) use this to reconstruct entries
// read back from persistent storage.
func NewEntry(ts time.Time, level Level, message string, fields map[string]any) LogEntry {
	return &entry{ts: ts, level: level, message: message, fields: fields}
}
