// Package file is a JSON-lines file LogWriter/LogReader/LogClearer —
// one backend behind the logsys interfaces, demonstrating pluggable
// storage alongside backends/memory and backends/sqlite.
package file

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"sync"
	"time"

	"contogether/logsys"
)

// record is the on-disk JSON representation of a logsys.LogEntry.
type record struct {
	Timestamp time.Time      `json:"timestamp"`
	Level     logsys.Level   `json:"level"`
	Message   string         `json:"message"`
	Fields    map[string]any `json:"fields,omitempty"`
}

// Store is a LogWriter/LogReader/LogClearer backed by an append-only
// JSON-lines file.
type Store struct {
	path string
	mu   sync.Mutex
	f    *os.File
	bufs sync.Pool // reused *bytes.Buffer for JSON encoding (low-GC write path)
}

// Open creates path if needed and returns a Store backed by it.
func Open(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &Store{
		path: path,
		f:    f,
		bufs: sync.Pool{New: func() any { return new(bytes.Buffer) }},
	}, nil
}

func (s *Store) Write(e logsys.LogEntry) error {
	buf := s.bufs.Get().(*bytes.Buffer)
	buf.Reset()
	defer s.bufs.Put(buf)

	if err := json.NewEncoder(buf).Encode(record{
		Timestamp: e.Timestamp(),
		Level:     e.Level(),
		Message:   e.Message(),
		Fields:    e.Fields(),
	}); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.f.Write(buf.Bytes())
	return err
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

func (s *Store) Read(minLevel logsys.Level, filter logsys.LogFilter) ([]logsys.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.f.Seek(0, 0); err != nil {
		return nil, err
	}
	defer s.f.Seek(0, 2) // restore append position regardless of outcome

	var out []logsys.LogEntry
	scanner := bufio.NewScanner(s.f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var r record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			continue // skip a malformed/partial line rather than fail the whole read
		}
		e := logsys.NewEntry(r.Timestamp, r.Level, r.Message, r.Fields)
		if !logsys.LevelAtLeast(e.Level(), minLevel) {
			continue
		}
		if !filter.Matches(e) {
			continue
		}
		out = append(out, e)
	}
	return out, scanner.Err()
}

func (s *Store) Clear(before time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.f.Seek(0, 0); err != nil {
		return err
	}
	scanner := bufio.NewScanner(s.f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	var kept [][]byte
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var r record
		if err := json.Unmarshal(line, &r); err == nil && r.Timestamp.Before(before) {
			continue
		}
		kept = append(kept, line)
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	tmp, err := os.CreateTemp("", "logsys-*.jsonl")
	if err != nil {
		return err
	}
	for _, line := range kept {
		if _, err := tmp.Write(append(line, '\n')); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := s.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	s.f = f
	return nil
}
