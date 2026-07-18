// Package memory is an in-process LogWriter/LogReader/LogClearer, used
// for tests, examples, and any case where log persistence across
// process restarts doesn't matter.
package memory

import (
	"sync"
	"time"

	"contogether/logsys"
)

// Store is a thread-safe, in-memory log backend implementing
// logsys.LogWriter, logsys.LogReader and logsys.LogClearer.
type Store struct {
	mu      sync.RWMutex
	entries []logsys.LogEntry
}

// New returns an empty Store.
func New() *Store {
	return &Store{}
}

func (s *Store) Write(e logsys.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, e)
	return nil
}

func (s *Store) Close() error { return nil }

func (s *Store) Read(minLevel logsys.Level, filter logsys.LogFilter) ([]logsys.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []logsys.LogEntry
	for _, e := range s.entries {
		if !logsys.LevelAtLeast(e.Level(), minLevel) {
			continue
		}
		if !filter.Matches(e) {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *Store) Clear(before time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	kept := s.entries[:0]
	for _, e := range s.entries {
		if e.Timestamp().Before(before) {
			continue
		}
		kept = append(kept, e)
	}
	s.entries = kept
	return nil
}
