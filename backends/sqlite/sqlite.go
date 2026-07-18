// Package sqlite is a SQLite-backed LogWriter/LogReader/LogClearer,
// demonstrating a database-backed pluggable storage option alongside
// backends/file and backends/memory. Uses modernc.org/sqlite (a pure-Go
// driver) so the module builds without cgo.
package sqlite

import (
	"database/sql"
	"encoding/json"
	"time"

	_ "modernc.org/sqlite"

	"contogether/logsys"
)

// Store is a LogWriter/LogReader/LogClearer backed by a SQLite table.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) a SQLite database at path and ensures
// the log_entries table exists.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite serializes writers at the file level; keeping the pool to a
	// single connection avoids "database is locked" errors under our
	// single-writer-goroutine access pattern.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS log_entries (
			id      INTEGER PRIMARY KEY AUTOINCREMENT,
			ts      INTEGER NOT NULL,
			level   TEXT NOT NULL,
			message TEXT NOT NULL,
			fields  TEXT NOT NULL
		)`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Write(e logsys.LogEntry) error {
	fieldsJSON, err := json.Marshal(e.Fields())
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO log_entries (ts, level, message, fields) VALUES (?, ?, ?, ?)`,
		e.Timestamp().UnixNano(), string(e.Level()), e.Message(), string(fieldsJSON),
	)
	return err
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Read(minLevel logsys.Level, filter logsys.LogFilter) ([]logsys.LogEntry, error) {
	rows, err := s.db.Query(`SELECT ts, level, message, fields FROM log_entries ORDER BY ts ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []logsys.LogEntry
	for rows.Next() {
		var ts int64
		var level, message, fieldsJSON string
		if err := rows.Scan(&ts, &level, &message, &fieldsJSON); err != nil {
			return nil, err
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			fields = nil
		}
		e := logsys.NewEntry(time.Unix(0, ts), logsys.Level(level), message, fields)
		if !logsys.LevelAtLeast(e.Level(), minLevel) {
			continue
		}
		if !filter.Matches(e) {
			continue
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) Clear(before time.Time) error {
	_, err := s.db.Exec(`DELETE FROM log_entries WHERE ts < ?`, before.UnixNano())
	return err
}
