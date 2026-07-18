package logsys

import "time"

// LogClearer defines how old entries are purged from storage. Split out
// from LogReader/LogWriter so a backend that only supports append+read
// (e.g. a remote log-aggregation sink) isn't forced to implement pruning.
type LogClearer interface {
	// Clear removes entries strictly before the given time; an entry
	// timestamped exactly at before is kept.
	Clear(before time.Time) error
}
