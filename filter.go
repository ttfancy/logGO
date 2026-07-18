package logsys

import (
	"strings"
	"time"
)

// LogFilter narrows ReadLogs results beyond the minimum level. The zero
// value matches everything.
type LogFilter struct {
	Since    time.Time // zero value = unbounded
	Until    time.Time // zero value = unbounded
	Contains string    // substring match against the message; empty = no filter
}

// Matches reports whether e satisfies the filter. Backend implementations
// call this after their own level check so filtering logic lives in one
// place rather than being reimplemented per backend.
func (f LogFilter) Matches(e LogEntry) bool {
	if !f.Since.IsZero() && e.Timestamp().Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && e.Timestamp().After(f.Until) {
		return false
	}
	if f.Contains != "" && !strings.Contains(e.Message(), f.Contains) {
		return false
	}
	return true
}
