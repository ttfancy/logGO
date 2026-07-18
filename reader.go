package logsys

// LogReader defines log read behavior: return entries at or above
// minLevel that also satisfy filter.
type LogReader interface {
	Read(minLevel Level, filter LogFilter) ([]LogEntry, error)
}
