package logsys

// LogWriter defines log write behavior. Implementations persist a single
// entry at a time; Manager is what makes writes asynchronous from the
// caller's perspective, so LogWriter itself can be a plain, synchronous
// interface.
type LogWriter interface {
	Write(e LogEntry) error
	Close() error
}
