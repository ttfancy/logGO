package logsys

// LogHandler receives every entry as it is written, as an extension point
// for features like remote log aggregation or alerting (see
// RegisterLogHandler). Handle is called synchronously from the manager's
// single write-loop goroutine: it must not block or retain e beyond the
// call.
type LogHandler interface {
	Handle(e LogEntry)
}

// LogHandlerFunc adapts a plain function to LogHandler, mirroring the
// standard library's http.HandlerFunc pattern.
type LogHandlerFunc func(e LogEntry)

func (f LogHandlerFunc) Handle(e LogEntry) { f(e) }
