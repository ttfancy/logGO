package logsys_test

import (
	"fmt"

	"contogether/logsys"
	"contogether/logsys/backends/memory"
)

// Example demonstrates basic usage: wire a backend into a Manager,
// register a handler, write logs, then read them back filtered by level.
func Example() {
	store := memory.New()
	manager := logsys.NewManager(store, store, store)

	manager.RegisterLogHandler(logsys.LogHandlerFunc(func(e logsys.LogEntry) {
		fmt.Printf("[handler] %s: %s\n", e.Level(), e.Message())
	}))

	_ = manager.WriteLog("INFO", "server started", logsys.F("port", 8080))
	_ = manager.WriteLog("ERROR", "failed to connect to database")

	manager.Close() // flush pending async writes before reading them back

	entries, _ := manager.ReadLogs("ERROR", logsys.LogFilter{})
	for _, e := range entries {
		fmt.Printf("[stored] %s: %s\n", e.Level(), e.Message())
	}

	// Output:
	// [handler] INFO: server started
	// [handler] ERROR: failed to connect to database
	// [stored] ERROR: failed to connect to database
}
