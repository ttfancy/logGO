// Package webui serves logGO's own UI — a single static page, no
// frontend build step, deliberately: logGO is a small project, and a
// full JS toolchain to poll one JSON endpoint and render a table would
// be abstraction the project doesn't need.
package webui

import (
	"embed"
	"net/http"
)

//go:embed index.html
var files embed.FS

// Handler serves the UI page at "/".
func Handler() (http.Handler, error) {
	return http.FileServerFS(files), nil
}
