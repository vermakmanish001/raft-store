// Package ui serves the bundled cluster dashboard.
//
// The page is embedded in the binary rather than read from disk, so a node has
// no runtime dependency on its source tree and can be shipped as a single
// file. It is plain HTML, CSS, and JavaScript with no build step and no
// external requests, which also means it works on a machine with no network.
package ui

import (
	"bytes"
	"embed"
	"net/http"
	"time"
)

//go:embed index.html
var assets embed.FS

// Handler serves the dashboard.
func Handler() http.Handler {
	page, err := assets.ReadFile("index.html")
	if err != nil {
		// Unreachable: the file is embedded at compile time, so a failure here
		// means the binary is malformed rather than misconfigured.
		panic("ui: embedded page missing: " + err.Error())
	}

	// A fixed modification time keeps caching deterministic across restarts,
	// so a reloaded page is not served stale from a previous build.
	modTime := time.Time{}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, "index.html", modTime, bytes.NewReader(page))
	})
}
