package api

import (
	"log/slog"
	"net/http"
	"time"
)

// middleware wraps an http.Handler with additional behavior.
type middleware func(http.Handler) http.Handler

// logRequests emits one structured log line per request, after the handler
// returns, recording the outcome and how long it took.
//
// Structured fields rather than a formatted string matter more than usual
// here: from Milestone 2 onward these lines are read alongside Raft events
// from several nodes at once, and correlating them by hand is impractical.
func logRequests(logger *slog.Logger) middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r)

			logger.InfoContext(r.Context(), "request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Duration("duration", time.Since(start)),
				slog.String("remote_addr", r.RemoteAddr),
			)
		})
	}
}

// statusRecorder captures the status code written by a handler, which the
// ResponseWriter interface otherwise provides no way to read back.
type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// WriteHeader records the status before delegating. Only the first call
// counts, matching net/http, which ignores subsequent calls.
func (r *statusRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

// Write marks the header as written, since a handler that calls Write without
// WriteHeader implicitly sends 200, the value status is initialized to.
func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

// Flush forwards to the underlying ResponseWriter when it supports flushing.
// Wrapping a ResponseWriter otherwise hides optional interfaces from handlers;
// streaming endpoints added later would silently buffer without this.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
