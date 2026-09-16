// Package api exposes the key-value store over HTTP.
//
// This layer owns transport concerns only: routing, decoding, status codes,
// and response encoding. It depends on the store.Store interface rather than
// a concrete store, so Milestone 4 can swap in the Raft-backed implementation
// without touching anything here. New failure modes that replication
// introduces, such as "this node is not the leader", will be added as further
// sentinel errors mapped to status codes in errorStatus.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/vermakmanish001/raft-store/internal/store"
)

// DefaultMaxBodyBytes caps the size of a value accepted by PUT. Without a cap,
// a single request could exhaust node memory; once entries flow through the
// Raft log, an oversized value would also be replicated to every peer.
const DefaultMaxBodyBytes int64 = 1 << 20 // 1 MiB

// Server routes HTTP requests to a Store.
//
// It is safe for concurrent use: Server holds no mutable state of its own, and
// the Store it wraps is required to be goroutine-safe.
type Server struct {
	store        store.Store
	logger       *slog.Logger
	maxBodyBytes int64
}

// Option customizes a Server.
type Option func(*Server)

// WithMaxBodyBytes overrides the maximum accepted value size.
func WithMaxBodyBytes(n int64) Option {
	return func(s *Server) { s.maxBodyBytes = n }
}

// NewServer returns a Server backed by st. A nil logger discards output.
func NewServer(st store.Store, logger *slog.Logger, opts ...Option) *Server {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	s := &Server{
		store:        st,
		logger:       logger,
		maxBodyBytes: DefaultMaxBodyBytes,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handler returns the fully configured HTTP handler, middleware included.
//
// Keys are a single path segment: a key containing a slash will not match and
// receives 404 from the mux. Method mismatches are answered with 405 by the
// pattern matcher, so no explicit method checks are needed in the handlers.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("GET /health", s.handleHealth)

	return logRequests(s.logger)(mux)
}

// handleGet serves GET /kv/{key}.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := s.store.Get(key)
	if err != nil {
		s.writeError(w, r, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, valueResponse{Key: key, Value: value})
}

// handlePut serves PUT /kv/{key}. The request body is the raw value, which
// keeps the API usable from curl without JSON quoting and keeps binary-ish
// payloads free of an encoding round trip.
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			s.writeJSON(w, r, http.StatusRequestEntityTooLarge,
				errorResponse{Error: "value exceeds maximum size"})
			return
		}
		s.writeJSON(w, r, http.StatusBadRequest,
			errorResponse{Error: "could not read request body"})
		return
	}

	if err := s.store.Put(key, string(body)); err != nil {
		s.writeError(w, r, err)
		return
	}

	// 204 rather than 201: PUT is idempotent and the store does not
	// distinguish a create from an overwrite, so claiming "Created" would be
	// a guess. There is no body to return.
	w.WriteHeader(http.StatusNoContent)
}

// handleDelete serves DELETE /kv/{key}.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Delete(r.PathValue("key")); err != nil {
		s.writeError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHealth reports process liveness. It deliberately says nothing about
// cluster health; once Raft exists, a node's role and whether it can reach a
// quorum belong on a separate status endpoint, so that a load balancer's
// liveness probe is never conflated with readiness to serve reads.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, healthResponse{Status: "ok"})
}

// writeError maps a store error to a status code and JSON body.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, message := errorStatus(err)

	// A 5xx means a bug or an unmapped error type, so log the underlying
	// error rather than leaking it to the client.
	if status >= http.StatusInternalServerError {
		s.logger.ErrorContext(r.Context(), "request failed",
			slog.String("path", r.URL.Path),
			slog.Any("error", err),
		)
	}

	s.writeJSON(w, r, status, errorResponse{Error: message})
}

// errorStatus translates a store error into an HTTP status and client-safe
// message. It is the single place where storage failures become transport
// failures, so later milestones extend this function rather than the handlers.
func errorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, store.ErrKeyNotFound):
		return http.StatusNotFound, "key not found"
	case errors.Is(err, store.ErrEmptyKey):
		return http.StatusBadRequest, "key must not be empty"
	default:
		return http.StatusInternalServerError, "internal error"
	}
}

// writeJSON encodes v as the response body.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)

	// The header and status are already committed, so an encoding failure
	// cannot be reported to the client. Log it and move on.
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.logger.ErrorContext(r.Context(), "encoding response failed",
			slog.String("path", r.URL.Path),
			slog.Any("error", err),
		)
	}
}

type valueResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type healthResponse struct {
	Status string `json:"status"`
}
