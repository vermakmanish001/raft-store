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
	"strconv"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/replica"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// Cluster reports consensus state. It is optional: a single-node deployment
// has none, and the API behaves identically apart from redirects and the
// status endpoint.
type Cluster interface {
	Status() replica.Status
}

// SessionStore is a store that can deduplicate client retries.
//
// It is an optional capability rather than part of store.Store, because a
// plain in-memory store has no way to honor it and should not be made to
// pretend otherwise.
type SessionStore interface {
	PutRequest(key, value string, req replica.Request) error
	DeleteRequest(key string, req replica.Request) error
}

// Headers a client uses to identify a request for deduplication.
const (
	// HeaderClientID is a stable identifier for the client, reused across
	// requests and across reconnections.
	HeaderClientID = "X-Client-ID"

	// HeaderSeq is a number the client increments once per distinct
	// operation, and holds constant across retries of that operation.
	HeaderSeq = "X-Request-Seq"
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
	cluster      Cluster
	logger       *slog.Logger
	maxBodyBytes int64
}

// Option customizes a Server.
type Option func(*Server)

// WithMaxBodyBytes overrides the maximum accepted value size.
func WithMaxBodyBytes(n int64) Option {
	return func(s *Server) { s.maxBodyBytes = n }
}

// WithCluster attaches consensus state, enabling leader redirection and the
// status endpoint.
func WithCluster(c Cluster) Option {
	return func(s *Server) { s.cluster = c }
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
	mux.HandleFunc("GET /status", s.handleStatus)

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

	if err := s.put(key, string(body), r); err != nil {
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
	if err := s.delete(r.PathValue("key"), r); err != nil {
		s.writeError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// put writes through the deduplicating path when the store supports it and
// the client identified its request.
func (s *Server) put(key, value string, r *http.Request) error {
	sessions, ok := s.store.(SessionStore)
	req, identified := requestFrom(r)
	if !ok || !identified {
		return s.store.Put(key, value)
	}
	return sessions.PutRequest(key, value, req)
}

func (s *Server) delete(key string, r *http.Request) error {
	sessions, ok := s.store.(SessionStore)
	req, identified := requestFrom(r)
	if !ok || !identified {
		return s.store.Delete(key)
	}
	return sessions.DeleteRequest(key, req)
}

// requestFrom reads the deduplication headers, reporting whether the client
// supplied a usable pair.
//
// Both headers are required together. A client ID without a sequence number
// cannot distinguish one operation from the next, and a sequence number
// without an ID does not say whose it is, so a partial pair is treated as
// absent rather than guessed at.
func requestFrom(r *http.Request) (replica.Request, bool) {
	id := r.Header.Get(HeaderClientID)
	raw := r.Header.Get(HeaderSeq)
	if id == "" || raw == "" {
		return replica.Request{}, false
	}

	seq, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || seq == 0 {
		// Sequence numbers start at 1, so zero is indistinguishable from
		// absent and is rejected rather than silently disabling dedup.
		return replica.Request{}, false
	}
	return replica.Request{ClientID: id, Seq: seq}, true
}

// handleHealth reports process liveness. It deliberately says nothing about
// cluster health; once Raft exists, a node's role and whether it can reach a
// quorum belong on a separate status endpoint, so that a load balancer's
// liveness probe is never conflated with readiness to serve reads.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, r, http.StatusOK, healthResponse{Status: "ok"})
}

// handleStatus reports consensus state: this node's role, term, and who it
// believes leads. Unlike /health it says nothing about liveness, so a load
// balancer should not probe it.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		s.writeJSON(w, r, http.StatusOK, map[string]string{"mode": "single-node"})
		return
	}
	s.writeJSON(w, r, http.StatusOK, s.cluster.Status())
}

// writeError maps a store error to a status code and JSON body.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	// A write that reached the wrong node is redirected rather than refused,
	// so a client that guessed wrong is not required to understand the
	// cluster's topology.
	if errors.Is(err, raft.ErrNotLeader) && s.redirectToLeader(w, r) {
		return
	}

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

	case errors.Is(err, raft.ErrNotLeader):
		// Reached only when the leader is unknown, since a known leader is
		// redirected to instead. The cluster is mid-election and the client
		// should retry shortly.
		return http.StatusServiceUnavailable, "no leader elected, retry shortly"

	case errors.Is(err, replica.ErrReadTimeout):
		// The read changed nothing, so retrying is always safe. Saying so
		// matters: a client told "outcome unknown" may hold back a retry it
		// could have made immediately.
		return http.StatusServiceUnavailable,
			"could not reach a quorum to confirm this node still leads, retry shortly"

	case errors.Is(err, replica.ErrTimeout):
		// Deliberately not reported as a failure. The entry may still commit,
		// so the outcome is genuinely unknown and the client must retry and
		// tolerate the write having already happened.
		return http.StatusServiceUnavailable, "timed out waiting for replication, outcome unknown"

	case errors.Is(err, replica.ErrShuttingDown):
		return http.StatusServiceUnavailable, "node is shutting down"

	default:
		return http.StatusInternalServerError, "internal error"
	}
}

// redirectToLeader sends the client to the current leader, reporting whether
// it was able to.
//
// The redirect is 307 rather than 302 because 307 requires the client to
// repeat the method and body unchanged. A PUT redirected as 302 may legally
// be retried as a GET, which would silently discard the write.
func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) bool {
	if s.cluster == nil {
		return false
	}

	addr := s.cluster.Status().LeaderAddr
	if addr == "" {
		return false // mid-election: nobody to redirect to
	}

	http.Redirect(w, r, addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	return true
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
