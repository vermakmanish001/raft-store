package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/api"
	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/replica"
	"github.com/vermakmanish001/raft-store/internal/store"
)

func TestGetKey(t *testing.T) {
	t.Parallel()

	st := store.New()
	if err := st.Put("alpha", "one"); err != nil {
		t.Fatalf("seeding store: %v", err)
	}
	h := api.NewServer(st, nil).Handler()

	t.Run("returns the stored value", func(t *testing.T) {
		t.Parallel()

		rec := do(t, h, http.MethodGet, "/kv/alpha", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		var got struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		decode(t, rec, &got)

		if got.Key != "alpha" || got.Value != "one" {
			t.Errorf("body = %+v, want key=alpha value=one", got)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
	})

	t.Run("missing key returns 404", func(t *testing.T) {
		t.Parallel()

		rec := do(t, h, http.MethodGet, "/kv/absent", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}

		var got struct {
			Error string `json:"error"`
		}
		decode(t, rec, &got)
		if got.Error == "" {
			t.Error("error body is empty, want a message")
		}
	})
}

func TestPutKey(t *testing.T) {
	t.Parallel()

	t.Run("stores a value and returns 204", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		h := api.NewServer(st, nil).Handler()

		rec := do(t, h, http.MethodPut, "/kv/alpha", "one")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}
		if body := rec.Body.String(); body != "" {
			t.Errorf("body = %q, want empty for 204", body)
		}

		got, err := st.Get("alpha")
		if err != nil || got != "one" {
			t.Errorf("store after PUT: value = %q, err = %v; want %q, nil", got, err, "one")
		}
	})

	t.Run("overwrites an existing value", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		h := api.NewServer(st, nil).Handler()

		do(t, h, http.MethodPut, "/kv/alpha", "one")
		do(t, h, http.MethodPut, "/kv/alpha", "two")

		if got, _ := st.Get("alpha"); got != "two" {
			t.Errorf("value after overwrite = %q, want %q", got, "two")
		}
	})

	t.Run("accepts an empty body as an empty value", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		h := api.NewServer(st, nil).Handler()

		rec := do(t, h, http.MethodPut, "/kv/alpha", "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}
		if got, err := st.Get("alpha"); err != nil || got != "" {
			t.Errorf("value = %q, err = %v; want empty string stored", got, err)
		}
	})

	t.Run("rejects an oversized value with 413", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		h := api.NewServer(st, nil, api.WithMaxBodyBytes(16)).Handler()

		rec := do(t, h, http.MethodPut, "/kv/alpha", strings.Repeat("x", 64))
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
		}
		if n := st.Len(); n != 0 {
			t.Errorf("Len() = %d, want 0; a rejected PUT must not store anything", n)
		}
	})
}

func TestDeleteKey(t *testing.T) {
	t.Parallel()

	t.Run("removes an existing key", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		if err := st.Put("alpha", "one"); err != nil {
			t.Fatalf("seeding store: %v", err)
		}
		h := api.NewServer(st, nil).Handler()

		rec := do(t, h, http.MethodDelete, "/kv/alpha", "")
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
		}
		if n := st.Len(); n != 0 {
			t.Errorf("Len() = %d, want 0", n)
		}
	})

	t.Run("missing key returns 404", func(t *testing.T) {
		t.Parallel()

		h := api.NewServer(store.New(), nil).Handler()

		rec := do(t, h, http.MethodDelete, "/kv/absent", "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", rec.Code, http.StatusNotFound)
		}
	})
}

func TestHealth(t *testing.T) {
	t.Parallel()

	h := api.NewServer(store.New(), nil).Handler()

	rec := do(t, h, http.MethodGet, "/health", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var got struct {
		Status string `json:"status"`
	}
	decode(t, rec, &got)
	if got.Status != "ok" {
		t.Errorf("status field = %q, want %q", got.Status, "ok")
	}
}

// TestRouting pins the behavior the mux gives us for free, so a later change
// to the route patterns cannot silently alter it.
func TestRouting(t *testing.T) {
	t.Parallel()

	st := store.New()
	if err := st.Put("alpha", "one"); err != nil {
		t.Fatalf("seeding store: %v", err)
	}
	h := api.NewServer(st, nil).Handler()

	tests := []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{"unsupported method on a key", http.MethodPost, "/kv/alpha", http.StatusMethodNotAllowed},
		{"unsupported method on health", http.MethodDelete, "/health", http.StatusMethodNotAllowed},
		{"unknown path", http.MethodGet, "/nope", http.StatusNotFound},
		{"missing key segment", http.MethodGet, "/kv/", http.StatusNotFound},
		{"key containing a slash", http.MethodGet, "/kv/a/b", http.StatusNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := do(t, h, tc.method, tc.path, "")
			if rec.Code != tc.want {
				t.Errorf("%s %s status = %d, want %d", tc.method, tc.path, rec.Code, tc.want)
			}
		})
	}
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()

	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decoding body %q: %v", rec.Body.String(), err)
	}
}

// stubCluster reports a fixed consensus state.
type stubCluster struct{ status replica.Status }

func (s stubCluster) Status() replica.Status { return s.status }

// TestLeaderRedirect covers the path a client hits when it addresses a
// follower. The write must be forwarded rather than refused.
func TestLeaderRedirect(t *testing.T) {
	t.Parallel()

	h := api.NewServer(
		notLeaderStore{},
		nil,
		api.WithCluster(stubCluster{status: replica.Status{
			ID:         "n2",
			Role:       "follower",
			Leader:     "n1",
			LeaderAddr: "http://127.0.0.1:9001",
		}}),
	).Handler()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"put", http.MethodPut, "/kv/alpha", "http://127.0.0.1:9001/kv/alpha"},
		{"get", http.MethodGet, "/kv/alpha", "http://127.0.0.1:9001/kv/alpha"},
		{"delete", http.MethodDelete, "/kv/alpha", "http://127.0.0.1:9001/kv/alpha"},
		{"query string is preserved", http.MethodGet, "/kv/alpha?x=1", "http://127.0.0.1:9001/kv/alpha?x=1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := do(t, h, tc.method, tc.path, "body")

			// 307 rather than 302: a 302 may legally be retried as a GET,
			// which would silently discard the write.
			if rec.Code != http.StatusTemporaryRedirect {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusTemporaryRedirect)
			}
			if got := rec.Header().Get("Location"); got != tc.want {
				t.Errorf("Location = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestNoLeaderReturnsUnavailable: mid-election there is nobody to redirect to.
func TestNoLeaderReturnsUnavailable(t *testing.T) {
	t.Parallel()

	h := api.NewServer(
		notLeaderStore{},
		nil,
		api.WithCluster(stubCluster{status: replica.Status{ID: "n2", Role: "follower"}}),
	).Handler()

	rec := do(t, h, http.MethodPut, "/kv/alpha", "value")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// TestNotLeaderWithoutClusterIsUnavailable guards the nil-cluster path.
func TestNotLeaderWithoutClusterIsUnavailable(t *testing.T) {
	t.Parallel()

	h := api.NewServer(notLeaderStore{}, nil).Handler()

	rec := do(t, h, http.MethodPut, "/kv/alpha", "value")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestStatusEndpoint(t *testing.T) {
	t.Parallel()

	t.Run("reports consensus state", func(t *testing.T) {
		t.Parallel()

		h := api.NewServer(store.New(), nil, api.WithCluster(stubCluster{
			status: replica.Status{ID: "n1", Role: "leader", Term: 7, Leader: "n1", CommitIndex: 12},
		})).Handler()

		rec := do(t, h, http.MethodGet, "/status", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		var got replica.Status
		decode(t, rec, &got)
		if got.Role != "leader" || got.Term != 7 || got.CommitIndex != 12 {
			t.Errorf("body = %+v, want leader at term 7 with commit 12", got)
		}
	})

	t.Run("reports single-node mode without a cluster", func(t *testing.T) {
		t.Parallel()

		h := api.NewServer(store.New(), nil).Handler()

		rec := do(t, h, http.MethodGet, "/status", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}

		var got map[string]string
		decode(t, rec, &got)
		if got["mode"] != "single-node" {
			t.Errorf("body = %v, want single-node mode", got)
		}
	})
}

func TestReplicationErrorsMapToStatusCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{"timeout is unavailable, not a failure", replica.ErrTimeout, http.StatusServiceUnavailable},
		{"shutting down", replica.ErrShuttingDown, http.StatusServiceUnavailable},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := api.NewServer(erroringStore{err: tc.err}, nil).Handler()

			rec := do(t, h, http.MethodPut, "/kv/alpha", "value")
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

// notLeaderStore fails every operation the way a follower does.
type notLeaderStore struct{}

func (notLeaderStore) Get(string) (string, error) { return "", raft.ErrNotLeader }
func (notLeaderStore) Put(string, string) error   { return raft.ErrNotLeader }
func (notLeaderStore) Delete(string) error        { return raft.ErrNotLeader }

// erroringStore fails every operation with a fixed error.
type erroringStore struct{ err error }

func (s erroringStore) Get(string) (string, error) { return "", s.err }
func (s erroringStore) Put(string, string) error   { return s.err }
func (s erroringStore) Delete(string) error        { return s.err }

// sessionRecorder records what reached the deduplicating write path.
type sessionRecorder struct {
	store.Store
	mu       sync.Mutex
	requests []replica.Request
	plain    int
}

func (s *sessionRecorder) PutRequest(key, value string, req replica.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	return nil
}

func (s *sessionRecorder) DeleteRequest(key string, req replica.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	return nil
}

func (s *sessionRecorder) Put(string, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plain++
	return nil
}

func (s *sessionRecorder) Delete(string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plain++
	return nil
}

func TestDeduplicationHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clientID  string
		seq       string
		wantDedup bool
		wantReq   replica.Request
	}{
		{
			name: "both headers present", clientID: "client-a", seq: "7",
			wantDedup: true, wantReq: replica.Request{ClientID: "client-a", Seq: 7},
		},
		{
			name:      "no headers at all",
			wantDedup: false,
		},
		{
			name: "client ID without a sequence number", clientID: "client-a",
			wantDedup: false,
		},
		{
			name: "sequence number without a client ID", seq: "7",
			wantDedup: false,
		},
		{
			name: "sequence number is not a number", clientID: "client-a", seq: "soon",
			wantDedup: false,
		},
		{
			name: "sequence number zero", clientID: "client-a", seq: "0",
			wantDedup: false,
		},
		{
			name: "negative sequence number", clientID: "client-a", seq: "-1",
			wantDedup: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			recorder := &sessionRecorder{Store: store.New()}
			h := api.NewServer(recorder, nil).Handler()

			req := httptest.NewRequest(http.MethodPut, "/kv/alpha", strings.NewReader("value"))
			if tc.clientID != "" {
				req.Header.Set(api.HeaderClientID, tc.clientID)
			}
			if tc.seq != "" {
				req.Header.Set(api.HeaderSeq, tc.seq)
			}

			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
			}

			recorder.mu.Lock()
			defer recorder.mu.Unlock()

			if tc.wantDedup {
				if len(recorder.requests) != 1 {
					t.Fatalf("deduplicating path called %d times, want 1", len(recorder.requests))
				}
				if recorder.requests[0] != tc.wantReq {
					t.Errorf("request = %+v, want %+v", recorder.requests[0], tc.wantReq)
				}
			} else {
				if recorder.plain != 1 {
					t.Errorf("plain path called %d times, want 1", recorder.plain)
				}
				if len(recorder.requests) != 0 {
					t.Errorf("deduplicating path was used with an unusable header pair: %+v",
						recorder.requests)
				}
			}
		})
	}
}

// TestDeduplicationHeadersOnDelete: the same handling applies to deletions.
func TestDeduplicationHeadersOnDelete(t *testing.T) {
	t.Parallel()

	recorder := &sessionRecorder{Store: store.New()}
	h := api.NewServer(recorder, nil).Handler()

	req := httptest.NewRequest(http.MethodDelete, "/kv/alpha", nil)
	req.Header.Set(api.HeaderClientID, "client-a")
	req.Header.Set(api.HeaderSeq, "3")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.requests) != 1 || recorder.requests[0].Seq != 3 {
		t.Errorf("requests = %+v, want one with seq 3", recorder.requests)
	}
}

// TestPlainStoreIgnoresHeaders: a store with no deduplication support must
// still serve the request rather than failing.
func TestPlainStoreIgnoresHeaders(t *testing.T) {
	t.Parallel()

	st := store.New()
	h := api.NewServer(st, nil).Handler()

	req := httptest.NewRequest(http.MethodPut, "/kv/alpha", strings.NewReader("value"))
	req.Header.Set(api.HeaderClientID, "client-a")
	req.Header.Set(api.HeaderSeq, "1")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got, err := st.Get("alpha"); err != nil || got != "value" {
		t.Errorf("store: %q, %v; want the write to have landed", got, err)
	}
}
