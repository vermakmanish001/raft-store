package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/api"
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
