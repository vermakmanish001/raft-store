package ui_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/ui"
)

func TestServesThePage(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	ui.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}

	body := rec.Body.String()
	if !strings.Contains(body, "<!doctype html>") {
		t.Error("response does not look like an HTML document")
	}
	if len(body) < 1000 {
		t.Errorf("page is %d bytes, which is too small to be the dashboard", len(body))
	}
}

// TestPageHasNoExternalDependencies keeps the dashboard usable on a machine
// with no network, and keeps a node from making requests an operator did not
// ask for.
func TestPageHasNoExternalDependencies(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	ui.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	body := rec.Body.String()

	for _, forbidden := range []string{"http://", "https://", "//cdn", "<script src", "<link rel=\"stylesheet\""} {
		if strings.Contains(body, forbidden) {
			t.Errorf("page references %q; it must be entirely self-contained", forbidden)
		}
	}
}

// TestFetchesAreSameOrigin: the page must address the node that served it,
// using relative paths, so it works whichever member you open.
func TestFetchesAreSameOrigin(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	ui.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	body := rec.Body.String()

	for _, want := range []string{`fetch("/cluster"`, `"/kv/"`} {
		if !strings.Contains(body, want) {
			t.Errorf("page does not contain %q; it may be addressing a hardcoded host", want)
		}
	}
}

func TestUnknownPathsAreNotFound(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"/nope", "/index.html", "/../secrets"} {
		rec := httptest.NewRecorder()
		ui.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want %d; the handler serves one page only",
				path, rec.Code, http.StatusNotFound)
		}
	}
}
