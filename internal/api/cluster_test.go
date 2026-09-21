package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/api"
	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/replica"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// peerServer stands in for another node answering /status.
func peerServer(t *testing.T, status replica.Status) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClusterViewGathersEveryMember(t *testing.T) {
	t.Parallel()

	peer := peerServer(t, replica.Status{
		ID: "n2", Role: "follower", Term: 7, Leader: "n1", CommitIndex: 42,
	})

	h := api.NewServer(store.New(), nil, api.WithCluster(stubCluster{
		status: replica.Status{ID: "n1", Role: "leader", Term: 7, Leader: "n1", CommitIndex: 42},
		peers: map[raft.NodeID]string{
			"n1": "http://127.0.0.1:1", // this node, read locally rather than fetched
			"n2": peer.URL,
		},
	})).Handler()

	rec := do(t, h, http.MethodGet, "/cluster", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var view api.ClusterView
	decode(t, rec, &view)

	if view.ObservedBy != "n1" {
		t.Errorf("ObservedBy = %q, want %q", view.ObservedBy, "n1")
	}
	if len(view.Nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(view.Nodes))
	}

	// Sorted by ID so the display does not reshuffle between polls.
	if view.Nodes[0].ID != "n1" || view.Nodes[1].ID != "n2" {
		t.Errorf("nodes = %q, %q; want them sorted by ID", view.Nodes[0].ID, view.Nodes[1].ID)
	}

	self := view.Nodes[0]
	if !self.Self {
		t.Error("the observing node is not marked as self")
	}
	if !self.Reachable || self.Status == nil || self.Status.Role != "leader" {
		t.Errorf("self = %+v, want a reachable leader read locally", self)
	}

	other := view.Nodes[1]
	if !other.Reachable || other.Status == nil {
		t.Fatalf("peer = %+v, want it reachable", other)
	}
	if other.Status.Term != 7 || other.Status.CommitIndex != 42 {
		t.Errorf("peer status = %+v, want term 7 and commit 42", other.Status)
	}
	if other.Self {
		t.Error("a peer is marked as self")
	}
}

// TestUnreachablePeerIsReportedNotFatal: a node being down is the interesting
// answer, not a failure. The view must still render the rest.
func TestUnreachablePeerIsReportedNotFatal(t *testing.T) {
	t.Parallel()

	h := api.NewServer(store.New(), nil, api.WithCluster(stubCluster{
		status: replica.Status{ID: "n1", Role: "leader", Term: 2, Leader: "n1"},
		peers: map[raft.NodeID]string{
			"n1": "http://127.0.0.1:1",
			// Port 1 on loopback refuses connections immediately.
			"n2": "http://127.0.0.1:1",
		},
	})).Handler()

	rec := do(t, h, http.MethodGet, "/cluster", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; one dead peer must not fail the whole view", rec.Code, http.StatusOK)
	}

	var view api.ClusterView
	decode(t, rec, &view)

	if len(view.Nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(view.Nodes))
	}
	if !view.Nodes[0].Reachable {
		t.Error("the observing node is reported unreachable")
	}
	if view.Nodes[1].Reachable {
		t.Error("a peer refusing connections is reported reachable")
	}
	if view.Nodes[1].Error == "" {
		t.Error("an unreachable peer carried no explanation")
	}
}

func TestPeerAnsweringBadlyIsReportedUnreachable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "boom", http.StatusInternalServerError)
			},
		},
		{
			name: "unparseable body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("this is not json"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			peer := httptest.NewServer(tc.handler)
			defer peer.Close()

			h := api.NewServer(store.New(), nil, api.WithCluster(stubCluster{
				status: replica.Status{ID: "n1", Role: "leader"},
				peers:  map[raft.NodeID]string{"n1": "http://x", "n2": peer.URL},
			})).Handler()

			rec := do(t, h, http.MethodGet, "/cluster", "")
			var view api.ClusterView
			decode(t, rec, &view)

			if view.Nodes[1].Reachable {
				t.Errorf("a peer answering with %s is reported reachable", tc.name)
			}
		})
	}
}

func TestClusterViewWithoutConsensus(t *testing.T) {
	t.Parallel()

	h := api.NewServer(store.New(), nil).Handler()

	rec := do(t, h, http.MethodGet, "/cluster", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var view api.ClusterView
	decode(t, rec, &view)
	if len(view.Nodes) != 0 {
		t.Errorf("got %d nodes, want none for a node with no cluster", len(view.Nodes))
	}
}

// TestCrossOriginHeaders: the dashboard is served by one node, and a write
// addressed to a follower is redirected to the leader on a different port,
// which a browser treats as cross-origin.
func TestCrossOriginHeaders(t *testing.T) {
	t.Parallel()

	h := api.NewServer(store.New(), nil).Handler()

	rec := do(t, h, http.MethodGet, "/status", "")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("Access-Control-Allow-Origin = %q, want %q", got, "*")
	}

	t.Run("preflight is answered without a body", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodOptions, "/kv/alpha", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Errorf("preflight status = %d, want %d", rec.Code, http.StatusNoContent)
		}
		if !strings.Contains(rec.Header().Get("Access-Control-Allow-Methods"), "PUT") {
			t.Errorf("Allow-Methods = %q, want it to include PUT",
				rec.Header().Get("Access-Control-Allow-Methods"))
		}
	})
}
