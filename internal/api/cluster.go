package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/replica"
)

// peerStatusTimeout bounds how long the cluster view waits on any one peer.
//
// It is deliberately short. This endpoint exists to show an operator what is
// happening, and an unreachable node is itself the interesting answer. Waiting
// on it would make the whole view hang at exactly the moment it matters most.
const peerStatusTimeout = 750 * time.Millisecond

// NodeView is one member's state as seen from the node serving the request.
type NodeView struct {
	ID   raft.NodeID `json:"id"`
	Addr string      `json:"addr"`

	// Self marks the node that answered this request, which is the only one
	// whose state is read directly rather than over the network.
	Self bool `json:"self"`

	// Reachable reports whether this member answered. An unreachable node is a
	// normal condition, not an error: it may be restarting, partitioned, or
	// mid-election.
	Reachable bool `json:"reachable"`

	Status *replica.Status `json:"status,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// ClusterView is the whole cluster as one node sees it.
//
// It is explicitly a point of view rather than ground truth. Members disagree
// during an election, and a partitioned node will report a leader that no
// longer leads. Presenting it as one node's opinion is honest; merging the
// answers into a single authoritative picture would invent agreement that does
// not exist.
type ClusterView struct {
	ObservedBy raft.NodeID `json:"observed_by"`
	Nodes      []NodeView  `json:"nodes"`
}

// handleCluster reports every member's state, fanning out to peers.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		s.writeJSON(w, r, http.StatusOK, ClusterView{Nodes: nil})
		return
	}

	self := s.cluster.Status()
	peers := s.cluster.Peers()

	views := make([]NodeView, 0, len(peers))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for id, addr := range peers {
		if id == self.ID {
			views = append(views, NodeView{
				ID: id, Addr: addr, Self: true, Reachable: true, Status: &self,
			})
			continue
		}

		wg.Add(1)
		go func(id raft.NodeID, addr string) {
			defer wg.Done()

			view := s.fetchPeer(r.Context(), id, addr)

			mu.Lock()
			views = append(views, view)
			mu.Unlock()
		}(id, addr)
	}
	wg.Wait()

	// Sorted by ID so the display does not reshuffle between polls, which
	// would make it unreadable.
	sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })

	s.writeJSON(w, r, http.StatusOK, ClusterView{ObservedBy: self.ID, Nodes: views})
}

// fetchPeer asks one peer for its status.
func (s *Server) fetchPeer(ctx context.Context, id raft.NodeID, addr string) NodeView {
	view := NodeView{ID: id, Addr: addr}

	ctx, cancel := context.WithTimeout(ctx, peerStatusTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/status", nil)
	if err != nil {
		view.Error = err.Error()
		return view
	}

	resp, err := s.peerClient.Do(req)
	if err != nil {
		// Unreachable is the answer, not a failure to report.
		view.Error = "unreachable"
		return view
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		view.Error = resp.Status
		return view
	}

	var status replica.Status
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&status); err != nil {
		view.Error = "unreadable response"
		return view
	}

	view.Reachable = true
	view.Status = &status
	return view
}
