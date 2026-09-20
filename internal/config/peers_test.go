package config_test

import (
	"errors"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/config"
	"github.com/vermakmanish001/raft-store/internal/raft"
)

func TestParsePeers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
		want map[raft.NodeID]string
	}{
		{
			name: "empty spec is a valid single-node cluster",
			spec: "",
			want: map[raft.NodeID]string{},
		},
		{
			name: "whitespace only",
			spec: "   ",
			want: map[raft.NodeID]string{},
		},
		{
			name: "single peer",
			spec: "n2=http://127.0.0.1:8082",
			want: map[raft.NodeID]string{"n2": "http://127.0.0.1:8082"},
		},
		{
			name: "several peers",
			spec: "n2=http://127.0.0.1:8082,n3=http://127.0.0.1:8083",
			want: map[raft.NodeID]string{
				"n2": "http://127.0.0.1:8082",
				"n3": "http://127.0.0.1:8083",
			},
		},
		{
			name: "surrounding whitespace is tolerated",
			spec: " n2 = http://127.0.0.1:8082 , n3=http://127.0.0.1:8083 ",
			want: map[raft.NodeID]string{
				"n2": "http://127.0.0.1:8082",
				"n3": "http://127.0.0.1:8083",
			},
		},
		{
			name: "trailing slash is trimmed so URLs concatenate cleanly",
			spec: "n2=http://127.0.0.1:8082/",
			want: map[raft.NodeID]string{"n2": "http://127.0.0.1:8082"},
		},
		{
			name: "https is accepted",
			spec: "n2=https://node2.internal:8443",
			want: map[raft.NodeID]string{"n2": "https://node2.internal:8443"},
		},
		{
			name: "empty entries between commas are skipped",
			spec: "n2=http://127.0.0.1:8082,,",
			want: map[raft.NodeID]string{"n2": "http://127.0.0.1:8082"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := config.ParsePeers(tc.spec)
			if err != nil {
				t.Fatalf("ParsePeers(%q) error = %v, want nil", tc.spec, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %d peers, want %d: %v", len(got), len(tc.want), got)
			}
			for id, url := range tc.want {
				if got[id] != url {
					t.Errorf("peer %q = %q, want %q", id, got[id], url)
				}
			}
		})
	}
}

// TestParsePeersRejectsMisconfiguration matters more than it looks. A cluster
// that starts with a typo in one address runs with a node nobody can reach,
// silently halving its fault tolerance.
func TestParsePeersRejectsMisconfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec string
	}{
		{"missing equals sign", "n2"},
		{"empty node ID", "=http://127.0.0.1:8082"},
		{"duplicate node ID", "n2=http://a:1,n2=http://b:2"},
		{"missing scheme", "n2=127.0.0.1:8082"},
		{"unsupported scheme", "n2=tcp://127.0.0.1:8082"},
		{"no host", "n2=http://"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if _, err := config.ParsePeers(tc.spec); !errors.Is(err, config.ErrInvalidPeers) {
				t.Errorf("ParsePeers(%q) error = %v, want %v", tc.spec, err, config.ErrInvalidPeers)
			}
		})
	}
}

func TestPeerIDs(t *testing.T) {
	t.Parallel()

	peers, err := config.ParsePeers("n2=http://a:1,n3=http://b:2")
	if err != nil {
		t.Fatalf("ParsePeers: %v", err)
	}

	ids := config.PeerIDs(peers)
	if len(ids) != 2 {
		t.Fatalf("got %d ids, want 2", len(ids))
	}

	seen := map[raft.NodeID]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if !seen["n2"] || !seen["n3"] {
		t.Errorf("ids = %v, want n2 and n3", ids)
	}
}
