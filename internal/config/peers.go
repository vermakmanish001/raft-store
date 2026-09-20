// Package config parses cluster configuration supplied on the command line.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/vermakmanish001/raft-store/internal/raft"
)

// ErrInvalidPeers reports a malformed peer specification.
var ErrInvalidPeers = errors.New("config: invalid peer list")

// ParsePeers parses a comma-separated list of "id=url" pairs, as in
//
//	n2=http://127.0.0.1:8082,n3=http://127.0.0.1:8083
//
// An empty spec yields an empty map, which is a valid single-node cluster.
//
// Every failure here is a startup misconfiguration, and each is reported with
// the offending text. A cluster that starts with a typo in one address does
// not fail visibly: it runs with a node nobody can reach, quietly halving its
// fault tolerance until the day a second node fails.
func ParsePeers(spec string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)

	spec = strings.TrimSpace(spec)
	if spec == "" {
		return peers, nil
	}

	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		id, rawURL, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("%w: %q is not in id=url form", ErrInvalidPeers, entry)
		}

		id = strings.TrimSpace(id)
		rawURL = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rawURL), "/"))

		if id == "" {
			return nil, fmt.Errorf("%w: empty node ID in %q", ErrInvalidPeers, entry)
		}
		if _, exists := peers[raft.NodeID(id)]; exists {
			return nil, fmt.Errorf("%w: node ID %q appears more than once", ErrInvalidPeers, id)
		}

		parsed, err := url.Parse(rawURL)
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %v", ErrInvalidPeers, entry, err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("%w: %q needs an http or https scheme", ErrInvalidPeers, entry)
		}
		if parsed.Host == "" {
			return nil, fmt.Errorf("%w: %q has no host", ErrInvalidPeers, entry)
		}

		peers[raft.NodeID(id)] = rawURL
	}

	return peers, nil
}

// PeerIDs returns the node IDs from a peer map, which is what the raft package
// needs to size a quorum.
func PeerIDs(peers map[raft.NodeID]string) []raft.NodeID {
	ids := make([]raft.NodeID, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	return ids
}
