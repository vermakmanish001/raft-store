package raft

import "fmt"

// This file carries snapshot transfer: how a leader hands a lagging follower a
// snapshot, and how a follower installs one.

// snapshotTo builds the InstallSnapshot destined for one peer.
func (n *Node) snapshotTo(peer NodeID) Message {
	_, data, err := n.storage.LoadSnapshot()
	if err != nil {
		n.setFatal(fmt.Errorf("raft: loading snapshot to send to %s: %w", peer, err))
		return nil
	}

	return InstallSnapshot{
		Header:            Header{From: n.id, To: peer, Term: n.currentTerm},
		LastIncludedIndex: n.snapshotIndex,
		LastIncludedTerm:  n.snapshotTerm,
		Data:              data,
	}
}

// handleInstallSnapshot installs a snapshot from the leader. Step has already
// reconciled terms, so m.Term equals currentTerm.
func (n *Node) handleInstallSnapshot(m InstallSnapshot) []Message {
	if n.role == Candidate {
		n.becomeFollower(m.Term, m.From)
	}
	n.leaderID = m.From
	n.resetElectionTimer()

	ack := func(match Index) []Message {
		return []Message{InstallSnapshotResponse{
			Header:     Header{From: n.id, To: m.From, Term: n.currentTerm},
			MatchIndex: match,
		}}
	}

	// A snapshot that ends before what this node has already committed adds
	// nothing, and installing it would move the node backward. This happens
	// routinely when a slow snapshot arrives after ordinary replication has
	// already caught the follower up.
	if m.LastIncludedIndex <= n.commitIndex {
		return ack(n.lastLogIndex())
	}

	// If this node already holds the entry the snapshot ends at, and agrees on
	// its term, then everything before it matches too, by the log matching
	// property. The entries after it are still good and are kept rather than
	// discarded and re-fetched.
	if n.logMatches(m.LastIncludedIndex, m.LastIncludedTerm) {
		n.log = n.entriesFrom(m.LastIncludedIndex + 1)
	} else {
		// The logs diverge at or before the snapshot boundary, so nothing this
		// node holds can be trusted. The snapshot is authoritative: it
		// describes committed state, and committed state is identical on every
		// node that has it.
		n.log = nil
	}

	if err := n.storage.SaveSnapshot(SnapshotMeta{
		Index: m.LastIncludedIndex,
		Term:  m.LastIncludedTerm,
	}, m.Data); err != nil {
		n.setFatal(fmt.Errorf("raft: persisting received snapshot at index %d: %w",
			m.LastIncludedIndex, err))
		return nil
	}

	n.snapshotIndex = m.LastIncludedIndex
	n.snapshotTerm = m.LastIncludedTerm

	// The snapshot describes committed, applied state by construction, so
	// both markers jump to its boundary. Leaving lastApplied behind would make
	// the node try to re-apply entries it no longer holds.
	if n.commitIndex < m.LastIncludedIndex {
		n.commitIndex = m.LastIncludedIndex
	}
	n.lastApplied = m.LastIncludedIndex

	// Handed to the caller so it can replace its state machine. Until it does,
	// this node's state machine and its log position disagree.
	n.pendingSnapshot = m.Data

	return ack(m.LastIncludedIndex)
}

// handleInstallSnapshotResponse updates the leader's view of a follower that
// has accepted a snapshot.
func (n *Node) handleInstallSnapshotResponse(m InstallSnapshotResponse) []Message {
	if n.role != Leader {
		return nil
	}

	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
		n.maybeAdvanceCommit()
	}

	// Resume ordinary replication immediately. The follower is now positioned
	// at the snapshot boundary and usually needs the entries that follow it.
	if msg := n.appendTo(m.From); msg != nil {
		return []Message{msg}
	}
	return nil
}
