package raft

// This file carries the AppendEntries half of the protocol. At this stage only
// the heartbeat path is exercised: no client writes reach the log yet, so a
// leader never populates Entries. The consistency check is implemented in full
// regardless, because it is what makes a heartbeat useful for detecting
// divergence, and because leaving it until entries exist would mean adding the
// subtlest part of the algorithm to code already in motion.

// broadcastHeartbeat sends an empty AppendEntries to every peer.
//
// Heartbeats are what convert a won election into sustained leadership. A
// leader that stops sending them is deposed within one election timeout, which
// is the mechanism by which a partitioned or crashed leader is replaced
// without any explicit failure detector.
func (n *Node) broadcastHeartbeat() []Message {
	msgs := make([]Message, 0, len(n.peers))
	for _, peer := range n.peers {
		msgs = append(msgs, AppendEntries{
			Header:       Header{From: n.id, To: peer, Term: n.currentTerm},
			PrevLogIndex: n.lastLogIndex(),
			PrevLogTerm:  n.lastLogTerm(),
			Entries:      nil,
			LeaderCommit: 0, // commit tracking arrives with log replication
		})
	}
	return msgs
}

// handleAppendEntries processes a heartbeat or replication request from a
// leader. Step has already reconciled terms, so m.Term equals currentTerm.
func (n *Node) handleAppendEntries(m AppendEntries) []Message {
	// Receiving AppendEntries at the current term proves a leader was elected
	// for it. A candidate must therefore abandon its campaign: at most one
	// leader exists per term, and this is not it.
	if n.role == Candidate {
		n.becomeFollower(m.Term, m.From)
	}

	// Recognize the sender as leader even if this node already considered
	// itself a follower, since it may not have known who the leader was.
	n.leaderID = m.From

	// Hearing from the leader is the entire purpose of a heartbeat: it defers
	// this node's next election.
	n.resetElectionTimer()

	// The log matching check of Section 5.3. PrevLogIndex 0 means the leader
	// is describing the very start of the log, which every log trivially
	// agrees on, so no check applies.
	if !n.logMatches(m.PrevLogIndex, m.PrevLogTerm) {
		return []Message{AppendEntriesResponse{
			Header:     Header{From: n.id, To: m.From, Term: n.currentTerm},
			Success:    false,
			MatchIndex: 0,
		}}
	}

	// Appending m.Entries belongs to the next milestone. A leader does not
	// populate that field yet, so there is nothing here to drop on the floor.

	return []Message{AppendEntriesResponse{
		Header:     Header{From: n.id, To: m.From, Term: n.currentTerm},
		Success:    true,
		MatchIndex: n.lastLogIndex(),
	}}
}

// logMatches reports whether this node's log contains an entry at index with
// the given term, which is the precondition for accepting entries after it.
func (n *Node) logMatches(index Index, term Term) bool {
	if index == 0 {
		return true
	}
	if index > n.lastLogIndex() {
		return false
	}

	// The log is 1-indexed while the slice is 0-indexed. Entries carry their
	// own Index, so this stays correct once compaction makes the slice a
	// suffix of the logical log rather than the whole of it.
	entry := n.log[index-1]
	return entry.Term == term
}

// handleAppendEntriesResponse processes a follower's reply.
//
// A leader will use these to track how far each follower has replicated, which
// is how it decides an entry is committed. With no entries in flight there is
// nothing to track yet, so the response is acknowledged and discarded. The
// term reconciliation that matters, a follower reporting a higher term and
// thereby deposing this leader, has already happened in Step.
func (n *Node) handleAppendEntriesResponse(AppendEntriesResponse) []Message {
	return nil
}
