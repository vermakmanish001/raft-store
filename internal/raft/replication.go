package raft

// This file carries the AppendEntries half of the protocol: how a leader
// pushes entries to followers, how a follower reconciles a divergent log, and
// how entries become committed.

// broadcastAppend sends each peer whatever entries the leader believes it is
// missing. A peer that is fully caught up receives an empty request, which is
// the heartbeat.
//
// Raft uses one RPC for both jobs deliberately. Because every heartbeat
// carries the consistency check, a leader discovers a divergent follower on
// the next heartbeat rather than waiting for the next client write.
func (n *Node) broadcastAppend() []Message {
	msgs := make([]Message, 0, len(n.peers))
	for _, peer := range n.peers {
		if msg := n.appendTo(peer); msg != nil {
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

// appendTo builds the message destined for one peer, which is a snapshot when
// the entries that peer needs have already been compacted away.
func (n *Node) appendTo(peer NodeID) Message {
	next := n.nextIndex[peer]
	if next == 0 {
		// Defensive: nextIndex is 1-based, so 0 means the entry is missing
		// from the map. Treating it as 1 replicates from the start of the log
		// rather than indexing out of range.
		next = 1
	}

	prevIndex := next - 1

	// The follower needs entries this leader no longer holds. Ordinary
	// repair walks nextIndex backward until the logs agree, but there is
	// nothing to walk back to once the prefix is inside a snapshot, so the
	// snapshot is sent instead.
	if prevIndex < n.snapshotIndex {
		return n.snapshotTo(peer)
	}

	return AppendEntries{
		Header:       Header{From: n.id, To: peer, Term: n.currentTerm},
		PrevLogIndex: prevIndex,
		PrevLogTerm:  n.termAt(prevIndex),
		Entries:      n.entriesFrom(next),
		LeaderCommit: n.commitIndex,
		ReadSeq:      n.readSeq,
	}
}

// handleAppendEntries processes a request from a leader. Step has already
// reconciled terms, so m.Term equals currentTerm.
func (n *Node) handleAppendEntries(m AppendEntries) []Message {
	// Receiving AppendEntries at the current term proves a leader was elected
	// for it. A candidate must abandon its campaign: at most one leader exists
	// per term, and this is not it.
	if n.role == Candidate {
		n.becomeFollower(m.Term, m.From)
	}

	// Recognize the sender as leader even if this node already considered
	// itself a follower, since it may not have known who the leader was.
	n.leaderID = m.From

	// Hearing from the leader defers this node's next election. This happens
	// even when the request is rejected below: a divergent log means this node
	// is behind, not that the leader is illegitimate, so campaigning against
	// it would only cost the cluster a term.
	n.resetElectionTimer()

	// The log matching check of Section 5.3.
	if !n.logMatches(m.PrevLogIndex, m.PrevLogTerm) {
		conflictIndex, conflictTerm := n.describeConflict(m.PrevLogIndex)
		return []Message{AppendEntriesResponse{
			Header:        Header{From: n.id, To: m.From, Term: n.currentTerm},
			Success:       false,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,

			// Echoed even on rejection. The counter proves this node still
			// recognizes the sender as leader, which is all a read barrier
			// asks; whether its log happens to match is a separate question.
			ReadSeq: m.ReadSeq,
		}}
	}

	n.mergeEntries(m.Entries)

	// Adopt the leader's commit index, but never past the last entry this
	// request actually delivered.
	//
	// The bound matters. A follower whose log is shorter than the leader's
	// commit index would otherwise mark entries committed that it does not
	// have, and then hand garbage to the state machine when asked to apply
	// them.
	if m.LeaderCommit > n.commitIndex {
		lastNew := m.PrevLogIndex + Index(len(m.Entries))
		n.commitIndex = min(m.LeaderCommit, lastNew)
	}

	return []Message{AppendEntriesResponse{
		Header:  Header{From: n.id, To: m.From, Term: n.currentTerm},
		Success: true,

		// Confirm exactly what this request delivered, not the log's end. The
		// follower may hold further entries from a deposed leader, and those
		// are not evidence that this leader's entries replicated.
		MatchIndex: m.PrevLogIndex + Index(len(m.Entries)),
		ReadSeq:    m.ReadSeq,
	}}
}

// mergeEntries appends the leader's entries, discarding any local suffix that
// conflicts with them.
func (n *Node) mergeEntries(entries []LogEntry) {
	for i, entry := range entries {
		existing, ok := n.entryAt(entry.Index)
		if !ok {
			// Past the end of the local log: everything from here is new.
			n.appendPersisted(entries[i:])
			return
		}

		if existing.Term == entry.Term {
			// Already present. Skipping rather than rewriting is what makes a
			// duplicate or reordered request harmless, and it is essential:
			// truncating here would delete committed entries every time a
			// retransmitted request arrived.
			continue
		}

		// Same index, different term. The local entry conflicts with the
		// leader's and cannot have been committed, so it and everything after
		// it are discarded (Section 5.3).
		n.truncateFrom(entry.Index)
		n.appendPersisted(entries[i:])
		return
	}
}

// describeConflict reports where this node's log diverges from what the leader
// expected, so the leader can back up by a whole term instead of one entry.
func (n *Node) describeConflict(prevLogIndex Index) (Index, Term) {
	// Case 1: the log is simply too short. Tell the leader where it ends, so
	// it resumes from there rather than probing backward through entries this
	// node never had.
	if prevLogIndex > n.lastLogIndex() {
		return n.lastLogIndex() + 1, 0
	}

	// Case 2: an entry exists but belongs to a different term. Report the
	// first index of that term, so the leader skips the entire conflicting
	// term in one exchange.
	conflictTerm := n.termAt(prevLogIndex)
	first := prevLogIndex
	for first > 1 && n.termAt(first-1) == conflictTerm {
		first--
	}
	return first, conflictTerm
}

// handleAppendEntriesResponse updates the leader's view of a follower and
// advances the commit index when a majority has caught up.
func (n *Node) handleAppendEntriesResponse(m AppendEntriesResponse) []Message {
	// Responses to a leadership this node no longer holds are meaningless.
	// Step has already handled the case where the responder's term is higher,
	// which would have deposed this node before reaching here.
	if n.role != Leader {
		return nil
	}

	// Recorded before the success check. A follower that rejected an append
	// still answered, and answering is what a read barrier needs to know.
	n.recordReadAck(m.From, m.ReadSeq)

	if !m.Success {
		n.backoffNextIndex(m)

		// Retry immediately rather than waiting for the next heartbeat. A
		// follower that is far behind would otherwise need one full heartbeat
		// interval per round of backtracking.
		if msg := n.appendTo(m.From); msg != nil {
			return []Message{msg}
		}
		return nil
	}

	// Responses can arrive out of order, and an older one carries a smaller
	// MatchIndex. Taking the maximum keeps a delayed response from retracting
	// progress that a later one already confirmed.
	if m.MatchIndex > n.matchIndex[m.From] {
		n.matchIndex[m.From] = m.MatchIndex
		n.nextIndex[m.From] = m.MatchIndex + 1
		n.maybeAdvanceCommit()
	}
	return nil
}

// backoffNextIndex walks a follower's nextIndex backward after a rejection,
// using the conflict hint when it is usable.
func (n *Node) backoffNextIndex(m AppendEntriesResponse) {
	next := n.nextIndex[m.From]

	switch {
	case m.ConflictTerm == 0 && m.ConflictIndex > 0:
		// The follower's log ends before the leader expected. Resume from its
		// actual end.
		next = m.ConflictIndex

	case m.ConflictTerm > 0:
		// The follower holds a conflicting term. If the leader also has
		// entries in that term, resume just after them, since everything up to
		// there already agrees. Otherwise skip the whole term.
		next = m.ConflictIndex
		for i := n.lastLogIndex(); i > 0; i-- {
			if n.termAt(i) == m.ConflictTerm {
				next = i + 1
				break
			}
		}

	default:
		// No usable hint. Fall back to the paper's one-entry decrement.
		if next > 1 {
			next--
		}
	}

	// nextIndex must never reach zero: the log is 1-indexed, and index 0 is
	// the position before the first entry, which is where replication starts.
	if next < 1 {
		next = 1
	}
	n.nextIndex[m.From] = next
}

// maybeAdvanceCommit raises the commit index to the highest entry replicated
// on a majority, subject to the restriction in Section 5.4.2.
func (n *Node) maybeAdvanceCommit() {
	if n.role != Leader {
		return
	}

	for candidate := n.lastLogIndex(); candidate > n.commitIndex; candidate-- {
		// The rule illustrated by Figure 8 of the paper, and the subtlest
		// requirement in Raft: a leader may only commit an entry from its OWN
		// term by counting replicas.
		//
		// An entry from an earlier term can be present on a majority and still
		// be overwritten later, because a future leader elected under the
		// election restriction may legitimately hold a different entry at that
		// index. Committing on replica count alone would therefore let already
		// applied data be silently replaced. Entries from previous terms
		// become committed indirectly, carried along when an entry from the
		// current term commits above them.
		//
		// Removing this check leaves every test passing and the system quietly
		// capable of losing acknowledged writes.
		if n.termAt(candidate) != n.currentTerm {
			continue
		}

		// The leader holds every entry in its own log, so it counts itself.
		replicas := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= candidate {
				replicas++
			}
		}

		if replicas >= n.quorum() {
			n.commitIndex = candidate
			return
		}
	}
}
