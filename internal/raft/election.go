package raft

// campaign starts a new election: the node advances its term, votes for
// itself, and solicits votes from every peer (Section 5.2).
func (n *Node) campaign() []Message {
	n.becomeCandidate()

	// A single-node cluster has already reached a majority with its own vote.
	// This is not a special case bolted on; it falls out of quorum() being 1,
	// and handling it here avoids broadcasting to nobody and then waiting for
	// responses that can never arrive.
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
		return n.broadcastAppend()
	}

	msgs := make([]Message, 0, len(n.peers))
	for _, peer := range n.peers {
		msgs = append(msgs, RequestVote{
			Header:       Header{From: n.id, To: peer, Term: n.currentTerm},
			LastLogIndex: n.lastLogIndex(),
			LastLogTerm:  n.lastLogTerm(),
		})
	}
	return msgs
}

// handleRequestVote decides whether to vote for a candidate.
//
// Step has already reconciled terms, so by this point the candidate's term
// equals this node's term.
func (n *Node) handleRequestVote(m RequestVote) []Message {
	granted := n.shouldGrantVote(m)

	if granted {
		n.votedFor = m.From

		// Resetting the election timer only when a vote is actually granted
		// matters. A node that reset on every RequestVote could be kept from
		// ever campaigning by a peer with a stale log repeatedly asking for
		// votes it can never receive, which would stall the cluster.
		n.resetElectionTimer()
	}

	return []Message{RequestVoteResponse{
		Header:      Header{From: n.id, To: m.From, Term: n.currentTerm},
		VoteGranted: granted,
	}}
}

// shouldGrantVote applies the two conditions of Figure 2's RequestVote
// receiver implementation.
func (n *Node) shouldGrantVote(m RequestVote) bool {
	// At most one vote per term. Granting a second vote in the same term could
	// produce two leaders, since two disjoint majorities cannot exist but two
	// majorities that both contain this node can.
	//
	// Re-granting to the same candidate is permitted, and necessary: if the
	// original response was lost, the candidate retries, and refusing the
	// retry would strand an election that had in fact been won.
	if n.votedFor != "" && n.votedFor != m.From {
		return false
	}

	// The election restriction of Section 5.4.1. A candidate whose log is less
	// up to date than the voter's cannot win, which is what prevents a leader
	// from being elected without every committed entry.
	//
	// "Up to date" compares the last entry's term first, and only then its
	// index, because a longer log built under an older term can still be
	// missing entries that a shorter, newer log contains.
	lastTerm, lastIndex := n.lastLogTerm(), n.lastLogIndex()
	if m.LastLogTerm != lastTerm {
		return m.LastLogTerm > lastTerm
	}
	return m.LastLogIndex >= lastIndex
}

// handleRequestVoteResponse tallies a vote and promotes the node to leader
// once a majority is reached.
func (n *Node) handleRequestVoteResponse(m RequestVoteResponse) []Message {
	// Responses to an election this node is no longer contesting are stale.
	// It may have already won, already stepped down, or moved to a later term;
	// counting such a vote would be counting it for the wrong election.
	//
	// Step guarantees m.Term is not greater than currentTerm and has already
	// dropped anything lower, so only the role needs checking here.
	if n.role != Candidate {
		return nil
	}

	// Keyed by voter, so a duplicate response cannot be counted twice.
	n.votes[m.From] = m.VoteGranted

	granted := 0
	for _, ok := range n.votes {
		if ok {
			granted++
		}
	}
	if granted < n.quorum() {
		return nil
	}

	n.becomeLeader()

	// Assert leadership immediately. Every other node is still counting down
	// to its own election, and the first heartbeat is what stops them.
	return n.broadcastAppend()
}
