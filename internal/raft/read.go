package raft

import "sort"

// ReadRequest is a barrier a leader must clear before a read may be served.
//
// It pairs the commit index at the moment the read arrived with the round of
// leadership confirmation that must complete first. Serving the read once both
// conditions hold makes it linearizable: the value returned reflects every
// write that completed before the read was issued, and none that had not yet
// been proposed.
type ReadRequest struct {
	// Seq identifies the confirmation round. The read may be served once a
	// majority of the cluster has echoed a value at least this high.
	Seq uint64

	// Index is the commit index when the read was registered. The state
	// machine must have applied up to here, or the read could return state
	// older than a write that had already been acknowledged.
	Index Index

	// Term guards against the barrier outliving the leadership that created
	// it. A read confirmed under one term means nothing under the next.
	Term Term
}

// ReadIndex registers a linearizable read barrier and returns the messages
// that will confirm it.
//
// Only a leader may serve a linearizable read, and even a leader may not serve
// one from local state alone. Two things can be stale. This node may have been
// deposed without noticing, in which case a newer leader has since committed
// writes it has never seen. And its own state machine may lag its commit
// index, so even a legitimate leader can hold entries it has not applied.
// Index addresses the second; Seq addresses the first.
func (n *Node) ReadIndex() (ReadRequest, []Message, error) {
	if n.fatal != nil {
		return ReadRequest{}, nil, n.fatal
	}
	if n.role != Leader {
		return ReadRequest{}, nil, ErrNotLeader
	}

	n.readSeq++

	req := ReadRequest{
		Seq:   n.readSeq,
		Index: n.commitIndex,
		Term:  n.currentTerm,
	}

	// A heartbeat round carrying the new counter. Piggybacking on
	// AppendEntries rather than adding an RPC means the confirmation costs no
	// extra round trip when a heartbeat was due anyway.
	return req, n.broadcastAppend(), nil
}

// ReadReady reports whether a read barrier has been satisfied.
func (n *Node) ReadReady(req ReadRequest) bool {
	// Leadership lost, or a new term began. Confirmation from the old term
	// proves nothing about the new one, so the read must be abandoned rather
	// than served.
	if n.role != Leader || n.currentTerm != req.Term {
		return false
	}
	return n.confirmedReadSeq() >= req.Seq && n.lastApplied >= req.Index
}

// ReadExpired reports whether a barrier can never be satisfied, because this
// node no longer leads the term the read was registered in.
//
// Callers distinguish this from "not ready yet" so a client waiting on a read
// is told to retry elsewhere rather than waiting for a timeout.
func (n *Node) ReadExpired(req ReadRequest) bool {
	return n.role != Leader || n.currentTerm != req.Term
}

// confirmedReadSeq returns the highest counter a majority of the cluster has
// echoed.
//
// The calculation mirrors commit index advancement: sort what every member has
// acknowledged, then take the value at the quorum position. That entry is, by
// construction, one that a majority has reached or exceeded.
func (n *Node) confirmedReadSeq() uint64 {
	// The leader has trivially acknowledged its own counter.
	acked := make([]uint64, 0, len(n.peers)+1)
	acked = append(acked, n.readSeq)

	for _, peer := range n.peers {
		acked = append(acked, n.ackedRead[peer])
	}

	sort.Slice(acked, func(i, j int) bool { return acked[i] > acked[j] })
	return acked[n.quorum()-1]
}

// recordReadAck notes a follower echoing a read counter.
func (n *Node) recordReadAck(peer NodeID, seq uint64) {
	// Responses can arrive out of order, and an older one carries a smaller
	// counter. Taking the maximum keeps a delayed response from retracting a
	// confirmation a later one already established.
	if seq > n.ackedRead[peer] {
		n.ackedRead[peer] = seq
	}
}
