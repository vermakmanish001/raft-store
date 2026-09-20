package raft

import (
	"errors"
	"testing"
)

func TestReadIndexRejectedByNonLeader(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")

	if _, _, err := n.ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Errorf("ReadIndex on a follower = %v, want %v", err, ErrNotLeader)
	}

	n.becomeCandidate()
	if _, _, err := n.ReadIndex(); !errors.Is(err, ErrNotLeader) {
		t.Errorf("ReadIndex on a candidate = %v, want %v", err, ErrNotLeader)
	}
}

// TestSingleNodeReadIsImmediatelyReady: with a quorum of one there is nobody
// to hear from, so the only remaining condition is having applied the entries.
func TestSingleNodeReadIsImmediatelyReady(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0")
	n.becomeCandidate()
	n.becomeLeader()
	n.CommittedEntries() // apply the term no-op

	req, msgs, err := n.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("got %d messages, want none for a single-node cluster", len(msgs))
	}
	if !n.ReadReady(req) {
		t.Error("ReadReady = false, want true")
	}
}

// TestReadWaitsForMajorityConfirmation is the heart of the mechanism. Until a
// majority answers, this node cannot know whether it is still the leader.
func TestReadWaitsForMajorityConfirmation(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2") // quorum 2
	n.becomeCandidate()
	n.becomeLeader()
	n.commitIndex = n.lastLogIndex()
	n.CommittedEntries()

	req, msgs, err := n.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want one per peer", len(msgs))
	}

	if n.ReadReady(req) {
		t.Fatal("ReadReady = true before any peer answered; the node cannot yet know it still leads")
	}

	// One peer answering makes two of three, a majority.
	n.Step(AppendEntriesResponse{
		Header:     Header{From: "n1", To: "n0", Term: n.Term()},
		Success:    true,
		MatchIndex: n.lastLogIndex(),
		ReadSeq:    req.Seq,
	})

	if !n.ReadReady(req) {
		t.Error("ReadReady = false after a majority answered, want true")
	}
}

// TestReadIsConfirmedByARejectedAppend: a follower that refuses an append has
// still acknowledged the leader, which is all the barrier asks.
func TestReadIsConfirmedByARejectedAppend(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()
	n.commitIndex = n.lastLogIndex()
	n.CommittedEntries()

	req, _, err := n.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	n.Step(AppendEntriesResponse{
		Header:        Header{From: "n1", To: "n0", Term: n.Term()},
		Success:       false,
		ConflictIndex: 1,
		ReadSeq:       req.Seq,
	})

	if !n.ReadReady(req) {
		t.Error("ReadReady = false, want true: a rejection still proves the follower recognizes this leader")
	}
}

// TestReadWaitsForApply covers the second staleness source. A legitimate
// leader can hold committed entries it has not yet handed to the state
// machine, and reading before then returns state older than an acknowledged
// write.
func TestReadWaitsForApply(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()

	if _, _, err := n.Propose([]byte("a write the client already saw succeed")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.matchIndex["n1"] = n.lastLogIndex()
	n.maybeAdvanceCommit()

	req, _, err := n.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	n.Step(AppendEntriesResponse{
		Header:     Header{From: "n1", To: "n0", Term: n.Term()},
		Success:    true,
		MatchIndex: n.lastLogIndex(),
		ReadSeq:    req.Seq,
	})

	if n.ReadReady(req) {
		t.Fatal("ReadReady = true before the entries were applied; the read would miss a committed write")
	}

	n.CommittedEntries() // the state machine catches up

	if !n.ReadReady(req) {
		t.Error("ReadReady = false after applying, want true")
	}
}

func TestReadExpiresWhenLeadershipIsLost(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()
	n.commitIndex = n.lastLogIndex()
	n.CommittedEntries()

	req, _, err := n.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}

	// A higher term arrives and deposes this node.
	n.Step(AppendEntries{Header: Header{From: "n1", To: "n0", Term: n.Term() + 1}})

	if !n.ReadExpired(req) {
		t.Error("ReadExpired = false after being deposed, want true")
	}
	if n.ReadReady(req) {
		t.Error("ReadReady = true after being deposed; confirmation from the old term proves nothing")
	}
}

// TestReadCountersResetOnElection: acknowledgements collected by a previous
// leader must not count toward this one's barriers.
func TestReadCountersResetOnElection(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()
	n.ackedRead["n1"] = 500
	n.readSeq = 400

	// Deposed, then elected again in a later term.
	n.becomeFollower(n.currentTerm+1, "n1")
	n.becomeCandidate()
	n.becomeLeader()

	if n.readSeq != 0 {
		t.Errorf("readSeq = %d after a new election, want 0", n.readSeq)
	}
	for _, peer := range []NodeID{"n1", "n2"} {
		if got := n.ackedRead[peer]; got != 0 {
			t.Errorf("ackedRead[%s] = %d, want 0; a previous leader's acks are not ours", peer, got)
		}
	}
}

// TestPartitionedLeaderCannotServeReads is the safety property. A leader cut
// off from its cluster believes it still leads until its election timeout
// elapses, and would otherwise answer reads from state a real leader has
// already moved past.
func TestPartitionedLeaderCannotServeReads(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	leader := c.elect()

	if _, _, err := leader.Propose([]byte("before the partition")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	c.advance(10)

	c.isolate(leader.ID())

	req, _, err := leader.ReadIndex()
	if err != nil {
		t.Fatalf("ReadIndex on a leader that does not yet know it is isolated: %v", err)
	}

	// However long it waits, no majority can answer it.
	for range 200 {
		c.advance(1)
		if leader.ReadReady(req) {
			t.Fatalf("a partitioned leader served a linearizable read\n%s", c.dump())
		}
	}

	// Eventually it also learns it is not the leader, at which point the read
	// is definitively dead rather than merely waiting.
	if !leader.ReadExpired(req) && leader.Role() == Leader {
		t.Log("the isolated node still believes it leads, which is expected; " +
			"the read correctly never completed")
	}
}
