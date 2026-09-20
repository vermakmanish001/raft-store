package raft

import (
	"errors"
	"testing"
)

func TestProposeRejectedByNonLeader(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")

	if _, _, err := n.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Errorf("Propose on a follower: error = %v, want %v", err, ErrNotLeader)
	}

	n.becomeCandidate()
	if _, _, err := n.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Errorf("Propose on a candidate: error = %v, want %v", err, ErrNotLeader)
	}
}

// TestSingleNodeCommitsImmediately: with a quorum of one there is nobody to
// wait for, so an entry is committed as soon as it is appended.
func TestSingleNodeCommitsImmediately(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 1)
	if !c.advanceUntil(50, func() bool { return len(c.leaders()) == 1 }) {
		t.Fatalf("no leader\n%s", c.dump())
	}

	n := c.nodes["n0"]
	index, _, err := n.Propose([]byte("set x=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if n.CommitIndex() < index {
		t.Errorf("commitIndex = %d, want at least %d", n.CommitIndex(), index)
	}

	entries := n.CommittedEntries()
	if len(entries) != 2 {
		t.Fatalf("got %d committed entries, want 2 (the term no-op and the command)", len(entries))
	}
	if entries[0].Type != EntryNoOp {
		t.Errorf("first entry type = %s, want %s", entries[0].Type, EntryNoOp)
	}
	if string(entries[1].Command) != "set x=1" {
		t.Errorf("command = %q, want %q", entries[1].Command, "set x=1")
	}
}

// TestFigure8CommitRule is the most important test in this package.
//
// It covers Section 5.4.2: a leader may not mark an entry from a previous term
// committed merely because it is stored on a majority. Such an entry can still
// be overwritten by a future leader, so committing it can silently destroy
// data that was already applied and acknowledged.
//
// Deleting the term check in maybeAdvanceCommit leaves every other test in
// this package passing and only this one failing.
func TestFigure8CommitRule(t *testing.T) {
	t.Parallel()

	// Five nodes, so a majority is three.
	n := newTestNode(t, "n0", "n1", "n2", "n3", "n4")
	n.currentTerm = 4
	n.role = Leader
	n.log = []LogEntry{
		{Term: 1, Index: 1, Type: EntryNormal},
		{Term: 2, Index: 2, Type: EntryNormal}, // from an earlier term
	}
	n.nextIndex = map[NodeID]Index{"n1": 3, "n2": 3, "n3": 3, "n4": 3}

	// Index 2 is now replicated on this leader plus two followers: a clear
	// majority of five.
	n.matchIndex = map[NodeID]Index{"n1": 2, "n2": 2, "n3": 0, "n4": 0}

	n.maybeAdvanceCommit()

	if n.CommitIndex() != 0 {
		t.Fatalf("commitIndex = %d, want 0: an entry from term 2 must not be committed "+
			"by replica count while the leader serves term 4, because a future leader "+
			"may still legitimately overwrite it", n.CommitIndex())
	}

	// Appending an entry in the leader's own term and replicating it to a
	// majority commits that entry, and carries the earlier one with it.
	index, _, err := n.Propose([]byte("current term write"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.matchIndex["n1"] = index
	n.matchIndex["n2"] = index
	n.maybeAdvanceCommit()

	if n.CommitIndex() != index {
		t.Errorf("commitIndex = %d, want %d once an entry from the current term is replicated",
			n.CommitIndex(), index)
	}

	// The previous term's entry is committed indirectly, which is the only
	// safe route to committing it.
	entries := n.CommittedEntries()
	if len(entries) != 3 {
		t.Fatalf("got %d committed entries, want 3", len(entries))
	}
	if entries[1].Term != 2 {
		t.Errorf("entry 2 term = %d, want 2; the earlier entry should be carried along", entries[1].Term)
	}
}

// TestCommitRequiresMajority guards the other half of the commit rule.
func TestCommitRequiresMajority(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2", "n3", "n4") // quorum 3
	n.becomeCandidate()
	n.becomeLeader()

	index, _, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// Leader plus one follower is two of five, short of a majority.
	n.matchIndex["n1"] = index
	n.maybeAdvanceCommit()
	if n.CommitIndex() >= index {
		t.Errorf("commitIndex = %d, want below %d with only 2 of 5 replicas", n.CommitIndex(), index)
	}

	// A second follower reaches three of five.
	n.matchIndex["n2"] = index
	n.maybeAdvanceCommit()
	if n.CommitIndex() != index {
		t.Errorf("commitIndex = %d, want %d with 3 of 5 replicas", n.CommitIndex(), index)
	}
}

func TestCommittedEntriesDrainedExactlyOnce(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 1)
	c.advanceUntil(50, func() bool { return len(c.leaders()) == 1 })
	n := c.nodes["n0"]

	if _, _, err := n.Propose([]byte("a")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	first := n.CommittedEntries()
	if len(first) == 0 {
		t.Fatal("first drain returned nothing")
	}
	if second := n.CommittedEntries(); len(second) != 0 {
		t.Errorf("second drain returned %d entries, want 0; entries must be delivered once", len(second))
	}

	if _, _, err := n.Propose([]byte("b")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	third := n.CommittedEntries()
	if len(third) != 1 || string(third[0].Command) != "b" {
		t.Errorf("got %d entries, want just the new one", len(third))
	}
}

// TestFollowerTruncatesConflictingEntries covers Section 5.3: an entry that
// conflicts with the leader's, and everything after it, must be discarded.
func TestFollowerTruncatesConflictingEntries(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 3
	n.log = []LogEntry{
		{Term: 1, Index: 1, Type: EntryNormal, Command: []byte("keep")},
		{Term: 2, Index: 2, Type: EntryNormal, Command: []byte("conflict")},
		{Term: 2, Index: 3, Type: EntryNormal, Command: []byte("also discarded")},
	}

	// The leader's index 2 belongs to term 3, so the local index 2 and
	// everything after it must go.
	out := n.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 3},
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			{Term: 3, Index: 2, Type: EntryNormal, Command: []byte("leader entry")},
		},
	})

	if !out[0].(AppendEntriesResponse).Success {
		t.Fatal("append rejected, want accepted")
	}
	if got := n.lastLogIndex(); got != 2 {
		t.Errorf("lastLogIndex = %d, want 2; the conflicting suffix should be gone", got)
	}

	entry, _ := n.entryAt(2)
	if string(entry.Command) != "leader entry" {
		t.Errorf("entry 2 = %q, want the leader's", entry.Command)
	}

	kept, _ := n.entryAt(1)
	if string(kept.Command) != "keep" {
		t.Errorf("entry 1 = %q, want it untouched", kept.Command)
	}
}

// TestDuplicateAppendIsIdempotent: a retransmitted request must not truncate
// entries the follower already holds, which would discard committed data.
func TestDuplicateAppendIsIdempotent(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 2

	req := AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 2},
		PrevLogIndex: 0,
		Entries: []LogEntry{
			{Term: 2, Index: 1, Type: EntryNormal, Command: []byte("a")},
			{Term: 2, Index: 2, Type: EntryNormal, Command: []byte("b")},
		},
		LeaderCommit: 2,
	}

	n.Step(req)
	afterFirst := n.lastLogIndex()

	n.Step(req)
	n.Step(req)

	if n.lastLogIndex() != afterFirst {
		t.Errorf("lastLogIndex = %d after replays, want %d", n.lastLogIndex(), afterFirst)
	}
	if got := len(n.log); got != 2 {
		t.Errorf("log length = %d, want 2; replays must not duplicate entries", got)
	}
}

// TestCommitIndexBoundedByDeliveredEntries: a follower must not adopt a commit
// index past what it actually holds, or it would hand absent entries to the
// state machine.
func TestCommitIndexBoundedByDeliveredEntries(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 2

	n.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 2},
		PrevLogIndex: 0,
		Entries: []LogEntry{
			{Term: 2, Index: 1, Type: EntryNormal},
		},
		LeaderCommit: 99, // leader is far ahead
	})

	if n.CommitIndex() != 1 {
		t.Errorf("commitIndex = %d, want 1: bounded by the entries actually delivered", n.CommitIndex())
	}
	if entries := n.CommittedEntries(); len(entries) != 1 {
		t.Errorf("got %d committed entries, want 1", len(entries))
	}
}

// TestOutOfOrderResponseDoesNotRetractProgress: a delayed response carries a
// stale MatchIndex, which must not undo confirmed replication.
func TestOutOfOrderResponseDoesNotRetractProgress(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()
	n.Propose([]byte("a"))
	n.Propose([]byte("b"))

	n.Step(AppendEntriesResponse{
		Header: Header{From: "n1", To: "n0", Term: n.Term()}, Success: true, MatchIndex: 3,
	})
	if n.matchIndex["n1"] != 3 {
		t.Fatalf("matchIndex = %d, want 3", n.matchIndex["n1"])
	}

	// An older response arrives late.
	n.Step(AppendEntriesResponse{
		Header: Header{From: "n1", To: "n0", Term: n.Term()}, Success: true, MatchIndex: 1,
	})
	if n.matchIndex["n1"] != 3 {
		t.Errorf("matchIndex = %d, want it to stay at 3", n.matchIndex["n1"])
	}
}

// TestConflictHintSkipsWholeTerm checks the backtracking optimization. Without
// it, a follower missing many entries needs one round trip per entry.
func TestConflictHintSkipsWholeTerm(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 5
	n.log = []LogEntry{
		{Term: 4, Index: 1}, {Term: 4, Index: 2}, {Term: 4, Index: 3},
	}

	// The leader believes the follower's entry 3 belongs to term 5.
	out := n.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 5},
		PrevLogIndex: 3,
		PrevLogTerm:  5,
	})

	resp := out[0].(AppendEntriesResponse)
	if resp.Success {
		t.Fatal("append accepted, want rejected on term mismatch")
	}
	if resp.ConflictTerm != 4 {
		t.Errorf("ConflictTerm = %d, want 4", resp.ConflictTerm)
	}
	if resp.ConflictIndex != 1 {
		t.Errorf("ConflictIndex = %d, want 1, the first index of the conflicting term", resp.ConflictIndex)
	}
}

func TestConflictHintForShortLog(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 3
	n.log = []LogEntry{{Term: 1, Index: 1}}

	out := n.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 3},
		PrevLogIndex: 10, // far past the end
		PrevLogTerm:  3,
	})

	resp := out[0].(AppendEntriesResponse)
	if resp.Success {
		t.Fatal("append accepted, want rejected")
	}
	if resp.ConflictTerm != 0 {
		t.Errorf("ConflictTerm = %d, want 0 to signal a short log", resp.ConflictTerm)
	}
	if resp.ConflictIndex != 2 {
		t.Errorf("ConflictIndex = %d, want 2, one past this log's end", resp.ConflictIndex)
	}
}

// leaderNode returns the current leader, failing the test if there is not
// exactly one.
func (c *cluster) leaderNode() *Node {
	c.t.Helper()
	return c.nodes[c.requireSingleLeader()]
}

// elect advances until a single leader exists.
func (c *cluster) elect() *Node {
	c.t.Helper()

	if !c.advanceUntil(200, func() bool { return len(c.leaders()) == 1 }) {
		c.t.Fatalf("no leader elected\n%s", c.dump())
	}
	return c.leaderNode()
}

func TestClusterReplicatesAndCommits(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	leader := c.elect()

	index, _, err := leader.Propose([]byte("set x=1"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	if !c.advanceUntil(50, func() bool { return leader.CommitIndex() >= index }) {
		t.Fatalf("entry %d not committed\n%s", index, c.dump())
	}

	// Every node must hold the entry, with identical contents. The log is the
	// mechanism by which all nodes reach the same state, so divergence here
	// would mean divergent state machines.
	for _, id := range c.ids {
		n := c.nodes[id]
		entry, ok := n.entryAt(index)
		if !ok {
			t.Errorf("%s is missing entry %d\n%s", id, index, c.dump())
			continue
		}
		if string(entry.Command) != "set x=1" {
			t.Errorf("%s entry %d = %q, want %q", id, index, entry.Command, "set x=1")
		}
	}

	// Followers learn of the commit on the next heartbeat.
	if !c.advanceUntil(50, func() bool {
		for _, id := range c.ids {
			if c.nodes[id].CommitIndex() < index {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("commit index did not propagate to all nodes\n%s", c.dump())
	}
}

// TestCommittedEntriesSurviveLeaderFailure is the durability claim that makes
// the system worth building: once an entry is committed, a leader change must
// not lose it.
func TestCommittedEntriesSurviveLeaderFailure(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 5)
	leader := c.elect()

	var lastIndex Index
	for _, cmd := range []string{"a", "b", "c"} {
		index, _, err := leader.Propose([]byte(cmd))
		if err != nil {
			t.Fatalf("Propose(%s): %v", cmd, err)
		}
		lastIndex = index
	}

	if !c.advanceUntil(100, func() bool { return leader.CommitIndex() >= lastIndex }) {
		t.Fatalf("entries not committed\n%s", c.dump())
	}

	c.isolate(leader.ID())

	fresh := c.advanceUntil(300, func() bool {
		for _, id := range c.ids {
			if id != leader.ID() && c.nodes[id].Role() == Leader {
				return true
			}
		}
		return false
	})
	if !fresh {
		t.Fatalf("no new leader after the old one failed\n%s", c.dump())
	}

	// The new leader must hold every committed entry. The election
	// restriction guarantees it: a committed entry is on a majority, an
	// election needs a majority, and the two sets must overlap.
	for _, id := range c.ids {
		if id == leader.ID() || c.nodes[id].Role() != Leader {
			continue
		}

		next := c.nodes[id]
		for i := Index(1); i <= lastIndex; i++ {
			old, _ := leader.entryAt(i)
			got, ok := next.entryAt(i)
			if !ok {
				t.Fatalf("new leader %s lost committed entry %d\n%s", id, i, c.dump())
			}
			if got.Term != old.Term || string(got.Command) != string(old.Command) {
				t.Errorf("new leader %s entry %d = %q (term %d), want %q (term %d)",
					id, i, got.Command, got.Term, old.Command, old.Term)
			}
		}
	}
}

// TestFollowerCatchesUpAfterPartition exercises the backtracking path: a
// follower that missed many entries must be resynchronized.
func TestFollowerCatchesUpAfterPartition(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	leader := c.elect()

	// Isolate a follower, not the leader, so the cluster keeps making
	// progress while one node falls far behind.
	var lagging NodeID
	for _, id := range c.ids {
		if id != leader.ID() {
			lagging = id
			break
		}
	}
	c.isolate(lagging)

	for i := range 25 {
		if _, _, err := leader.Propose([]byte{byte('a' + i%26)}); err != nil {
			t.Fatalf("Propose: %v", err)
		}
		c.advance(2)
	}

	lastIndex := leader.lastLogIndex()
	if c.nodes[lagging].lastLogIndex() >= lastIndex {
		t.Fatal("setup failed: the isolated node did not fall behind")
	}

	c.heal()

	if !c.advanceUntil(300, func() bool {
		return c.nodes[lagging].lastLogIndex() >= lastIndex
	}) {
		t.Fatalf("lagging node did not catch up: at %d, leader at %d\n%s",
			c.nodes[lagging].lastLogIndex(), lastIndex, c.dump())
	}

	// Catching up means matching exactly, not merely reaching the same length.
	for i := Index(1); i <= lastIndex; i++ {
		want, _ := leader.entryAt(i)
		got, ok := c.nodes[lagging].entryAt(i)
		if !ok || got.Term != want.Term || string(got.Command) != string(want.Command) {
			t.Fatalf("entry %d diverged: got %+v, want %+v", i, got, want)
		}
	}
}

// TestAppliedSequencesAgree is the state machine safety property of Section
// 5.4.3: if any node applies an entry at a given index, no other node ever
// applies a different entry at that index.
//
// It is checked by draining committed entries from every node throughout a
// run punctuated by partitions, and comparing what each one saw.
func TestAppliedSequencesAgree(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 5)
	c.elect()

	applied := make(map[NodeID][]LogEntry)

	drain := func() {
		for _, id := range c.ids {
			applied[id] = append(applied[id], c.nodes[id].CommittedEntries()...)
		}
	}

	for round := range 60 {
		if leaders := c.leaders(); len(leaders) == 1 {
			// Propose only to an unpartitioned leader; a partitioned one
			// cannot replicate and its entries will be discarded.
			if !c.isolated[leaders[0]] {
				c.nodes[leaders[0]].Propose([]byte{byte(round)})
			}
		}

		// Partition a minority so progress remains possible.
		c.heal()
		if round%7 == 0 {
			c.isolate(c.ids[round%len(c.ids)])
		}

		c.advance(5)
		drain()
		c.checkNoSplitBrain()
	}

	c.heal()
	c.advance(200)
	drain()

	// Compare every node's applied sequence against every other's. One may be
	// a prefix of another, since nodes apply at different rates, but where
	// both have applied an index the entries must be identical.
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			shared := min(len(applied[a]), len(applied[b]))
			for k := range shared {
				x, y := applied[a][k], applied[b][k]
				if x.Index != y.Index || x.Term != y.Term || string(x.Command) != string(y.Command) {
					t.Fatalf("applied sequences diverge at position %d:\n  %s: %+v\n  %s: %+v",
						k, a, x, b, y)
				}
			}
		}
	}

	// Sanity: the run must have actually applied something, or the comparison
	// above proved nothing.
	total := 0
	for _, entries := range applied {
		total += len(entries)
	}
	if total == 0 {
		t.Fatal("no entries were applied; the test proved nothing")
	}
}

// TestBackoffNextIndex covers each way a leader walks a follower's nextIndex
// backward after a rejection. Getting this wrong costs availability rather
// than safety: replication stalls or crawls, but nothing is lost.
func TestBackoffNextIndex(t *testing.T) {
	t.Parallel()

	// Leader log: terms 1,1,2,2,3 at indexes 1..5.
	leaderLog := []LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2},
		{Term: 2, Index: 3}, {Term: 2, Index: 4},
		{Term: 3, Index: 5},
	}

	tests := []struct {
		name          string
		startNext     Index
		conflictIndex Index
		conflictTerm  Term
		want          Index
	}{
		{
			name:          "short follower log resumes at its end",
			startNext:     6,
			conflictIndex: 3,
			conflictTerm:  0,
			want:          3,
		},
		{
			name:          "conflicting term the leader also has resumes after it",
			startNext:     6,
			conflictIndex: 3,
			conflictTerm:  2,
			want:          5, // leader's last term-2 entry is index 4
		},
		{
			name:          "conflicting term the leader lacks skips the whole term",
			startNext:     6,
			conflictIndex: 2,
			conflictTerm:  9,
			want:          2,
		},
		{
			name:      "no hint falls back to a single decrement",
			startNext: 4,
			want:      3,
		},
		{
			name:      "never backs past the first index",
			startNext: 1,
			want:      1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n := newTestNode(t, "n0", "n1")
			n.role = Leader
			n.currentTerm = 3
			n.log = leaderLog
			n.nextIndex = map[NodeID]Index{"n1": tc.startNext}
			n.matchIndex = map[NodeID]Index{"n1": 0}

			n.backoffNextIndex(AppendEntriesResponse{
				Header:        Header{From: "n1", To: "n0", Term: 3},
				ConflictIndex: tc.conflictIndex,
				ConflictTerm:  tc.conflictTerm,
			})

			if got := n.nextIndex["n1"]; got != tc.want {
				t.Errorf("nextIndex = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestRejectedAppendRetriesImmediately: waiting a full heartbeat interval per
// round of backtracking would make a lagging follower take far too long to
// recover.
func TestRejectedAppendRetriesImmediately(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()
	n.Propose([]byte("a"))

	out := n.Step(AppendEntriesResponse{
		Header:        Header{From: "n1", To: "n0", Term: n.Term()},
		Success:       false,
		ConflictIndex: 1,
	})

	if len(out) != 1 {
		t.Fatalf("got %d messages, want an immediate retry", len(out))
	}
	if _, ok := out[0].(AppendEntries); !ok {
		t.Errorf("got %T, want AppendEntries", out[0])
	}
}

func TestLogHelperEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("entriesFrom past the end returns nothing", func(t *testing.T) {
		t.Parallel()

		n := newTestNode(t, "n0", "n1")
		n.log = []LogEntry{{Term: 1, Index: 1}}

		if got := n.entriesFrom(99); got != nil {
			t.Errorf("entriesFrom(99) = %v, want nil", got)
		}
	})

	t.Run("entriesFrom zero starts at the first entry", func(t *testing.T) {
		t.Parallel()

		n := newTestNode(t, "n0", "n1")
		n.log = []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}

		if got := n.entriesFrom(0); len(got) != 2 {
			t.Errorf("entriesFrom(0) returned %d entries, want 2", len(got))
		}
	})

	t.Run("entriesFrom returns a copy", func(t *testing.T) {
		t.Parallel()

		n := newTestNode(t, "n0", "n1")
		n.log = []LogEntry{{Term: 1, Index: 1, Command: []byte("a")}}

		got := n.entriesFrom(1)
		got[0].Term = 99

		if n.log[0].Term != 1 {
			t.Error("mutating the returned slice changed the log; it must be a copy")
		}
	})

	t.Run("truncateFrom ignores out-of-range indexes", func(t *testing.T) {
		t.Parallel()

		n := newTestNode(t, "n0", "n1")
		n.log = []LogEntry{{Term: 1, Index: 1}}

		n.truncateFrom(0)
		n.truncateFrom(99)

		if len(n.log) != 1 {
			t.Errorf("log length = %d, want 1; out-of-range truncation must be a no-op", len(n.log))
		}
	})

	t.Run("termAt of an absent index is zero", func(t *testing.T) {
		t.Parallel()

		n := newTestNode(t, "n0", "n1")
		if got := n.termAt(5); got != 0 {
			t.Errorf("termAt(5) = %d, want 0", got)
		}
	})
}

func TestLastAppliedTracksDrain(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 1)
	c.elect()
	n := c.nodes["n0"]

	if n.LastApplied() != 0 {
		t.Errorf("LastApplied() = %d, want 0 before draining", n.LastApplied())
	}

	index, _, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	n.CommittedEntries()

	if n.LastApplied() != index {
		t.Errorf("LastApplied() = %d, want %d after draining", n.LastApplied(), index)
	}
}

func TestEntryTypeString(t *testing.T) {
	t.Parallel()

	tests := map[EntryType]string{
		EntryNormal:  "normal",
		EntryNoOp:    "no-op",
		EntryType(7): "EntryType(7)",
	}

	for typ, want := range tests {
		if got := typ.String(); got != want {
			t.Errorf("EntryType(%d).String() = %q, want %q", uint8(typ), got, want)
		}
	}
}
