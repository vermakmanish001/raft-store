package raft

import (
	"strings"
	"testing"
)

// leaderWithLog returns a leader holding entries 1..n, all applied.
func leaderWithLog(t *testing.T, entries int, peers ...NodeID) *Node {
	t.Helper()

	n := newTestNode(t, "n0", peers...)
	n.becomeCandidate()
	n.becomeLeader() // appends the term no-op at index 1

	for range entries - 1 {
		if _, _, err := n.Propose([]byte("command")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}

	n.commitIndex = n.lastLogIndex()
	n.CommittedEntries() // advance lastApplied
	return n
}

func TestCompactDiscardsEntries(t *testing.T) {
	t.Parallel()

	n := leaderWithLog(t, 10, "n1", "n2")
	if n.LogLength() != 10 {
		t.Fatalf("setup: log length = %d, want 10", n.LogLength())
	}

	if err := n.Compact(6, []byte("snapshot")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if got := n.LogLength(); got != 4 {
		t.Errorf("log length = %d, want 4 entries remaining after compacting to 6", got)
	}
	if got := n.SnapshotIndex(); got != 6 {
		t.Errorf("SnapshotIndex = %d, want 6", got)
	}

	// The end of the log is unchanged: compaction removes a prefix, not a
	// suffix.
	if got := n.lastLogIndex(); got != 10 {
		t.Errorf("lastLogIndex = %d, want 10", got)
	}
}

// TestIndexingSurvivesCompaction is where off-by-one errors live. After a
// snapshot the slice holds a suffix, so every index calculation shifts.
func TestIndexingSurvivesCompaction(t *testing.T) {
	t.Parallel()

	n := leaderWithLog(t, 10, "n1", "n2")
	before := make(map[Index]Term)
	for i := Index(1); i <= 10; i++ {
		before[i] = n.termAt(i)
	}

	if err := n.Compact(6, []byte("snapshot")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Entries after the boundary must still read back identically.
	for i := Index(7); i <= 10; i++ {
		entry, ok := n.entryAt(i)
		if !ok {
			t.Errorf("entryAt(%d) is missing after compaction", i)
			continue
		}
		if entry.Index != i {
			t.Errorf("entryAt(%d) returned index %d", i, entry.Index)
		}
		if got := n.termAt(i); got != before[i] {
			t.Errorf("termAt(%d) = %d, want %d", i, got, before[i])
		}
	}

	// The boundary itself is answered from snapshot metadata.
	if got := n.termAt(6); got != before[6] {
		t.Errorf("termAt(6) = %d, want %d; the boundary must answer from snapshot metadata", got, before[6])
	}

	// Below the boundary the entries are gone.
	if _, ok := n.entryAt(5); ok {
		t.Error("entryAt(5) returned an entry the snapshot swallowed")
	}

	// entriesFrom inside the snapshot signals that a snapshot is needed.
	if got := n.entriesFrom(3); got != nil {
		t.Errorf("entriesFrom(3) returned %d entries, want nil to signal a snapshot is needed", len(got))
	}
	if got := n.entriesFrom(7); len(got) != 4 {
		t.Errorf("entriesFrom(7) returned %d entries, want 4", len(got))
	}
}

func TestLogMatchingAcrossTheSnapshotBoundary(t *testing.T) {
	t.Parallel()

	n := leaderWithLog(t, 10, "n1", "n2")
	boundaryTerm := n.termAt(6)
	if err := n.Compact(6, []byte("snapshot")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	if !n.logMatches(6, boundaryTerm) {
		t.Error("logMatches at the boundary = false; a leader asking about it would resend everything")
	}
	if n.logMatches(6, boundaryTerm+99) {
		t.Error("logMatches at the boundary accepted the wrong term")
	}

	// Below the boundary the entry is gone, but it was committed before being
	// compacted, so no leader can legitimately hold a different one there.
	if !n.logMatches(3, 12345) {
		t.Error("logMatches below the boundary = false; compacted entries are committed and agreed")
	}
}

func TestCompactRefusesToOutrunTheStateMachine(t *testing.T) {
	t.Parallel()

	n := leaderWithLog(t, 5, "n1", "n2")

	// Propose more without applying them.
	for range 3 {
		if _, _, err := n.Propose([]byte("uncommitted")); err != nil {
			t.Fatalf("Propose: %v", err)
		}
	}

	err := n.Compact(n.lastLogIndex(), []byte("snapshot"))
	if err == nil {
		t.Fatal("Compact past lastApplied succeeded; those entries' effects would exist nowhere")
	}
	if !strings.Contains(err.Error(), "applied") {
		t.Errorf("Compact error = %v, want it to mention what has been applied", err)
	}
}

func TestCompactIsIdempotent(t *testing.T) {
	t.Parallel()

	n := leaderWithLog(t, 10, "n1", "n2")

	if err := n.Compact(6, []byte("snapshot")); err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	length := n.LogLength()

	// An older or repeated index must not move the log backward.
	if err := n.Compact(6, []byte("again")); err != nil {
		t.Errorf("repeat Compact: %v", err)
	}
	if err := n.Compact(3, []byte("older")); err != nil {
		t.Errorf("older Compact: %v", err)
	}

	if n.LogLength() != length || n.SnapshotIndex() != 6 {
		t.Errorf("log length %d, snapshot index %d; want %d and 6 unchanged",
			n.LogLength(), n.SnapshotIndex(), length)
	}
}

// TestLeaderSendsSnapshotWhenEntriesAreGone: the ordinary repair path walks
// nextIndex backward until the logs agree, which is impossible once the
// entries needed have been compacted away.
func TestLeaderSendsSnapshotWhenEntriesAreGone(t *testing.T) {
	t.Parallel()

	n := leaderWithLog(t, 10, "n1", "n2")
	if err := n.Compact(8, []byte("the state machine")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// A follower that is far behind needs entries the leader no longer holds.
	n.nextIndex["n1"] = 2

	msg := n.appendTo("n1")
	snap, ok := msg.(InstallSnapshot)
	if !ok {
		t.Fatalf("appendTo returned %T, want InstallSnapshot", msg)
	}
	if snap.LastIncludedIndex != 8 {
		t.Errorf("LastIncludedIndex = %d, want 8", snap.LastIncludedIndex)
	}
	if string(snap.Data) != "the state machine" {
		t.Errorf("Data = %q, want the stored snapshot", snap.Data)
	}

	// A follower that is merely a little behind still gets ordinary entries.
	n.nextIndex["n2"] = 9
	if _, ok := n.appendTo("n2").(AppendEntries); !ok {
		t.Errorf("appendTo for a caught-up follower returned %T, want AppendEntries", n.appendTo("n2"))
	}
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.currentTerm = 5

	out := n.Step(InstallSnapshot{
		Header:            Header{From: "n1", To: "n0", Term: 5},
		LastIncludedIndex: 40,
		LastIncludedTerm:  4,
		Data:              []byte("state at index 40"),
	})

	if len(out) != 1 {
		t.Fatalf("got %d messages, want 1", len(out))
	}
	resp, ok := out[0].(InstallSnapshotResponse)
	if !ok {
		t.Fatalf("got %T, want InstallSnapshotResponse", out[0])
	}
	if resp.MatchIndex != 40 {
		t.Errorf("MatchIndex = %d, want 40", resp.MatchIndex)
	}

	if n.SnapshotIndex() != 40 {
		t.Errorf("SnapshotIndex = %d, want 40", n.SnapshotIndex())
	}
	if n.CommitIndex() != 40 || n.LastApplied() != 40 {
		t.Errorf("commit %d, applied %d; want both 40, since a snapshot is committed and applied by construction",
			n.CommitIndex(), n.LastApplied())
	}
	if n.Leader() != "n1" {
		t.Errorf("Leader = %q, want %q", n.Leader(), "n1")
	}

	meta, data := n.TakePendingSnapshot()
	if string(data) != "state at index 40" {
		t.Errorf("pending snapshot = %q, want it handed to the caller", data)
	}
	if meta.Index != 40 {
		t.Errorf("pending meta index = %d, want 40", meta.Index)
	}

	// Handed over exactly once.
	if _, again := n.TakePendingSnapshot(); again != nil {
		t.Error("the snapshot was handed over twice")
	}
}

// TestFollowerKeepsMatchingSuffix: if the follower already holds the entry the
// snapshot ends at, everything after it is still good and need not be refetched.
func TestFollowerKeepsMatchingSuffix(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 3
	n.log = []LogEntry{
		{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 2, Index: 3},
		{Term: 3, Index: 4}, {Term: 3, Index: 5},
	}

	n.Step(InstallSnapshot{
		Header:            Header{From: "n1", To: "n0", Term: 3},
		LastIncludedIndex: 3,
		LastIncludedTerm:  2, // matches this node's entry 3
		Data:              []byte("snapshot"),
	})

	if n.lastLogIndex() != 5 {
		t.Errorf("lastLogIndex = %d, want 5; matching entries after the boundary should be kept",
			n.lastLogIndex())
	}
	if n.LogLength() != 2 {
		t.Errorf("log length = %d, want 2 (indexes 4 and 5)", n.LogLength())
	}
}

// TestFollowerDiscardsDivergentLog: when the logs disagree at the boundary,
// nothing the follower holds can be trusted.
func TestFollowerDiscardsDivergentLog(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 5
	n.log = []LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
	}

	n.Step(InstallSnapshot{
		Header:            Header{From: "n1", To: "n0", Term: 5},
		LastIncludedIndex: 3,
		LastIncludedTerm:  4, // disagrees with this node's entry 3
		Data:              []byte("snapshot"),
	})

	if n.LogLength() != 0 {
		t.Errorf("log length = %d, want 0; a divergent log must be discarded", n.LogLength())
	}
	if n.lastLogIndex() != 3 {
		t.Errorf("lastLogIndex = %d, want 3 from the snapshot boundary", n.lastLogIndex())
	}
}

// TestStaleSnapshotIsIgnored: a slow snapshot can arrive after ordinary
// replication already caught the follower up, and installing it would move the
// node backward.
func TestStaleSnapshotIsIgnored(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 5
	n.log = []LogEntry{{Term: 5, Index: 1}, {Term: 5, Index: 2}, {Term: 5, Index: 3}}
	n.commitIndex = 3
	n.lastApplied = 3

	n.Step(InstallSnapshot{
		Header:            Header{From: "n1", To: "n0", Term: 5},
		LastIncludedIndex: 2, // behind what is already committed
		LastIncludedTerm:  5,
		Data:              []byte("stale"),
	})

	if n.SnapshotIndex() != 0 {
		t.Errorf("SnapshotIndex = %d, want 0; a stale snapshot must not be installed", n.SnapshotIndex())
	}
	if n.CommitIndex() != 3 {
		t.Errorf("CommitIndex = %d, want 3; the node must not move backward", n.CommitIndex())
	}
	if _, data := n.TakePendingSnapshot(); data != nil {
		t.Error("a stale snapshot was handed to the caller")
	}
}

// TestSnapshotSurvivesRestart: a restarting node picks up its log position
// from the snapshot rather than starting over.
func TestSnapshotSurvivesRestart(t *testing.T) {
	t.Parallel()

	storage := NewMemoryStorage()

	first := nodeWithStorage(t, storage, "n1", "n2")
	first.becomeCandidate()
	first.becomeLeader()
	for range 5 {
		first.Propose([]byte("command"))
	}
	first.commitIndex = first.lastLogIndex()
	first.CommittedEntries()

	if err := first.Compact(4, []byte("state at index 4")); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	restarted := nodeWithStorage(t, storage, "n1", "n2")

	if restarted.SnapshotIndex() != 4 {
		t.Errorf("SnapshotIndex after restart = %d, want 4", restarted.SnapshotIndex())
	}
	if restarted.CommitIndex() != 4 || restarted.LastApplied() != 4 {
		t.Errorf("commit %d, applied %d after restart; want both 4",
			restarted.CommitIndex(), restarted.LastApplied())
	}
	if restarted.lastLogIndex() != first.lastLogIndex() {
		t.Errorf("lastLogIndex = %d, want %d", restarted.lastLogIndex(), first.lastLogIndex())
	}

	_, data := restarted.TakePendingSnapshot()
	if string(data) != "state at index 4" {
		t.Errorf("pending snapshot = %q, want the persisted one", data)
	}
}

// TestSnapshotFromAnOlderTermIsRejected guards the stale-leader path.
func TestSnapshotFromAnOlderTermIsRejected(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 9

	out := n.Step(InstallSnapshot{
		Header:            Header{From: "n1", To: "n0", Term: 4},
		LastIncludedIndex: 100,
		Data:              []byte("from a deposed leader"),
	})

	if len(out) != 1 {
		t.Fatalf("got %d messages, want a response telling the sender it is stale", len(out))
	}
	resp := out[0].(InstallSnapshotResponse)
	if resp.Term != 9 {
		t.Errorf("response term = %d, want 9 so the stale leader steps down", resp.Term)
	}
	if n.SnapshotIndex() != 0 {
		t.Errorf("SnapshotIndex = %d, want 0", n.SnapshotIndex())
	}
}
