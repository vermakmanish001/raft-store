package raft

import (
	"errors"
	"math/rand"
	"testing"
)

func nodeWithStorage(t *testing.T, s Storage, peers ...NodeID) *Node {
	t.Helper()

	n, err := NewNode(Config{
		ID:                 "n0",
		Peers:              peers,
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 10,
		HeartbeatInterval:  3,
		Rand:               rand.New(rand.NewSource(1)),
		Storage:            s,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

// TestVoteSurvivesRestart is the reason persistence exists at all.
//
// A node that forgets its vote will grant a second one in the same term. Two
// candidates can then each assemble a majority that includes this node and
// both become leader for that term, after which they can commit conflicting
// entries at the same index. Everything else in this package is built on the
// assumption that cannot happen.
func TestVoteSurvivesRestart(t *testing.T) {
	t.Parallel()

	storage := NewMemoryStorage()

	first := nodeWithStorage(t, storage, "n1", "n2")
	out := first.Step(RequestVote{Header: Header{From: "n1", To: "n0", Term: 1}})
	if !out[0].(RequestVoteResponse).VoteGranted {
		t.Fatal("first vote was not granted")
	}

	// The process dies and comes back, reading the same storage.
	restarted := nodeWithStorage(t, storage, "n1", "n2")

	if restarted.Term() != 1 {
		t.Errorf("term after restart = %d, want 1", restarted.Term())
	}
	if restarted.VotedFor() != "n1" {
		t.Errorf("VotedFor after restart = %q, want %q", restarted.VotedFor(), "n1")
	}

	second := restarted.Step(RequestVote{Header: Header{From: "n2", To: "n0", Term: 1}})
	if second[0].(RequestVoteResponse).VoteGranted {
		t.Error("granted a second vote in term 1 after restarting; two leaders are now possible")
	}
}

func TestLogSurvivesRestart(t *testing.T) {
	t.Parallel()

	storage := NewMemoryStorage()

	first := nodeWithStorage(t, storage, "n1", "n2")
	first.becomeCandidate()
	first.becomeLeader() // appends the term no-op
	if _, _, err := first.Propose([]byte("durable write")); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	wantIndex := first.lastLogIndex()
	wantTerm := first.Term()

	restarted := nodeWithStorage(t, storage, "n1", "n2")

	if got := restarted.lastLogIndex(); got != wantIndex {
		t.Errorf("lastLogIndex after restart = %d, want %d", got, wantIndex)
	}
	if got := restarted.Term(); got != wantTerm {
		t.Errorf("term after restart = %d, want %d", got, wantTerm)
	}

	entry, ok := restarted.entryAt(wantIndex)
	if !ok || string(entry.Command) != "durable write" {
		t.Errorf("entry %d = %+v, want the durable write", wantIndex, entry)
	}
}

// TestRestartedNodeIsAFollower: whatever it was before, a node that comes back
// must not assume leadership. Its peers may have elected someone else while it
// was gone.
func TestRestartedNodeIsAFollower(t *testing.T) {
	t.Parallel()

	storage := NewMemoryStorage()

	first := nodeWithStorage(t, storage, "n1", "n2")
	first.becomeCandidate()
	first.becomeLeader()
	if first.Role() != Leader {
		t.Fatalf("setup failed: role = %s", first.Role())
	}

	restarted := nodeWithStorage(t, storage, "n1", "n2")
	if restarted.Role() != Follower {
		t.Errorf("role after restart = %s, want follower", restarted.Role())
	}
}

// TestTruncationSurvivesRestart: entries discarded because they conflicted
// with a leader must not reappear.
func TestTruncationSurvivesRestart(t *testing.T) {
	t.Parallel()

	storage := NewMemoryStorage()

	first := nodeWithStorage(t, storage, "n1")
	first.currentTerm = 1
	first.Step(AppendEntries{
		Header:  Header{From: "n1", To: "n0", Term: 1},
		Entries: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}},
	})

	// A new leader replaces index 2 onward.
	first.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 2},
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Index: 2, Command: []byte("replacement")}},
	})

	restarted := nodeWithStorage(t, storage, "n1")

	if got := restarted.lastLogIndex(); got != 2 {
		t.Fatalf("lastLogIndex after restart = %d, want 2; discarded entries came back", got)
	}
	entry, _ := restarted.entryAt(2)
	if entry.Term != 2 || string(entry.Command) != "replacement" {
		t.Errorf("entry 2 = %+v, want the replacement", entry)
	}
}

// failingStorage fails every write, standing in for a full or broken disk.
type failingStorage struct {
	MemoryStorage
	fail error
}

func (s *failingStorage) SaveHardState(HardState) error { return s.fail }
func (s *failingStorage) Append([]LogEntry) error       { return s.fail }

// TestNodeStopsWhenItCannotPersist: a node that cannot record its term and
// vote must stop rather than carry on, because carrying on means answering
// RPCs it may not remember having answered. Stopping makes it look failed,
// which the cluster already knows how to survive.
func TestNodeStopsWhenItCannotPersist(t *testing.T) {
	t.Parallel()

	diskFull := errors.New("no space left on device")
	n := nodeWithStorage(t, &failingStorage{fail: diskFull}, "n1", "n2")

	if n.Err() != nil {
		t.Fatalf("Err before any write = %v, want nil", n.Err())
	}

	// Campaigning must persist the new term and self-vote, which fails.
	for range 10 {
		n.Tick()
	}

	if n.Err() == nil {
		t.Fatal("Err = nil after a failed persist; the node carried on unsafely")
	}
	if !errors.Is(n.Err(), diskFull) {
		t.Errorf("Err = %v, want it to wrap %v", n.Err(), diskFull)
	}

	// Once stopped, it does nothing further.
	if out := n.Tick(); out != nil {
		t.Errorf("Tick after failure produced %d messages, want none", len(out))
	}
	if out := n.Step(RequestVote{Header: Header{From: "n1", To: "n0", Term: 99}}); out != nil {
		t.Errorf("Step after failure produced %d messages, want none", len(out))
	}
	if _, _, err := n.Propose([]byte("x")); !errors.Is(err, diskFull) {
		t.Errorf("Propose after failure = %v, want the storage error", err)
	}
}

// TestLoadFailureIsReported: a node whose storage cannot be read must refuse
// to start rather than come up with an empty log and no memory of its votes.
func TestLoadFailureIsReported(t *testing.T) {
	t.Parallel()

	broken := errors.New("disk is on fire")
	_, err := NewNode(Config{
		ID:                 "n0",
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
		Storage:            &unreadableStorage{err: broken},
	})

	if !errors.Is(err, broken) {
		t.Errorf("NewNode error = %v, want it to wrap %v", err, broken)
	}
}

type unreadableStorage struct {
	MemoryStorage
	err error
}

func (s *unreadableStorage) Load() (HardState, []LogEntry, error) {
	return HardState{}, nil, s.err
}

// TestNodeStopsWhenItCannotPersistEntries covers the other half of the
// storage contract. An entry held in memory but not on disk would vanish on
// restart after this node had already told a leader it held it.
func TestNodeStopsWhenItCannotPersistEntries(t *testing.T) {
	t.Parallel()

	diskFull := errors.New("no space left on device")
	storage := &appendFailingStorage{fail: diskFull}

	n := nodeWithStorage(t, storage, "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader() // appends the term no-op, which cannot be persisted

	if n.Err() == nil {
		t.Fatal("Err = nil after a failed append; the node carried on unsafely")
	}
	if !errors.Is(n.Err(), diskFull) {
		t.Errorf("Err = %v, want it to wrap %v", n.Err(), diskFull)
	}
}

type appendFailingStorage struct {
	MemoryStorage
	fail error
}

func (s *appendFailingStorage) Append([]LogEntry) error { return s.fail }

// TestNodeStopsWhenItCannotTruncate: discarding conflicting entries is a
// durable operation too, and a failure leaves the log disagreeing with disk.
func TestNodeStopsWhenItCannotTruncate(t *testing.T) {
	t.Parallel()

	broken := errors.New("device failure")
	storage := &truncateFailingStorage{fail: broken}

	n := nodeWithStorage(t, storage, "n1")
	n.currentTerm = 2
	n.Step(AppendEntries{
		Header:  Header{From: "n1", To: "n0", Term: 2},
		Entries: []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
	})

	// A leader replaces index 2, which forces a truncation.
	n.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 2},
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Index: 2, Command: []byte("replacement")}},
	})

	if !errors.Is(n.Err(), broken) {
		t.Errorf("Err = %v, want it to wrap %v", n.Err(), broken)
	}
}

type truncateFailingStorage struct {
	MemoryStorage
	fail error
}

func (s *truncateFailingStorage) TruncateFrom(Index) error { return s.fail }

// TestCompactStopsTheNodeWhenStorageFails: a snapshot that cannot be persisted
// must not be treated as installed, or the log would be discarded with nothing
// to replace it.
func TestCompactStopsTheNodeWhenStorageFails(t *testing.T) {
	t.Parallel()

	broken := errors.New("device failure")
	storage := &snapshotFailingStorage{fail: broken}

	n := nodeWithStorage(t, storage, "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()
	n.commitIndex = n.lastLogIndex()
	n.CommittedEntries()

	before := n.LogLength()
	err := n.Compact(n.LastApplied(), []byte("snapshot"))

	if !errors.Is(err, broken) {
		t.Errorf("Compact = %v, want it to wrap %v", err, broken)
	}
	if n.LogLength() != before {
		t.Errorf("log length = %d, want %d; entries were discarded despite the snapshot failing",
			n.LogLength(), before)
	}
	if n.SnapshotIndex() != 0 {
		t.Errorf("SnapshotIndex = %d, want 0", n.SnapshotIndex())
	}
}

type snapshotFailingStorage struct {
	MemoryStorage
	fail error
}

func (s *snapshotFailingStorage) SaveSnapshot(SnapshotMeta, []byte) error { return s.fail }
