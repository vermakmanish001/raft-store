package raft

import (
	"errors"
	"math/rand"
	"testing"
)

// newTestNode builds a node with the given peers and a fixed election timeout,
// so tests that drive ticks directly know exactly when a campaign starts.
func newTestNode(t *testing.T, id NodeID, peers ...NodeID) *Node {
	t.Helper()

	n, err := NewNode(Config{
		ID:                 id,
		Peers:              peers,
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 10, // no spread: deterministic for direct tick tests
		HeartbeatInterval:  3,
		Rand:               rand.New(rand.NewSource(1)),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	valid := Config{
		ID:                 "n0",
		Peers:              []NodeID{"n1"},
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
	}

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty ID", func(c *Config) { c.ID = "" }},
		{"zero election timeout", func(c *Config) { c.ElectionTimeoutMin = 0 }},
		{"max below min", func(c *Config) { c.ElectionTimeoutMax = 5 }},
		{"zero heartbeat", func(c *Config) { c.HeartbeatInterval = 0 }},
		{"heartbeat not shorter than election timeout", func(c *Config) { c.HeartbeatInterval = 10 }},
		{"self listed as peer", func(c *Config) { c.Peers = []NodeID{"n0"} }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			cfg.Peers = append([]NodeID(nil), valid.Peers...)
			tc.mutate(&cfg)

			if _, err := NewNode(cfg); !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("NewNode error = %v, want %v", err, ErrInvalidConfig)
			}
		})
	}

	t.Run("valid config is accepted", func(t *testing.T) {
		t.Parallel()

		if _, err := NewNode(valid); err != nil {
			t.Errorf("NewNode error = %v, want nil", err)
		}
	})
}

func TestQuorum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		peers int
		want  int
	}{
		{0, 1}, // single node: itself
		{1, 2}, // two nodes: both, since one is not a majority
		{2, 2}, // three nodes
		{3, 3}, // four nodes: three, never two
		{4, 3}, // five nodes
	}

	for _, tc := range tests {
		peers := make([]NodeID, tc.peers)
		for i := range peers {
			peers[i] = NodeID(string(rune('a' + i)))
		}

		n := newTestNode(t, "self", peers...)
		if got := n.quorum(); got != tc.want {
			t.Errorf("cluster of %d: quorum = %d, want %d", tc.peers+1, got, tc.want)
		}
	}
}

// TestHigherTermCausesStepDown covers rule 1 of Figure 2's Rules for Servers,
// the single mechanism that prevents a stale leader from persisting.
func TestHigherTermCausesStepDown(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()
	n.becomeLeader()

	if n.Role() != Leader {
		t.Fatalf("setup failed: role = %s, want leader", n.Role())
	}

	n.Step(AppendEntries{
		Header: Header{From: "n1", To: "n0", Term: n.Term() + 5},
	})

	if n.Role() != Follower {
		t.Errorf("role = %s, want follower after seeing a higher term", n.Role())
	}
	if n.Term() != 6 {
		t.Errorf("term = %d, want 6 (adopted from the message)", n.Term())
	}
	if n.Leader() != "n1" {
		t.Errorf("Leader() = %q, want %q; AppendEntries identifies its sender as leader", n.Leader(), "n1")
	}
	if n.VotedFor() != "" {
		t.Errorf("VotedFor() = %q, want empty; a new term must clear the previous vote", n.VotedFor())
	}
}

// TestStaleTermRequestRejected verifies that an old leader's requests are
// refused and that the reply carries the current term, which is how the stale
// sender discovers it must step down.
func TestStaleTermRequestRejected(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.currentTerm = 5

	t.Run("stale AppendEntries", func(t *testing.T) {
		out := n.Step(AppendEntries{Header: Header{From: "n1", To: "n0", Term: 3}})
		if len(out) != 1 {
			t.Fatalf("got %d messages, want 1", len(out))
		}

		resp, ok := out[0].(AppendEntriesResponse)
		if !ok {
			t.Fatalf("got %T, want AppendEntriesResponse", out[0])
		}
		if resp.Success {
			t.Error("Success = true, want false for a stale term")
		}
		if resp.Term != 5 {
			t.Errorf("response term = %d, want 5 so the sender learns it is behind", resp.Term)
		}
	})

	t.Run("stale RequestVote", func(t *testing.T) {
		out := n.Step(RequestVote{Header: Header{From: "n1", To: "n0", Term: 3}})
		if len(out) != 1 {
			t.Fatalf("got %d messages, want 1", len(out))
		}

		resp, ok := out[0].(RequestVoteResponse)
		if !ok {
			t.Fatalf("got %T, want RequestVoteResponse", out[0])
		}
		if resp.VoteGranted {
			t.Error("VoteGranted = true, want false for a stale term")
		}
	})
}

// TestVoteGrantedOncePerTerm is the invariant that makes two leaders in one
// term impossible.
func TestVoteGrantedOncePerTerm(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")

	first := n.Step(RequestVote{Header: Header{From: "n1", To: "n0", Term: 1}})
	if !first[0].(RequestVoteResponse).VoteGranted {
		t.Fatal("first vote was not granted")
	}

	second := n.Step(RequestVote{Header: Header{From: "n2", To: "n0", Term: 1}})
	if second[0].(RequestVoteResponse).VoteGranted {
		t.Error("second candidate in the same term was granted a vote; two leaders become possible")
	}

	// A new term releases the vote.
	third := n.Step(RequestVote{Header: Header{From: "n2", To: "n0", Term: 2}})
	if !third[0].(RequestVoteResponse).VoteGranted {
		t.Error("vote not granted in a new term; votedFor must be cleared on term change")
	}
}

// TestVoteRegrantedToSameCandidate covers a lost response. The candidate
// retries, and refusing the retry would strand an election it had already won.
func TestVoteRegrantedToSameCandidate(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	req := RequestVote{Header: Header{From: "n1", To: "n0", Term: 1}}

	if !n.Step(req)[0].(RequestVoteResponse).VoteGranted {
		t.Fatal("first vote was not granted")
	}
	if !n.Step(req)[0].(RequestVoteResponse).VoteGranted {
		t.Error("retry from the same candidate was refused; a lost response would deadlock the election")
	}
}

// TestElectionRestriction covers Section 5.4.1: a candidate whose log is less
// up to date than the voter's must be refused. This is what guarantees a
// leader holds every committed entry.
func TestElectionRestriction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		voterLog     []LogEntry
		lastLogTerm  Term
		lastLogIndex Index
		want         bool
	}{
		{
			name:     "empty voter log accepts anything",
			voterLog: nil,
			want:     true,
		},
		{
			name:         "candidate with higher last term wins regardless of length",
			voterLog:     []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}},
			lastLogTerm:  2,
			lastLogIndex: 1,
			want:         true,
		},
		{
			name:         "candidate with lower last term is refused even if longer",
			voterLog:     []LogEntry{{Term: 2, Index: 1}},
			lastLogTerm:  1,
			lastLogIndex: 99,
			want:         false,
		},
		{
			name:         "same term, longer log wins",
			voterLog:     []LogEntry{{Term: 1, Index: 1}},
			lastLogTerm:  1,
			lastLogIndex: 2,
			want:         true,
		},
		{
			name:         "same term, equal length is acceptable",
			voterLog:     []LogEntry{{Term: 1, Index: 1}},
			lastLogTerm:  1,
			lastLogIndex: 1,
			want:         true,
		},
		{
			name:         "same term, shorter log is refused",
			voterLog:     []LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}},
			lastLogTerm:  1,
			lastLogIndex: 1,
			want:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n := newTestNode(t, "n0", "n1")
			n.log = tc.voterLog

			out := n.Step(RequestVote{
				Header:       Header{From: "n1", To: "n0", Term: 1},
				LastLogTerm:  tc.lastLogTerm,
				LastLogIndex: tc.lastLogIndex,
			})

			if got := out[0].(RequestVoteResponse).VoteGranted; got != tc.want {
				t.Errorf("VoteGranted = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCandidateStepsDownOnAppendEntries: receiving AppendEntries at the
// current term proves someone else won, so the campaign must be abandoned.
func TestCandidateStepsDownOnAppendEntries(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.becomeCandidate()

	if n.Role() != Candidate {
		t.Fatalf("setup failed: role = %s", n.Role())
	}

	n.Step(AppendEntries{Header: Header{From: "n1", To: "n0", Term: n.Term()}})

	if n.Role() != Follower {
		t.Errorf("role = %s, want follower once a leader is heard from", n.Role())
	}
	if n.Leader() != "n1" {
		t.Errorf("Leader() = %q, want %q", n.Leader(), "n1")
	}
}

// TestDuplicateVoteResponseNotCountedTwice: a retrying transport can deliver
// the same response twice, which must not manufacture a majority.
func TestDuplicateVoteResponseNotCountedTwice(t *testing.T) {
	t.Parallel()

	// Five nodes need three votes. Self plus one distinct peer is two.
	n := newTestNode(t, "n0", "n1", "n2", "n3", "n4")
	n.becomeCandidate()

	grant := RequestVoteResponse{
		Header:      Header{From: "n1", To: "n0", Term: n.Term()},
		VoteGranted: true,
	}
	n.Step(grant)
	n.Step(grant)
	n.Step(grant)

	if n.Role() == Leader {
		t.Error("became leader on repeated votes from one peer; votes must be counted per voter")
	}

	// A genuine second voter completes the majority.
	n.Step(RequestVoteResponse{
		Header:      Header{From: "n2", To: "n0", Term: n.Term()},
		VoteGranted: true,
	})
	if n.Role() != Leader {
		t.Errorf("role = %s, want leader after 3 of 5 votes", n.Role())
	}
}

// TestVoteResponseIgnoredWhenNotCandidate: a response arriving after the node
// has already won or stepped down belongs to a settled election.
func TestVoteResponseIgnoredWhenNotCandidate(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.currentTerm = 3 // follower at term 3, not campaigning

	out := n.Step(RequestVoteResponse{
		Header:      Header{From: "n1", To: "n0", Term: 3},
		VoteGranted: true,
	})

	if len(out) != 0 {
		t.Errorf("got %d messages, want none", len(out))
	}
	if n.Role() != Follower {
		t.Errorf("role = %s, want follower; a stray vote must not promote a non-candidate", n.Role())
	}
}

// TestMessageForAnotherNodeIgnored lets a broadcasting transport stay simple.
func TestMessageForAnotherNodeIgnored(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	before := n.Term()

	out := n.Step(AppendEntries{Header: Header{From: "n1", To: "n9", Term: 99}})

	if len(out) != 0 {
		t.Errorf("got %d messages, want none", len(out))
	}
	if n.Term() != before {
		t.Errorf("term = %d, want %d; a misaddressed message must not change state", n.Term(), before)
	}
}

// TestFollowerCampaignsAfterElectionTimeout pins the tick accounting.
func TestFollowerCampaignsAfterElectionTimeout(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2") // fixed timeout of 10

	for i := range 9 {
		if out := n.Tick(); len(out) != 0 {
			t.Fatalf("tick %d produced %d messages, want none before the timeout", i+1, len(out))
		}
		if n.Role() != Follower {
			t.Fatalf("tick %d: role = %s, want follower", i+1, n.Role())
		}
	}

	out := n.Tick() // the tenth tick reaches the timeout

	if n.Role() != Candidate {
		t.Errorf("role = %s, want candidate", n.Role())
	}
	if n.Term() != 1 {
		t.Errorf("term = %d, want 1", n.Term())
	}
	if n.VotedFor() != "n0" {
		t.Errorf("VotedFor() = %q, want self", n.VotedFor())
	}
	if len(out) != 2 {
		t.Errorf("got %d RequestVote messages, want 2 (one per peer)", len(out))
	}
}

// TestLeaderSendsHeartbeatsOnInterval pins the heartbeat cadence, and that a
// heartbeat to a caught-up follower carries no entries.
func TestLeaderSendsHeartbeatsOnInterval(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2") // heartbeat interval 3
	n.becomeCandidate()
	n.becomeLeader()

	// A new leader appends a no-op for its term, so its first append carries
	// that entry rather than being empty. Acknowledge it on both followers'
	// behalf so they count as caught up.
	noop := n.lastLogIndex()
	for _, peer := range []NodeID{"n1", "n2"} {
		n.Step(AppendEntriesResponse{
			Header:     Header{From: peer, To: "n0", Term: n.Term()},
			Success:    true,
			MatchIndex: noop,
		})
	}

	for i := range 2 {
		if out := n.Tick(); len(out) != 0 {
			t.Fatalf("tick %d produced %d messages, want none before the interval", i+1, len(out))
		}
	}

	out := n.Tick()
	if len(out) != 2 {
		t.Fatalf("got %d messages on the interval, want 2", len(out))
	}
	for _, m := range out {
		ae, ok := m.(AppendEntries)
		if !ok {
			t.Fatalf("got %T, want AppendEntries", m)
		}
		if len(ae.Entries) != 0 {
			t.Errorf("heartbeat to a caught-up follower carried %d entries, want 0", len(ae.Entries))
		}
		if ae.PrevLogIndex != noop {
			t.Errorf("PrevLogIndex = %d, want %d", ae.PrevLogIndex, noop)
		}
	}
}

// TestNewLeaderAppendsNoOp covers the entry a leader adds for its own term.
// See EntryNoOp: without it the commit index can never advance in an idle
// cluster, stranding entries from the previous term.
func TestNewLeaderAppendsNoOp(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1", "n2")
	n.log = []LogEntry{{Term: 1, Index: 1, Type: EntryNormal, Command: []byte("old")}}
	n.currentTerm = 1

	n.becomeCandidate() // term 2
	n.becomeLeader()

	last, ok := n.entryAt(n.lastLogIndex())
	if !ok {
		t.Fatal("log is empty after becoming leader")
	}
	if last.Type != EntryNoOp {
		t.Errorf("last entry type = %s, want %s", last.Type, EntryNoOp)
	}
	if last.Term != n.Term() {
		t.Errorf("no-op term = %d, want the leader's own term %d", last.Term, n.Term())
	}
	if last.Index != 2 {
		t.Errorf("no-op index = %d, want 2", last.Index)
	}

	// nextIndex is initialized before the no-op is appended, so it points at
	// the no-op itself. That is what makes the first append carry the entry
	// rather than waiting for a rejection to discover the follower needs it.
	for _, peer := range []NodeID{"n1", "n2"} {
		if got := n.nextIndex[peer]; got != 2 {
			t.Errorf("nextIndex[%s] = %d, want 2 (the no-op's own index)", peer, got)
		}
		if got := n.matchIndex[peer]; got != 0 {
			t.Errorf("matchIndex[%s] = %d, want 0; nothing is confirmed yet", peer, got)
		}
	}

	// Confirm the first append actually carries it.
	out := n.appendTo("n1")
	if len(out.Entries) != 1 || out.Entries[0].Type != EntryNoOp {
		t.Errorf("first append carried %d entries, want the no-op", len(out.Entries))
	}
}

// TestLogMatching covers the consistency check that a heartbeat performs on
// every exchange, and that log replication will depend on in full.
func TestLogMatching(t *testing.T) {
	t.Parallel()

	log := []LogEntry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 3, Index: 3},
	}

	tests := []struct {
		name  string
		index Index
		term  Term
		want  bool
	}{
		{"index 0 always matches", 0, 0, true},
		{"index 0 matches whatever term is claimed", 0, 99, true},
		{"matching entry", 2, 1, true},
		{"matching entry at the end", 3, 3, true},
		{"same index, wrong term", 3, 2, false},
		{"index beyond the end of the log", 4, 3, false},
		{"index far beyond the end", 999, 1, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n := newTestNode(t, "n0", "n1")
			n.log = log

			if got := n.logMatches(tc.index, tc.term); got != tc.want {
				t.Errorf("logMatches(%d, %d) = %v, want %v", tc.index, tc.term, got, tc.want)
			}
		})
	}
}

// TestAppendEntriesRejectedOnLogMismatch: a follower whose log diverges from
// what the leader describes must refuse, which is how the leader discovers it
// needs to back up and resynchronize.
func TestAppendEntriesRejectedOnLogMismatch(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	n.log = []LogEntry{{Term: 1, Index: 1}}
	n.currentTerm = 2

	out := n.Step(AppendEntries{
		Header:       Header{From: "n1", To: "n0", Term: 2},
		PrevLogIndex: 5, // the follower has nothing at index 5
		PrevLogTerm:  2,
	})

	if len(out) != 1 {
		t.Fatalf("got %d messages, want 1", len(out))
	}
	resp := out[0].(AppendEntriesResponse)
	if resp.Success {
		t.Error("Success = true, want false when the log does not match")
	}

	// The election timer still resets and the leader is still recognized. A
	// divergent log means the follower is behind, not that the leader is
	// illegitimate, so campaigning against it would be counterproductive.
	if n.Leader() != "n1" {
		t.Errorf("Leader() = %q, want %q even on a rejected append", n.Leader(), "n1")
	}
	if n.electionElapsed != 0 {
		t.Errorf("electionElapsed = %d, want 0; a rejected append is still contact from a leader", n.electionElapsed)
	}
}

func TestAccessors(t *testing.T) {
	t.Parallel()

	n := newTestNode(t, "n0", "n1")
	if n.ID() != "n0" {
		t.Errorf("ID() = %q, want %q", n.ID(), "n0")
	}
}

func TestRoleString(t *testing.T) {
	t.Parallel()

	tests := map[Role]string{
		Follower:  "follower",
		Candidate: "candidate",
		Leader:    "leader",
		Role(9):   "Role(9)",
	}

	for role, want := range tests {
		if got := role.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", uint8(role), got, want)
		}
	}
}
