package raft

import (
	"errors"
	"fmt"
	"math/rand"
)

// Config describes a node and its cluster.
//
// Timeouts are counted in ticks rather than wall-clock durations. The raft
// package has no clock, so the caller decides what a tick means: a real
// deployment drives Tick from a ticker of, say, 10ms, while a test drives it
// in a loop and advances a whole election in microseconds. Neither the
// algorithm nor its tests change when that choice changes.
type Config struct {
	// ID is this node's identifier. It must not be empty and must not appear
	// in Peers.
	ID NodeID

	// Peers are the other members of the cluster. A single-node cluster has
	// none, which is legal: such a node elects itself immediately.
	Peers []NodeID

	// ElectionTimeoutMin and ElectionTimeoutMax bound the randomized election
	// timeout, in ticks. Each node picks a fresh value in [min, max) whenever
	// its election timer resets.
	//
	// The randomization is load-bearing, not a tuning detail. With a fixed
	// timeout, nodes that time out together campaign together, split the vote
	// so nobody reaches a majority, then time out together again. That cycle
	// can repeat indefinitely. Randomizing means one node almost always times
	// out first and wins before the others start (Section 5.2).
	ElectionTimeoutMin int
	ElectionTimeoutMax int

	// HeartbeatInterval is how many ticks a leader waits between heartbeats.
	//
	// It must be comfortably below ElectionTimeoutMin. If a heartbeat cannot
	// reliably arrive within one election timeout, followers depose a healthy
	// leader and the cluster churns through elections instead of doing work.
	HeartbeatInterval int

	// Rand supplies the election timeout randomization. Tests pass a seeded
	// source to make a run reproducible; production leaves it nil and gets a
	// randomly seeded one.
	Rand *rand.Rand

	// Storage durably records term, vote, and log. A nil Storage gets an
	// in-memory one, which is correct while the process runs and loses
	// everything when it does not.
	Storage Storage
}

// ErrInvalidConfig reports a configuration that cannot produce a correct node.
var ErrInvalidConfig = errors.New("raft: invalid config")

// validate rejects configurations that would break the algorithm rather than
// merely perform poorly, so a misconfiguration fails at startup instead of
// manifesting later as an unexplained election storm.
func (c Config) validate() error {
	switch {
	case c.ID == "":
		return fmt.Errorf("%w: ID must not be empty", ErrInvalidConfig)
	case c.ElectionTimeoutMin <= 0:
		return fmt.Errorf("%w: ElectionTimeoutMin must be positive, got %d",
			ErrInvalidConfig, c.ElectionTimeoutMin)
	case c.ElectionTimeoutMax < c.ElectionTimeoutMin:
		return fmt.Errorf("%w: ElectionTimeoutMax (%d) must be >= ElectionTimeoutMin (%d)",
			ErrInvalidConfig, c.ElectionTimeoutMax, c.ElectionTimeoutMin)
	case c.HeartbeatInterval <= 0:
		return fmt.Errorf("%w: HeartbeatInterval must be positive, got %d",
			ErrInvalidConfig, c.HeartbeatInterval)
	case c.HeartbeatInterval >= c.ElectionTimeoutMin:
		return fmt.Errorf("%w: HeartbeatInterval (%d) must be < ElectionTimeoutMin (%d), "+
			"otherwise followers time out before a heartbeat can reach them",
			ErrInvalidConfig, c.HeartbeatInterval, c.ElectionTimeoutMin)
	}

	for _, p := range c.Peers {
		if p == c.ID {
			return fmt.Errorf("%w: Peers must not contain this node's own ID %q",
				ErrInvalidConfig, c.ID)
		}
	}
	return nil
}

// Node is a single Raft participant, implemented as a pure state machine.
//
// A Node is not safe for concurrent use. It expects to be driven by exactly
// one goroutine calling Tick and Step. That is a deliberate simplification:
// serializing access removes the need for locks inside the algorithm, where a
// missed lock would be a correctness bug rather than a performance one.
type Node struct {
	id    NodeID
	peers []NodeID

	// Persistent state, which Section 5 requires be written to durable storage
	// before responding to any RPC. Durability arrives in a later milestone;
	// until then a restart loses these and the node may vote twice in one
	// term, which is precisely the bug persistence exists to prevent.
	currentTerm Term
	votedFor    NodeID // empty means no vote cast in currentTerm
	log         []LogEntry

	// Volatile state.
	role     Role
	leaderID NodeID // best known leader for currentTerm, empty if unknown

	// votes accumulates responses during a campaign. It is keyed by voter so
	// that a duplicate response, which a retrying transport can easily
	// produce, cannot be counted twice and manufacture a false majority.
	votes map[NodeID]bool

	// commitIndex is the highest index known to be committed, meaning it is
	// durable across the cluster and safe to apply. lastApplied is how far the
	// caller has actually consumed. The gap between them is the work queue
	// that CommittedEntries drains.
	//
	// Both are volatile. A restarting node relearns its commit index from the
	// leader rather than persisting it, because a committed entry is by
	// definition already on a majority, so the information is recoverable.
	commitIndex Index
	lastApplied Index

	// Leader-only bookkeeping, rebuilt on each election.
	//
	// nextIndex is the leader's guess at where each follower's log ends, and
	// it is only a guess: a new leader optimistically assumes every follower
	// matches its own log, then walks the value back as followers reject.
	// matchIndex is what a follower has actually confirmed, and only it may be
	// used to decide a commit. Conflating the two would let an optimistic
	// guess be mistaken for durability.
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// readSeq counts leadership-confirmation rounds, and ackedRead records
	// how far each follower has echoed. Both are leader-only and rebuilt on
	// election, because a previous leader's confirmations say nothing about
	// this one's.
	readSeq   uint64
	ackedRead map[NodeID]uint64

	electionElapsed  int
	heartbeatElapsed int

	// electionTimeout is re-randomized on every reset, not chosen once at
	// startup. A node that picked an unlucky value would otherwise lose every
	// election it ever contested.
	electionTimeout int

	storage Storage

	// fatal records a failure that makes further participation unsafe, almost
	// always an inability to persist. A node that cannot write its term and
	// vote to disk must stop rather than continue, because continuing means
	// answering RPCs it may forget having answered. Once set, Tick and Step do
	// nothing, so the node simply looks failed to its peers, which is a
	// situation the cluster already knows how to survive.
	fatal error

	cfg  Config
	rand *rand.Rand
}

// NewNode returns a follower at term 0 with an empty log.
//
// Starting as a follower is required rather than conventional: if nodes
// started as candidates they would all campaign on boot, split the vote, and
// delay the first election by at least one timeout.
func NewNode(cfg Config) (*Node, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(rand.Int63()))
	}

	storage := cfg.Storage
	if storage == nil {
		storage = NewMemoryStorage()
	}

	// Recover whatever a previous incarnation of this node persisted. A fresh
	// node gets a zero state and an empty log, so there is no separate
	// first-boot path to get wrong.
	hard, entries, err := storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading persisted state: %w", err)
	}

	n := &Node{
		id:    cfg.ID,
		peers: append([]NodeID(nil), cfg.Peers...), // copy, so the caller cannot mutate membership behind our back
		role:  Follower,
		votes: make(map[NodeID]bool),

		// Restored, not reset. A node that came back believing it had never
		// voted would be free to vote a second time in a term it had already
		// voted in, which is how one term ends up with two leaders.
		currentTerm: hard.Term,
		votedFor:    hard.VotedFor,
		log:         entries,

		storage: storage,
		cfg:     cfg,
		rand:    rng,
	}

	// A restarting node always begins as a follower, whatever it was before.
	// Its peers may have elected someone else while it was gone, and assuming
	// leadership without hearing from them would mean competing with a leader
	// that legitimately holds a later term.
	n.resetElectionTimer()
	return n, nil
}

// Err returns the failure that stopped this node, or nil.
//
// The driver is expected to check it after Tick and Step and shut the node
// down when it is set. There is no recovery: a node that could not persist has
// no way to know what it already promised.
func (n *Node) Err() error { return n.fatal }

// setFatal records the first failure and keeps it.
func (n *Node) setFatal(err error) {
	if n.fatal == nil {
		n.fatal = err
	}
}

// persistHardState writes term and vote through to storage.
//
// It is called before the node acts on either, never after. Answering a
// RequestVote and then persisting the vote would leave a window in which a
// crash loses the record of a vote the peer has already counted.
func (n *Node) persistHardState() {
	if n.fatal != nil {
		return
	}
	err := n.storage.SaveHardState(HardState{Term: n.currentTerm, VotedFor: n.votedFor})
	if err != nil {
		n.setFatal(fmt.Errorf("raft: persisting term %d and vote %q: %w",
			n.currentTerm, n.votedFor, err))
	}
}

// ID returns this node's identifier.
func (n *Node) ID() NodeID { return n.id }

// Role returns the node's current role.
func (n *Node) Role() Role { return n.role }

// Term returns the node's current term.
func (n *Node) Term() Term { return n.currentTerm }

// Leader returns the node's best knowledge of the current leader, or the empty
// ID if none is known. A follower learns this from AppendEntries, so it is
// unset between a leader's failure and the next successful election.
func (n *Node) Leader() NodeID { return n.leaderID }

// VotedFor returns the candidate this node voted for in the current term, or
// the empty ID if it has not voted.
func (n *Node) VotedFor() NodeID { return n.votedFor }

// Tick advances logical time by one unit and returns any messages produced.
//
// A follower or candidate that reaches its election timeout starts a campaign;
// a leader that reaches its heartbeat interval broadcasts. Returning messages
// rather than sending them is what keeps this package free of I/O.
func (n *Node) Tick() []Message {
	if n.fatal != nil {
		return nil
	}

	switch n.role {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatInterval {
			n.heartbeatElapsed = 0
			return n.broadcastAppend()
		}

	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			// A candidate that times out starts a new campaign at a higher
			// term rather than continuing the old one. The previous term's
			// votes are spent, so persisting with it could never reach a
			// majority.
			return n.campaign()
		}
	}
	return nil
}

// Step delivers one inbound message and returns any messages produced.
//
// Messages addressed to another node are ignored rather than treated as an
// error, so a broadcasting transport does not have to filter.
func (n *Node) Step(msg Message) []Message {
	if n.fatal != nil {
		return nil
	}

	h := msg.header()
	if h.To != "" && h.To != n.id {
		return nil
	}

	// Rule 1 of "Rules for Servers" (Figure 2), applied before dispatch and
	// regardless of message type: a higher term always wins, and seeing one
	// means this node's view of the cluster is stale.
	if h.Term > n.currentTerm {
		// A vote is scoped to a term, so moving to a new term must clear it.
		// Carrying votedFor across terms would let one node vote for two
		// different candidates in the same term and elect two leaders.
		leader := NodeID("")
		if ae, ok := msg.(AppendEntries); ok {
			// Only AppendEntries identifies a leader. A RequestVote at a
			// higher term means an election is underway, not decided.
			leader = ae.From
		}
		n.becomeFollower(h.Term, leader)
	}

	// A message from an older term is stale. Requests are answered with this
	// node's term so the sender learns it has fallen behind and steps down;
	// responses are dropped, since replying to a response would loop.
	if h.Term < n.currentTerm {
		switch msg.(type) {
		case RequestVote:
			return []Message{RequestVoteResponse{
				Header:      Header{From: n.id, To: h.From, Term: n.currentTerm},
				VoteGranted: false,
			}}
		case AppendEntries:
			return []Message{AppendEntriesResponse{
				Header:  Header{From: n.id, To: h.From, Term: n.currentTerm},
				Success: false,
			}}
		default:
			return nil
		}
	}

	switch m := msg.(type) {
	case RequestVote:
		return n.handleRequestVote(m)
	case RequestVoteResponse:
		return n.handleRequestVoteResponse(m)
	case AppendEntries:
		return n.handleAppendEntries(m)
	case AppendEntriesResponse:
		return n.handleAppendEntriesResponse(m)
	default:
		return nil
	}
}

// becomeFollower reverts to follower at the given term.
func (n *Node) becomeFollower(term Term, leader NodeID) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.persistHardState()
	}
	n.role = Follower
	n.leaderID = leader
	n.votes = make(map[NodeID]bool)
	n.heartbeatElapsed = 0
	n.resetElectionTimer()
}

// becomeCandidate advances to the next term and votes for itself.
func (n *Node) becomeCandidate() {
	n.currentTerm++
	n.role = Candidate
	n.leaderID = ""
	n.votedFor = n.id
	n.votes = map[NodeID]bool{n.id: true}

	// The new term and the self-vote reach disk before a single RequestVote
	// goes out. A peer that granted a vote in this term must be able to rely
	// on this node remembering it campaigned in it.
	n.persistHardState()

	n.resetElectionTimer()
}

// becomeLeader assumes leadership of the current term.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id

	// Heartbeat immediately rather than after one interval. Until followers
	// hear from the new leader they are still counting down to their own
	// elections, so any delay here invites an unnecessary term change.
	n.heartbeatElapsed = 0

	// Reset per-follower state. nextIndex starts optimistically at the end of
	// this leader's log, and matchIndex starts at zero: nothing is confirmed
	// until a follower says so. Carrying either across terms would treat a
	// previous leader's knowledge as this leader's, and the two logs may
	// differ.
	n.nextIndex = make(map[NodeID]Index, len(n.peers))
	n.matchIndex = make(map[NodeID]Index, len(n.peers))
	n.ackedRead = make(map[NodeID]uint64, len(n.peers))
	n.readSeq = 0
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1
		n.matchIndex[peer] = 0
		n.ackedRead[peer] = 0
	}

	// Append a no-op for this term. See EntryNoOp: without an entry from its
	// own term, a leader may never advance the commit index, leaving entries
	// from the previous term replicated but unapplied.
	n.appendEntry(n.currentTerm, EntryNoOp, nil)

	// A single-node cluster has no followers to confirm anything, so the
	// no-op is committed the moment it is appended.
	n.maybeAdvanceCommit()
}

// resetElectionTimer clears the election countdown and draws a fresh random
// timeout. See Config.ElectionTimeoutMin for why the value is redrawn.
func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0

	spread := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	if spread <= 0 {
		n.electionTimeout = n.cfg.ElectionTimeoutMin
		return
	}
	n.electionTimeout = n.cfg.ElectionTimeoutMin + n.rand.Intn(spread)
}

// CommitIndex returns the highest index known to be committed.
func (n *Node) CommitIndex() Index { return n.commitIndex }

// LastApplied returns how far the caller has consumed committed entries.
func (n *Node) LastApplied() Index { return n.lastApplied }

// ErrNotLeader reports that a write was directed at a node that does not lead.
//
// It is a sentinel so callers can react rather than parse: the HTTP layer maps
// it to a redirect toward Leader(), and a client library would retry there.
var ErrNotLeader = errors.New("raft: not leader")

// Propose submits a command for replication, returning the log index it was
// assigned and the messages the caller must send.
//
// Returning an index rather than waiting for commitment keeps this package
// free of blocking. The caller watches CommittedEntries for that index to
// appear, which is what lets one goroutine drive many concurrent requests.
//
// An index is a promise of position, not of durability. An entry appended by a
// leader that is deposed before replicating it will be overwritten, so a
// caller must wait for the entry to be committed rather than treating a
// successful Propose as success.
func (n *Node) Propose(command []byte) (Index, []Message, error) {
	if n.fatal != nil {
		return 0, nil, n.fatal
	}
	if n.role != Leader {
		return 0, nil, ErrNotLeader
	}

	entry := n.appendEntry(n.currentTerm, EntryNormal, command)

	// A single-node cluster commits immediately; there is nobody to wait for.
	n.maybeAdvanceCommit()

	// Replicate now rather than waiting for the next heartbeat. Otherwise
	// every write would pay up to one heartbeat interval of latency for no
	// reason.
	return entry.Index, n.broadcastAppend(), nil
}

// CommittedEntries returns entries that have been committed but not yet
// returned, advancing lastApplied past them.
//
// Entries are returned in log order and exactly once. The caller must apply
// them in that order: every node applies the same entries in the same sequence
// and must reach identical state, which is the entire purpose of the log.
//
// No-op entries are included rather than filtered. The caller decides what to
// do with them, and hiding them here would make lastApplied disagree with what
// the caller has seen.
func (n *Node) CommittedEntries() []LogEntry {
	if n.lastApplied >= n.commitIndex {
		return nil
	}

	entries := make([]LogEntry, 0, n.commitIndex-n.lastApplied)
	for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
		entry, ok := n.entryAt(i)
		if !ok {
			// Unreachable: commitIndex never exceeds the log. Stopping rather
			// than indexing past the end keeps a bug elsewhere from becoming a
			// panic in the apply path.
			break
		}
		entries = append(entries, entry)
		n.lastApplied = i
	}
	return entries
}

// quorum is the number of votes constituting a majority of the cluster,
// including this node.
//
// It is computed from cluster size rather than from the peer count so that a
// single-node cluster needs one vote, its own. Integer division rounds down,
// so a 3-node cluster needs 2 and a 4-node cluster needs 3: a majority, never
// merely half.
func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}
