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

	electionElapsed  int
	heartbeatElapsed int

	// electionTimeout is re-randomized on every reset, not chosen once at
	// startup. A node that picked an unlucky value would otherwise lose every
	// election it ever contested.
	electionTimeout int

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

	n := &Node{
		id:    cfg.ID,
		peers: append([]NodeID(nil), cfg.Peers...), // copy, so the caller cannot mutate membership behind our back
		role:  Follower,
		votes: make(map[NodeID]bool),
		cfg:   cfg,
		rand:  rng,
	}
	n.resetElectionTimer()
	return n, nil
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
	switch n.role {
	case Leader:
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatInterval {
			n.heartbeatElapsed = 0
			return n.broadcastHeartbeat()
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

// lastLogIndex returns the index of the final log entry, or 0 for an empty log.
func (n *Node) lastLogIndex() Index {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// lastLogTerm returns the term of the final log entry, or 0 for an empty log.
func (n *Node) lastLogTerm() Term {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}
