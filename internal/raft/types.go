// Package raft implements the Raft consensus algorithm as a pure state
// machine: it contains no goroutines, no timers, and no network or disk I/O.
//
// Callers drive it with two methods. Tick advances logical time by one unit,
// and Step delivers one inbound message. Both return the messages the caller
// must send to peers. Nothing inside this package ever blocks, sleeps, or
// touches a socket.
//
// That design is not stylistic. Consensus bugs are overwhelmingly ordering
// bugs, and they surface only under interleavings that are rare on a healthy
// network: a vote arriving after the term moved on, two candidates campaigning
// at once, a leader deposed mid-broadcast. A state machine driven by an
// explicit clock lets a test construct those interleavings exactly and replay
// them identically every run. The same code driven by real timers and real
// sockets can only wait and hope, which produces tests that pass for the wrong
// reasons and fail intermittently in CI.
//
// Section numbers in comments refer to the extended Raft paper, "In Search of
// an Understandable Consensus Algorithm" by Ongaro and Ousterhout.
package raft

import "fmt"

// NodeID uniquely identifies a node within a cluster. It is a string rather
// than an integer so that configurations can use meaningful names, and so that
// a recycled address cannot silently impersonate a previous member.
type NodeID string

// Term is a logical clock. Raft divides time into terms of arbitrary length,
// numbered with consecutive integers, and every RPC carries one.
//
// Terms are the mechanism by which stale leaders are detected: a node that
// sees a term greater than its own immediately reverts to follower, and a node
// that receives a message from an older term rejects it. Comparing terms is
// what makes it safe for a partitioned leader to rejoin without corrupting
// state, because its writes are refused by anyone who has moved on.
type Term uint64

// Index is a position in the replicated log. The log is 1-indexed, so zero is
// reserved to mean "no entry", which lets an empty log report lastIndex 0
// without a special case.
type Index uint64

// Role is the state a node occupies within a term. Every node is exactly one
// of follower, candidate, or leader at any moment (Figure 4).
type Role uint8

const (
	// Follower is passive: it only responds to candidates and leaders. All
	// nodes start here, and any node reverts here on seeing a higher term.
	Follower Role = iota

	// Candidate is campaigning for leadership of a new term.
	Candidate

	// Leader handles all client requests and replicates entries to followers.
	// At most one leader can exist per term, which is the guarantee the
	// election rules exist to provide.
	Leader
)

// String implements fmt.Stringer so log lines and test failures name the role
// rather than printing an opaque integer.
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("Role(%d)", uint8(r))
	}
}

// LogEntry is a single command in the replicated log.
//
// Term is carried on every entry, not just on the log as a whole, because the
// log matching property depends on it: if two logs contain an entry with the
// same index and term, then the logs are identical in all preceding entries
// (Section 5.3). Replication in a later milestone relies on that invariant to
// resolve divergence by comparing a single entry rather than the whole log.
type LogEntry struct {
	Term  Term
	Index Index

	// Command is the opaque payload handed to the state machine once the
	// entry commits. The raft package never interprets it; keeping it opaque
	// is what stops consensus from growing a dependency on the key-value
	// store it happens to replicate.
	Command []byte
}
