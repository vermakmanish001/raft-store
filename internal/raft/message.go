package raft

// Message is any RPC exchanged between nodes. Implementations embed Header,
// which supplies the sealed header method and keeps the set of message types
// closed to this package, so a type switch over them can be exhaustive.
type Message interface {
	header() Header
}

// Header carries the fields every Raft RPC shares.
//
// Term appears on every message, request and response alike, because term
// comparison is the first rule applied to any received message regardless of
// its kind (Figure 2, Rules for Servers): if the message term exceeds the
// local term, the node updates its term and reverts to follower before doing
// anything else. Putting Term anywhere other than a shared header would make
// it possible to write a message type that skips that check.
type Header struct {
	From NodeID
	To   NodeID
	Term Term
}

func (h Header) header() Header { return h }

// Sender returns the node a message came from.
//
// The header method is unexported so that the set of message types stays
// closed to this package. These functions give transports the routing fields
// they need without opening that door, and without adding methods that would
// collide with Header's own field names.
func Sender(m Message) NodeID { return m.header().From }

// Recipient returns the node a message is addressed to.
func Recipient(m Message) NodeID { return m.header().To }

// RequestVote is sent by a candidate to gather votes (Section 5.2).
type RequestVote struct {
	Header

	// LastLogIndex and LastLogTerm describe the candidate's log so voters can
	// apply the election restriction of Section 5.4.1: a voter refuses any
	// candidate whose log is less up to date than its own.
	//
	// This is what guarantees the Leader Completeness Property. A leader must
	// hold every committed entry, and since a committed entry lives on a
	// majority of nodes, and any election also requires a majority, the two
	// sets must intersect. The voter in that intersection rejects a candidate
	// missing the entry, so no such candidate can win.
	LastLogIndex Index
	LastLogTerm  Term
}

// RequestVoteResponse answers a RequestVote.
type RequestVoteResponse struct {
	Header

	// VoteGranted reports whether the sender voted for the candidate. A false
	// value is not merely an absence: it tells the candidate the vote is
	// permanently lost for this term, since a node votes at most once per term.
	VoteGranted bool
}

// AppendEntries replicates log entries and serves as the heartbeat that
// suppresses elections (Section 5.2 and 5.3).
//
// A heartbeat is an AppendEntries carrying no entries. Raft deliberately uses
// one RPC for both jobs rather than a separate heartbeat message, so that
// every heartbeat also carries the consistency check below. A leader therefore
// discovers a divergent follower on the next heartbeat rather than waiting for
// the next client write.
type AppendEntries struct {
	Header

	// PrevLogIndex and PrevLogTerm identify the entry immediately preceding
	// Entries. The receiver refuses the request unless its own log contains a
	// matching entry at that position, which inductively establishes that the
	// two logs agree on everything before it (the Log Matching Property,
	// Section 5.3).
	//
	// Heartbeats carry these fields too, so the check runs continuously.
	PrevLogIndex Index
	PrevLogTerm  Term

	// Entries is empty for a heartbeat. Log replication fills it in a later
	// milestone; the wire format is fixed now so it does not have to change.
	Entries []LogEntry

	// LeaderCommit is the leader's commit index, which lets a follower learn
	// that entries it already holds are now committed without a second round
	// trip.
	LeaderCommit Index

	// ReadSeq is a counter the leader increments for each read barrier. A
	// follower echoes it back untouched.
	//
	// It is what lets a leader prove it still leads before serving a read. A
	// leader partitioned away from its cluster continues to believe it leads
	// until its next election timeout, and would otherwise answer reads from
	// state that the real leader has already moved past. Hearing this counter
	// echoed by a majority establishes that no other leader existed at the
	// moment the read was registered, which is what makes the read
	// linearizable without assuming anything about clock drift.
	ReadSeq uint64
}

// AppendEntriesResponse answers an AppendEntries.
type AppendEntriesResponse struct {
	Header

	// Success reports that the follower's log contained the entry described by
	// PrevLogIndex and PrevLogTerm, and that any entries were appended.
	Success bool

	// MatchIndex is the highest log index the follower is now known to hold.
	// Returning it explicitly lets a leader update its bookkeeping without
	// inferring it from the request it sent, which matters because responses
	// can arrive out of order or after the leader has moved on.
	MatchIndex Index

	// ConflictIndex and ConflictTerm accelerate recovery from a divergent log.
	//
	// The algorithm as described backs nextIndex up one entry per round trip,
	// so a follower that missed a thousand entries needs a thousand
	// exchanges. These fields let a follower describe where its log actually
	// diverges, so the leader can skip an entire term in one step.
	//
	// This is an optimization, not a correctness requirement: the leader
	// treats them as a hint and never advances nextIndex based on them. A
	// follower that returned nonsense would only make recovery slower.
	ConflictIndex Index
	ConflictTerm  Term

	// ReadSeq echoes the value from the request that prompted this response.
	ReadSeq uint64
}

// InstallSnapshot transfers a snapshot to a follower whose log has fallen
// behind what the leader still holds (Section 7).
//
// It exists because compaction makes the ordinary repair path impossible. A
// leader normally walks a lagging follower backward until their logs agree,
// but once entries have been folded into a snapshot the leader no longer has
// them to send, so there is nothing to walk back to. The snapshot replaces
// that prefix wholesale.
type InstallSnapshot struct {
	Header

	// LastIncludedIndex and LastIncludedTerm identify the final entry the
	// snapshot covers. The follower adopts them as its own log boundary, which
	// is what lets it answer a later consistency check about a position whose
	// entry it no longer holds.
	LastIncludedIndex Index
	LastIncludedTerm  Term

	// Data is the serialized state machine.
	//
	// It is sent whole rather than in chunks. The paper describes a chunked
	// transfer with an offset and a done flag, which matters once a snapshot
	// outgrows what fits comfortably in one message; at that point this field
	// becomes a chunk and the receiver assembles them.
	Data []byte
}

// InstallSnapshotResponse acknowledges a snapshot.
type InstallSnapshotResponse struct {
	Header

	// MatchIndex is how far the follower is now known to have replicated,
	// which after a successful install is the snapshot's last included index.
	MatchIndex Index
}

// Compile-time assertions that every message type satisfies Message.
var (
	_ Message = RequestVote{}
	_ Message = RequestVoteResponse{}
	_ Message = AppendEntries{}
	_ Message = AppendEntriesResponse{}
	_ Message = InstallSnapshot{}
	_ Message = InstallSnapshotResponse{}
)
