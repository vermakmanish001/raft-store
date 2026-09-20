// Package replica runs a Raft-backed replica of the key-value store.
//
// It is the only place where consensus, storage, and the network meet. The
// raft package is a pure state machine with no clock or I/O; this package
// supplies both, owning the goroutine that drives it and the registry that
// connects a client's request to the log entry that satisfies it.
package replica

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/vermakmanish001/raft-store/internal/fsm"
	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// Errors a client can observe.
var (
	// ErrTimeout reports that a write was not committed in time. The entry may
	// still commit afterward, so this is genuinely ambiguous: the client
	// cannot tell a slow success from a failure, and must retry.
	ErrTimeout = errors.New("replica: timed out waiting for commit")

	// ErrShuttingDown reports that the replica stopped before the write
	// resolved.
	ErrShuttingDown = errors.New("replica: shutting down")

	// ErrReadTimeout reports that a node could not confirm it still leads
	// within the timeout, so it refused to answer rather than risk returning
	// state a newer leader has already moved past.
	//
	// It is distinct from ErrTimeout because the situations differ in a way
	// the client cares about. A write timeout is ambiguous: the entry may
	// still commit. A read that could not be confirmed changed nothing at
	// all, so retrying it is always safe.
	ErrReadTimeout = errors.New("replica: could not confirm leadership for a linearizable read")

	// ErrStorageFailed reports that the node could not persist and has
	// stopped participating. There is no recovery in-process: a node that
	// could not write its term and vote has no way to know what it already
	// promised its peers.
	ErrStorageFailed = errors.New("replica: storage failed, node stopped")
)

// Transport delivers Raft messages to peers and surfaces those received.
//
// The interface is declared here, in the consumer, rather than alongside its
// implementation, so that this package can be tested against an in-memory
// transport without importing any networking code.
type Transport interface {
	// Send must not block. Raft tolerates message loss, so a transport that
	// cannot deliver promptly should drop rather than stall the caller.
	Send(raft.Message)

	// Inbound returns messages received from peers.
	Inbound() <-chan raft.Message
}

// Config describes a replica.
type Config struct {
	ID    raft.NodeID
	Peers []raft.NodeID

	// ClientAddrs maps a node ID to the base URL of its client API, used to
	// tell a client where the leader is. Raft itself never needs this; it is
	// purely for redirection.
	ClientAddrs map[raft.NodeID]string

	Transport Transport
	Store     store.Store
	Logger    *slog.Logger

	// TickInterval is how much wall-clock time one logical tick represents.
	// The raft package counts timeouts in ticks and has no clock of its own.
	TickInterval time.Duration

	ElectionTimeoutMin int
	ElectionTimeoutMax int
	HeartbeatInterval  int

	// WriteTimeout bounds how long a client waits for its entry to commit.
	WriteTimeout time.Duration

	// ReadTimeout bounds how long a linearizable read waits for a majority to
	// confirm this node still leads.
	//
	// It is shorter than WriteTimeout by default because the two fail
	// differently. A write that times out may still commit, so waiting longer
	// can turn an ambiguous answer into a definite one. A read that cannot be
	// confirmed will not become confirmable by waiting: either a majority is
	// reachable within about an election timeout or it is not.
	ReadTimeout time.Duration

	// Storage durably records term, vote, and log. A nil Storage gets an
	// in-memory one, so the node participates correctly while it runs and
	// remembers nothing across a restart.
	Storage raft.Storage

	// SnapshotThreshold is how many uncompacted log entries trigger a
	// snapshot. Zero disables compaction, which lets the log grow forever.
	//
	// The value trades startup time against snapshot cost. A low threshold
	// snapshots often, so the log stays short and a restart replays little,
	// but serializing the whole state machine repeatedly is wasted work. A
	// high one does the opposite.
	SnapshotThreshold int
}

// Defaults chosen so that an election completes in well under a second while
// leaving ample room for heartbeats to arrive. With a 50ms tick, a leader
// heartbeats every 100ms and a follower gives up on it after 500ms to 1s.
const (
	defaultTickInterval       = 50 * time.Millisecond
	defaultElectionTimeoutMin = 10
	defaultElectionTimeoutMax = 20
	defaultHeartbeatInterval  = 2
	defaultWriteTimeout       = 5 * time.Second
	defaultReadTimeout        = 2 * time.Second
	defaultSnapshotThreshold  = 1024
)

func (c *Config) applyDefaults() {
	if c.TickInterval <= 0 {
		c.TickInterval = defaultTickInterval
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = defaultElectionTimeoutMin
	}
	if c.ElectionTimeoutMax <= 0 {
		c.ElectionTimeoutMax = defaultElectionTimeoutMax
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = defaultSnapshotThreshold
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Status is a snapshot of a replica's consensus state.
type Status struct {
	ID          raft.NodeID `json:"id"`
	Role        string      `json:"role"`
	Term        uint64      `json:"term"`
	Leader      raft.NodeID `json:"leader,omitempty"`
	LeaderAddr  string      `json:"leader_addr,omitempty"`
	CommitIndex uint64      `json:"commit_index"`
	LastApplied uint64      `json:"last_applied"`

	// SnapshotIndex is the last log index folded into a snapshot, and
	// LogEntries is how many remain uncompacted.
	SnapshotIndex uint64 `json:"snapshot_index"`
	LogEntries    int    `json:"log_entries"`
}

// Request identifies a client operation so a retry can be recognized as one.
//
// A zero Request disables deduplication for that operation, which is the
// at-least-once behavior and is fine for a one-shot command typed at a
// terminal. A client library supplies a stable ClientID and a Seq that
// increments per distinct operation, retrying the same pair until it gets an
// answer.
type Request struct {
	ClientID string
	Seq      uint64
}

// proposal is a client write awaiting commitment.
type proposal struct {
	command []byte

	// result is buffered so the consensus loop can resolve a proposal whose
	// caller has already given up, without blocking.
	result chan error
}

// readBarrier is a client read awaiting leadership confirmation.
type readBarrier struct {
	result chan error
}

// pendingRead links a registered barrier to the client waiting on it.
type pendingRead struct {
	req    raft.ReadRequest
	result chan error
}

// pending links a log index to the client waiting on it.
type pending struct {
	// term is the term the entry was appended in. An entry at a given index
	// can be replaced if this node is deposed before the entry commits, so the
	// term is what distinguishes "the client's entry committed" from "some
	// other leader's entry committed at the same index".
	term   raft.Term
	result chan error
}

// Replica is a Raft-backed store.Store.
//
// It satisfies the same interface as the in-memory store, which is why the
// HTTP layer needs no knowledge that replication exists.
type Replica struct {
	cfg  Config
	node *raft.Node
	fsm  *fsm.FSM

	// store is read directly for Get. Writes go through the log and reach it
	// only by way of the FSM.
	store store.Store

	tr     Transport
	logger *slog.Logger

	proposeC chan proposal
	readC    chan readBarrier
	statusC  chan chan Status
	stopC    chan struct{}
	doneC    chan struct{}

	// pending and pendingReads are owned exclusively by the run loop. Nothing
	// else may touch them.
	pending      map[raft.Index]pending
	pendingReads []pendingRead

	// storageErr is set by the run loop just before it exits, and read only
	// after doneC is closed, which is what makes the handoff safe without a
	// lock.
	storageErr error
}

// Compile-time assertion that a Replica can stand in for a plain store.
var _ store.Store = (*Replica)(nil)

// New builds a replica. Call Start to begin participating in the cluster.
func New(cfg Config) (*Replica, error) {
	cfg.applyDefaults()

	if cfg.Transport == nil {
		return nil, errors.New("replica: Transport is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("replica: Store is required")
	}

	node, err := raft.NewNode(raft.Config{
		ID:                 cfg.ID,
		Peers:              cfg.Peers,
		ElectionTimeoutMin: cfg.ElectionTimeoutMin,
		ElectionTimeoutMax: cfg.ElectionTimeoutMax,
		HeartbeatInterval:  cfg.HeartbeatInterval,
		Storage:            cfg.Storage,
	})
	if err != nil {
		return nil, fmt.Errorf("replica: %w", err)
	}

	machine := fsm.New(cfg.Store)

	// A node that loaded a snapshot at startup hands it over here. Installing
	// it before the loop starts means the state machine and the node's log
	// position agree from the first request onward.
	if _, data := node.TakePendingSnapshot(); data != nil {
		if err := machine.Restore(data); err != nil {
			return nil, fmt.Errorf("replica: restoring snapshot at startup: %w", err)
		}
	}

	return &Replica{
		cfg:      cfg,
		node:     node,
		fsm:      machine,
		store:    cfg.Store,
		tr:       cfg.Transport,
		logger:   cfg.Logger,
		proposeC: make(chan proposal),
		readC:    make(chan readBarrier),
		statusC:  make(chan chan Status),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		pending:  make(map[raft.Index]pending),
	}, nil
}

// Start launches the consensus loop.
func (r *Replica) Start() {
	go r.run()
}

// Close stops the replica and fails any writes still in flight.
func (r *Replica) Close() error {
	select {
	case <-r.doneC:
		return nil // already stopped
	case r.stopC <- struct{}{}:
		<-r.doneC
		return nil
	}
}

// run is the consensus loop. It is the only goroutine permitted to touch
// r.node or r.pending, which is what makes the raft package's lack of internal
// locking safe.
func (r *Replica) run() {
	defer close(r.doneC)

	ticker := time.NewTicker(r.cfg.TickInterval)
	defer ticker.Stop()

	inbound := r.tr.Inbound()

	for {
		select {
		case <-ticker.C:
			r.dispatch(r.node.Tick())

		case msg := <-inbound:
			r.dispatch(r.node.Step(msg))

		case p := <-r.proposeC:
			r.propose(p)

		case b := <-r.readC:
			r.registerRead(b)

		case reply := <-r.statusC:
			reply <- r.status()

		case <-r.stopC:
			r.failPending(ErrShuttingDown)
			return
		}

		// A node that could not persist has stopped participating. Keeping the
		// loop running would leave clients waiting on writes that can never
		// commit, so the replica shuts itself down and reports why.
		if err := r.node.Err(); err != nil {
			r.logger.Error("storage failure, node is stopping",
				slog.String("id", string(r.cfg.ID)),
				slog.Any("error", err),
			)
			r.storageErr = err
			r.failPending(fmt.Errorf("%w: %v", ErrStorageFailed, err))
			return
		}
	}
}

// dispatch sends outbound messages, applies newly committed entries, and
// reconciles pending writes against the node's current role.
func (r *Replica) dispatch(msgs []raft.Message) {
	for _, msg := range msgs {
		r.tr.Send(msg)
	}

	// A snapshot received from the leader replaces the state machine outright,
	// and must be installed before any committed entry is applied on top of
	// it. Applying first would run entries against state the snapshot is about
	// to discard.
	r.installPendingSnapshot()

	r.applyCommitted()
	r.resolveReads()
	r.maybeSnapshot()

	// A node that is no longer leader cannot know whether its uncommitted
	// entries will survive. Failing the clients now is the honest answer: they
	// must retry against the new leader.
	//
	// This makes writes at-least-once rather than exactly-once. A write that
	// commits just as leadership changes is reported as failed, and a client
	// retry applies it twice. Fixing that needs request deduplication in
	// replicated state, which is a later milestone.
	if r.node.Role() != raft.Leader && len(r.pending) > 0 {
		r.failPending(raft.ErrNotLeader)
	}
}

// applyCommitted feeds newly committed entries to the state machine and wakes
// whichever clients were waiting on them.
func (r *Replica) applyCommitted() {
	for _, entry := range r.node.CommittedEntries() {
		err := r.fsm.Apply(entry)
		if err != nil && !isClientError(err) {
			// A decode failure or unknown operation means this node cannot
			// reproduce the agreed-upon state. Continuing would let it drift
			// silently from its peers, so it is logged loudly.
			r.logger.Error("applying committed entry failed",
				slog.Uint64("index", uint64(entry.Index)),
				slog.Any("error", err),
			)
		}
		r.resolve(entry, err)
	}
}

// installPendingSnapshot replaces the state machine with a snapshot the node
// accepted from a leader.
func (r *Replica) installPendingSnapshot() {
	meta, data := r.node.TakePendingSnapshot()
	if data == nil {
		return
	}

	if err := r.fsm.Restore(data); err != nil {
		// The node's log position now claims state the machine does not hold.
		// Continuing would answer reads from the wrong data, so the replica
		// stops instead.
		r.logger.Error("restoring a received snapshot failed, node is stopping",
			slog.Uint64("index", uint64(meta.Index)),
			slog.Any("error", err),
		)
		r.storageErr = err
		return
	}

	// Writes that were waiting cannot be resolved from a snapshot: it says
	// what the state is, not which proposals produced it. They are failed so
	// the clients retry rather than wait forever.
	if len(r.pending) > 0 {
		r.failPending(raft.ErrNotLeader)
	}

	r.logger.Info("installed snapshot from leader",
		slog.Uint64("index", uint64(meta.Index)),
		slog.Uint64("term", uint64(meta.Term)),
	)
}

// maybeSnapshot compacts the log once it has grown past the threshold.
func (r *Replica) maybeSnapshot() {
	if r.cfg.SnapshotThreshold <= 0 || r.node.LogLength() < r.cfg.SnapshotThreshold {
		return
	}

	applied := r.node.LastApplied()
	if applied <= r.node.SnapshotIndex() {
		return // nothing new has been applied since the last snapshot
	}

	// The snapshot is taken at lastApplied rather than the commit index. An
	// entry that is committed but not yet applied is not reflected in the
	// state machine, so a snapshot claiming to cover it would silently lose
	// its effect.
	data, err := r.fsm.Snapshot()
	if err != nil {
		r.logger.Error("taking a snapshot failed, log compaction skipped",
			slog.Any("error", err))

		// Disable further attempts rather than retrying on every apply, which
		// would turn one failure into a flood of identical log lines.
		r.cfg.SnapshotThreshold = 0
		return
	}

	if err := r.node.Compact(applied, data); err != nil {
		r.logger.Error("compacting the log failed",
			slog.Uint64("index", uint64(applied)),
			slog.Any("error", err))
		return
	}

	r.logger.Info("compacted log",
		slog.Uint64("index", uint64(applied)),
		slog.Int("bytes", len(data)),
		slog.Int("entries_remaining", r.node.LogLength()),
	)
}

// resolve completes the client write waiting on an entry, if any.
func (r *Replica) resolve(entry raft.LogEntry, applyErr error) {
	waiting, ok := r.pending[entry.Index]
	if !ok {
		return
	}
	delete(r.pending, entry.Index)

	// The index committed, but with a different entry than this client's. This
	// node was deposed and the new leader's entry took that slot, so the
	// client's write was never committed.
	if waiting.term != entry.Term {
		waiting.result <- raft.ErrNotLeader
		return
	}

	waiting.result <- applyErr
}

// propose appends a client command to the log and records who is waiting.
func (r *Replica) propose(p proposal) {
	index, msgs, err := r.node.Propose(p.command)
	if err != nil {
		p.result <- err
		return
	}

	r.pending[index] = pending{term: r.node.Term(), result: p.result}

	for _, msg := range msgs {
		r.tr.Send(msg)
	}

	// A single-node cluster commits on append, so the entry may already be
	// ready to apply.
	r.applyCommitted()

	// Checked here as well as on each tick, because the log grows on append.
	// Relying on ticks alone would let a burst of writes run between two of
	// them and leave the log arbitrarily long in the meantime.
	r.maybeSnapshot()
}

// registerRead opens a read barrier and records the client waiting on it.
func (r *Replica) registerRead(b readBarrier) {
	req, msgs, err := r.node.ReadIndex()
	if err != nil {
		b.result <- err
		return
	}

	r.pendingReads = append(r.pendingReads, pendingRead{req: req, result: b.result})

	for _, msg := range msgs {
		r.tr.Send(msg)
	}

	// A single-node cluster confirms itself, so the barrier may already be
	// satisfied and need not wait for a tick.
	r.resolveReads()
}

// resolveReads releases reads whose barrier is satisfied and fails those that
// can no longer be satisfied.
func (r *Replica) resolveReads() {
	if len(r.pendingReads) == 0 {
		return
	}

	kept := r.pendingReads[:0]
	for _, read := range r.pendingReads {
		switch {
		case r.node.ReadReady(read.req):
			read.result <- nil

		case r.node.ReadExpired(read.req):
			// Leadership was lost. Confirmation under the old term proves
			// nothing, so the client is told to find the new leader rather
			// than waiting for a timeout it can do nothing about.
			read.result <- raft.ErrNotLeader

		default:
			kept = append(kept, read)
		}
	}
	r.pendingReads = kept
}

// failPending completes every waiting write and read with the same error.
func (r *Replica) failPending(err error) {
	for index, waiting := range r.pending {
		waiting.result <- err
		delete(r.pending, index)
	}
	for _, read := range r.pendingReads {
		read.result <- err
	}
	r.pendingReads = nil
}

// status snapshots consensus state. Called only from the run loop.
func (r *Replica) status() Status {
	leader := r.node.Leader()

	return Status{
		ID:          r.node.ID(),
		Role:        r.node.Role().String(),
		Term:        uint64(r.node.Term()),
		Leader:      leader,
		LeaderAddr:  r.cfg.ClientAddrs[leader],
		CommitIndex: uint64(r.node.CommitIndex()),
		LastApplied: uint64(r.node.LastApplied()),

		SnapshotIndex: uint64(r.node.SnapshotIndex()),
		LogEntries:    r.node.LogLength(),
	}
}

// Status returns a snapshot of this replica's consensus state.
func (r *Replica) Status() Status {
	reply := make(chan Status, 1)

	select {
	case r.statusC <- reply:
		return <-reply
	case <-r.doneC:
		return Status{ID: r.cfg.ID, Role: "stopped"}
	}
}

// Err returns the storage failure that stopped this replica, or nil.
//
// It is meaningful only once the replica has stopped, which a caller learns
// by seeing ErrStorageFailed from a write or "stopped" from Status.
func (r *Replica) Err() error {
	select {
	case <-r.doneC:
		return r.storageErr
	default:
		return nil
	}
}

// isClientError reports whether an apply error is a deterministic outcome to
// report to the client rather than a fault in this node.
//
// Deleting an absent key fails identically on every replica, so it is a
// result. A decode failure is not.
func isClientError(err error) bool {
	return errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrEmptyKey)
}
