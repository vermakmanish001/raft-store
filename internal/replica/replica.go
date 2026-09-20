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

	// Storage durably records term, vote, and log. A nil Storage gets an
	// in-memory one, so the node participates correctly while it runs and
	// remembers nothing across a restart.
	Storage raft.Storage
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
}

// proposal is a client write awaiting commitment.
type proposal struct {
	command []byte

	// result is buffered so the consensus loop can resolve a proposal whose
	// caller has already given up, without blocking.
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
	statusC  chan chan Status
	stopC    chan struct{}
	doneC    chan struct{}

	// pending is owned exclusively by the run loop. Nothing else may touch it.
	pending map[raft.Index]pending

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

	return &Replica{
		cfg:      cfg,
		node:     node,
		fsm:      fsm.New(cfg.Store),
		store:    cfg.Store,
		tr:       cfg.Transport,
		logger:   cfg.Logger,
		proposeC: make(chan proposal),
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

	r.applyCommitted()

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
}

// failPending completes every waiting write with the same error.
func (r *Replica) failPending(err error) {
	for index, waiting := range r.pending {
		waiting.result <- err
		delete(r.pending, index)
	}
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
