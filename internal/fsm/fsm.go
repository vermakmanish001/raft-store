// Package fsm translates committed Raft log entries into key-value mutations.
//
// It is the seam between consensus and storage. The raft package treats a
// command as opaque bytes and the store package knows nothing of replication;
// this package is the only place that understands both.
package fsm

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// Op identifies the mutation a command performs.
//
// Reads are absent deliberately. A read changes no state, so replicating it
// through the log would cost a round trip and a log entry to accomplish
// nothing. Serving reads safely is a separate problem, solved by the read
// barrier in the raft package rather than by writing to the log.
type Op string

const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
)

// ErrUnknownOp reports a command this version does not understand.
//
// Because entries are replayed from a durable log, a node can encounter a
// command written by a newer version of the software. Failing loudly is the
// correct response: silently skipping it would let two nodes apply different
// sequences and diverge, which is precisely what the log exists to prevent.
var ErrUnknownOp = errors.New("fsm: unknown operation")

// DefaultMaxSessions bounds how many clients are remembered for deduplication.
const DefaultMaxSessions = 4096

// Command is the payload carried in a Raft log entry.
//
// It is encoded as JSON. A binary format would be smaller and faster, and the
// encoding is deliberately isolated here so it can be replaced later, but JSON
// makes a log readable with ordinary tools, which matters enormously while
// debugging a consensus implementation.
//
// Encoding must be deterministic: every node encodes and decodes the same
// bytes, and a difference in interpretation would mean divergent state. A
// struct with fixed fields provides that; a map would not.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`

	// ClientID and Seq identify a request so a retry can be recognized.
	//
	// They are what make a write exactly-once rather than at-least-once. A
	// client whose write commits but whose response is lost, because the
	// leader crashed in between, cannot tell success from failure and must
	// retry. Without these fields the retry applies a second time. That is
	// harmless for a repeated Put of the same value, but not in general: if
	// another client wrote the same key in the interim, the late duplicate
	// silently overwrites the newer value.
	//
	// They are optional. A request without them is applied unconditionally,
	// which is the at-least-once behavior and is fine for a one-shot command
	// typed at a terminal.
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

// Encode serializes the command for storage in a log entry.
func (c Command) Encode() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("fsm: encoding command: %w", err)
	}
	return b, nil
}

// Decode parses a command from a log entry's payload.
func Decode(data []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return Command{}, fmt.Errorf("fsm: decoding command: %w", err)
	}
	return c, nil
}

// resultCode is a command's outcome, recorded so a duplicate request can be
// answered identically to the original.
//
// The outcome is stored as a code rather than an error value because it must
// be reproduced identically on every replica. An error carrying a message
// built at apply time would differ between nodes.
type resultCode uint8

const (
	resultOK resultCode = iota
	resultKeyNotFound
)

func (r resultCode) err() error {
	if r == resultKeyNotFound {
		return store.ErrKeyNotFound
	}
	return nil
}

func codeFor(err error) resultCode {
	if errors.Is(err, store.ErrKeyNotFound) {
		return resultKeyNotFound
	}
	return resultOK
}

// session records the last request applied for one client.
//
// Only the most recent sequence number is kept, not a history. A client sends
// one request at a time and retries the same one until it succeeds, so a
// request older than the last one applied is by definition already answered.
type session struct {
	lastSeq    uint64
	lastResult resultCode

	// usedAt orders sessions for eviction. It counts applied commands rather
	// than wall-clock time, because eviction must be identical on every
	// replica and clocks are not.
	usedAt uint64
}

// FSM applies committed entries to a store.
//
// It holds no lock of its own. Entries must be applied by a single goroutine
// in log order, which the caller guarantees; adding a lock here would imply
// concurrent application is acceptable, and it is not. Two nodes applying the
// same entries in different orders would reach different states.
type FSM struct {
	store store.Store

	// sessions deduplicates client retries. It is derived entirely from the
	// log, so every replica reconstructs the same table, and a node that
	// restarts rebuilds it by replaying entries.
	sessions    map[string]session
	maxSessions int

	// applied counts commands, providing the deterministic clock that orders
	// eviction.
	applied uint64
}

// Option customizes an FSM.
type Option func(*FSM)

// WithMaxSessions overrides how many clients are remembered.
func WithMaxSessions(n int) Option {
	return func(f *FSM) { f.maxSessions = n }
}

// New returns an FSM backed by the given store.
func New(s store.Store, opts ...Option) *FSM {
	f := &FSM{
		store:       s,
		sessions:    make(map[string]session),
		maxSessions: DefaultMaxSessions,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Apply executes one committed entry and returns the operation's outcome.
//
// The returned error is a result, not a failure of the apply machinery. A
// delete of an absent key yields store.ErrKeyNotFound on every node, so it is
// deterministic and safe to report back to the waiting client. A failure to
// decode, by contrast, means the log itself is not what this node expects.
func (f *FSM) Apply(entry raft.LogEntry) error {
	// A no-op exists only to let a new leader advance its commit index. It
	// carries no command and must not reach the store.
	if entry.Type == raft.EntryNoOp {
		return nil
	}

	cmd, err := Decode(entry.Command)
	if err != nil {
		return err
	}

	f.applied++

	// A repeat of a request already applied returns the original outcome
	// without touching the store. Re-executing it is exactly the double-apply
	// this table exists to prevent.
	if cmd.ClientID != "" {
		if prior, ok := f.sessions[cmd.ClientID]; ok && cmd.Seq <= prior.lastSeq {
			prior.usedAt = f.applied
			f.sessions[cmd.ClientID] = prior
			return prior.lastResult.err()
		}
	}

	result := f.execute(cmd, entry.Index)

	if cmd.ClientID != "" {
		f.remember(cmd, result)
	}
	return result
}

// execute performs the mutation.
func (f *FSM) execute(cmd Command, index raft.Index) error {
	switch cmd.Op {
	case OpPut:
		return f.store.Put(cmd.Key, cmd.Value)
	case OpDelete:
		return f.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("%w: %q at index %d", ErrUnknownOp, cmd.Op, index)
	}
}

// remember records the outcome so a retry can be answered from it.
func (f *FSM) remember(cmd Command, result error) {
	// An apply failure that is not a client-visible result means this node
	// could not reproduce the agreed state. Recording it would answer a retry
	// with a fault rather than re-attempting, so it is left unrecorded.
	if result != nil && !errors.Is(result, store.ErrKeyNotFound) {
		return
	}

	f.sessions[cmd.ClientID] = session{
		lastSeq:    cmd.Seq,
		lastResult: codeFor(result),
		usedAt:     f.applied,
	}
	f.evictIfNeeded()
}

// evictIfNeeded drops the least recently used session once the table is full.
//
// Eviction is a real tradeoff, not a free optimization: a client whose session
// is evicted and then retries will have its write applied a second time. The
// alternative, an unbounded table, lets any client that never returns consume
// memory forever. A production system resolves this with explicit sessions
// that clients renew and close; the cap is the honest approximation.
//
// The choice of victim is driven by the apply counter, so every replica evicts
// the same client at the same log index. Using wall-clock time here would let
// replicas diverge.
func (f *FSM) evictIfNeeded() {
	if f.maxSessions <= 0 || len(f.sessions) <= f.maxSessions {
		return
	}

	// The victim is the session with the smallest usedAt. That value comes
	// from a counter incremented once per applied command, so no two sessions
	// can share one and the minimum is unique. Uniqueness is what makes the
	// result independent of Go's randomized map iteration order, and so
	// identical on every replica; a timestamp, which can tie, would not be
	// enough.
	var (
		oldestID string
		oldestAt uint64
		first    = true
	)
	for id, s := range f.sessions {
		if first || s.usedAt < oldestAt {
			oldestID, oldestAt, first = id, s.usedAt, false
		}
	}
	delete(f.sessions, oldestID)
}

// Sessions returns how many clients are currently remembered.
func (f *FSM) Sessions() int { return len(f.sessions) }
