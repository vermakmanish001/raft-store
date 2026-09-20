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
// nothing. Serving reads safely is a separate problem, addressed by a
// read-index protocol rather than by writing to the log.
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

// FSM applies committed entries to a store.
//
// It holds no lock of its own. Entries must be applied by a single goroutine
// in log order, which the caller guarantees; adding a lock here would imply
// concurrent application is acceptable, and it is not. Two nodes applying the
// same entries in different orders would reach different states.
type FSM struct {
	store store.Store
}

// New returns an FSM backed by the given store.
func New(s store.Store) *FSM {
	return &FSM{store: s}
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

	switch cmd.Op {
	case OpPut:
		return f.store.Put(cmd.Key, cmd.Value)
	case OpDelete:
		return f.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("%w: %q at index %d", ErrUnknownOp, cmd.Op, entry.Index)
	}
}
