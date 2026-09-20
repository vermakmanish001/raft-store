package replica

import (
	"fmt"
	"time"

	"github.com/vermakmanish001/raft-store/internal/fsm"
	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// Get returns the value stored under key.
//
// Reads are served from this node's applied state and are NOT linearizable.
// Two gaps remain, both closed by the read-index protocol in a later
// milestone. A leader that has been deposed without noticing will answer from
// stale state, and even a legitimate leader may answer from state older than a
// write committed moments ago on its own log.
//
// Requiring leadership narrows the window without eliminating it, and is done
// here so that every operation routes through the leader uniformly.
func (r *Replica) Get(key string) (string, error) {
	if err := r.requireLeader(); err != nil {
		return "", err
	}
	return r.store.Get(key)
}

// Put replicates a write through the log and returns once it is committed and
// applied.
func (r *Replica) Put(key, value string) error {
	return r.apply(fsm.Command{Op: fsm.OpPut, Key: key, Value: value})
}

// Delete replicates a deletion through the log.
func (r *Replica) Delete(key string) error {
	return r.apply(fsm.Command{Op: fsm.OpDelete, Key: key})
}

// requireLeader reports ErrNotLeader unless this node currently leads.
func (r *Replica) requireLeader() error {
	if r.Status().Role != raft.Leader.String() {
		return raft.ErrNotLeader
	}
	return nil
}

// apply submits a command and waits for the resulting entry to commit.
//
// Validation happens before the command reaches the log. Replicating a write
// that every node will reject wastes a round trip and an entry that can never
// be compacted away, so an empty key is refused here rather than at apply time.
func (r *Replica) apply(cmd fsm.Command) error {
	if cmd.Key == "" {
		return store.ErrEmptyKey
	}

	command, err := cmd.Encode()
	if err != nil {
		return err
	}

	// Buffered, so the consensus loop can resolve this proposal even after the
	// caller has timed out and stopped listening.
	result := make(chan error, 1)

	timer := time.NewTimer(r.cfg.WriteTimeout)
	defer timer.Stop()

	select {
	case r.proposeC <- proposal{command: command, result: result}:
	case <-timer.C:
		// The consensus loop never accepted the proposal, so nothing was
		// appended and no retry can duplicate it.
		return ErrTimeout
	case <-r.doneC:
		return ErrShuttingDown
	}

	select {
	case err := <-result:
		return err
	case <-r.doneC:
		if storageErr := r.Err(); storageErr != nil {
			return fmt.Errorf("%w: %v", ErrStorageFailed, storageErr)
		}
		return ErrShuttingDown
	case <-timer.C:
		// The entry was appended and may yet commit. The outcome is genuinely
		// unknown to the client, which is why this is reported as a timeout
		// rather than a failure.
		return ErrTimeout
	}
}
