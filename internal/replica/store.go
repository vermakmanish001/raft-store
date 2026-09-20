package replica

import (
	"fmt"
	"time"

	"github.com/vermakmanish001/raft-store/internal/fsm"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// Get returns the value stored under key.
//
// The read is linearizable: it reflects every write that completed before the
// call, and none that had not been proposed. Two sources of staleness are
// ruled out before the store is touched. This node may have been deposed
// without noticing, so it waits to hear from a majority that it still leads.
// And its state machine may lag its commit index, so it waits until everything
// committed at the moment the read arrived has been applied.
//
// The cost is one round trip to a majority, piggybacked on a heartbeat. A
// system willing to trade correctness for latency would skip it and serve from
// local state, which is what this did previously.
func (r *Replica) Get(key string) (string, error) {
	if key == "" {
		return "", store.ErrEmptyKey
	}
	if err := r.awaitReadBarrier(); err != nil {
		return "", err
	}

	// Reading after the barrier is satisfied is safe without holding anything:
	// the store is independently concurrency-safe, and further entries applied
	// between the barrier and this call only make the answer fresher, never
	// staler.
	return r.store.Get(key)
}

// Put replicates a write through the log and returns once it is committed and
// applied. It is deduplicated only if req carries a client ID.
func (r *Replica) Put(key, value string) error {
	return r.PutRequest(key, value, Request{})
}

// Delete replicates a deletion through the log.
func (r *Replica) Delete(key string) error {
	return r.DeleteRequest(key, Request{})
}

// PutRequest replicates a write, deduplicating retries that carry the same
// client ID and sequence number.
func (r *Replica) PutRequest(key, value string, req Request) error {
	return r.apply(fsm.Command{
		Op:       fsm.OpPut,
		Key:      key,
		Value:    value,
		ClientID: req.ClientID,
		Seq:      req.Seq,
	})
}

// DeleteRequest replicates a deletion, deduplicating retries.
func (r *Replica) DeleteRequest(key string, req Request) error {
	return r.apply(fsm.Command{
		Op:       fsm.OpDelete,
		Key:      key,
		ClientID: req.ClientID,
		Seq:      req.Seq,
	})
}

// awaitReadBarrier blocks until this node has proved it may serve a read.
func (r *Replica) awaitReadBarrier() error {
	// Buffered, so the consensus loop can resolve a barrier whose caller has
	// already given up.
	result := make(chan error, 1)

	timer := time.NewTimer(r.cfg.ReadTimeout)
	defer timer.Stop()

	select {
	case r.readC <- readBarrier{result: result}:
	case <-timer.C:
		return ErrReadTimeout
	case <-r.doneC:
		return r.shutdownErr()
	}

	select {
	case err := <-result:
		return err
	case <-timer.C:
		// No majority answered in time, so this node cannot establish that it
		// still leads. Refusing is the point: answering would risk returning
		// state that a newer leader has already superseded.
		return ErrReadTimeout
	case <-r.doneC:
		return r.shutdownErr()
	}
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
		return r.shutdownErr()
	}

	select {
	case err := <-result:
		return err
	case <-r.doneC:
		return r.shutdownErr()
	case <-timer.C:
		// The entry was appended and may yet commit. The outcome is genuinely
		// unknown to the client, which is why this is reported as a timeout
		// rather than a failure. A client that supplied a Request can retry
		// safely; one that did not may apply the write twice.
		return ErrTimeout
	}
}

// shutdownErr distinguishes an orderly stop from a storage failure.
func (r *Replica) shutdownErr() error {
	if storageErr := r.Err(); storageErr != nil {
		return fmt.Errorf("%w: %v", ErrStorageFailed, storageErr)
	}
	return ErrShuttingDown
}
