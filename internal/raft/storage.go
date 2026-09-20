package raft

import (
	"fmt"
	"sync"
)

// HardState is the state Section 5 requires be durable before a node responds
// to any RPC.
//
// The requirement is not a durability nicety, it is what prevents two leaders
// in one term. A node that crashes after voting and restarts having forgotten
// the vote will happily vote again in the same term. Two candidates can then
// each collect a majority that includes this node, and both become leader for
// the same term, after which they can commit conflicting entries at the same
// index.
type HardState struct {
	Term     Term
	VotedFor NodeID
}

// Storage durably records the state a node must not lose across a restart.
//
// It is an interface rather than a file handle so the raft package still opens
// nothing itself: tests inject an in-memory implementation and keep running
// without a disk, while a real node injects a write-ahead log.
//
// Every method must not return until the data is durable. Returning once the
// write has merely been handed to the operating system defeats the entire
// purpose, because a crash would lose exactly the writes the algorithm assumed
// were safe.
//
// Implementations are called only from the goroutine driving the Node, so they
// need not be safe for concurrent use on that account, though nothing prevents
// it.
type Storage interface {
	// SaveHardState records the current term and vote.
	SaveHardState(HardState) error

	// Append durably adds entries to the end of the log.
	Append([]LogEntry) error

	// TruncateFrom removes the entry at index and everything after it.
	TruncateFrom(Index) error

	// Load returns the persisted state, or a zero state and no entries for a
	// node that has never run before.
	Load() (HardState, []LogEntry, error)
}

// MemoryStorage is a Storage that keeps everything in memory.
//
// It is what a node with no configured data directory uses, and what the tests
// use. Such a node participates correctly while it runs, but on restart it
// returns having forgotten its term and vote, which is exactly the situation
// HardState exists to prevent. That is acceptable only because the cluster
// treats it as a brand new node, and unacceptable in production.
type MemoryStorage struct {
	mu      sync.Mutex
	state   HardState
	entries []LogEntry
}

// NewMemoryStorage returns an empty in-memory Storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

var _ Storage = (*MemoryStorage)(nil)

func (s *MemoryStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = hs
	return nil
}

func (s *MemoryStorage) Append(entries []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = append(s.entries, entries...)
	return nil
}

func (s *MemoryStorage) TruncateFrom(index Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index == 0 {
		return fmt.Errorf("raft: TruncateFrom(0): the log is 1-indexed")
	}
	if index > Index(len(s.entries)) {
		return nil
	}
	s.entries = s.entries[:index-1]
	return nil
}

func (s *MemoryStorage) Load() (HardState, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, append([]LogEntry(nil), s.entries...), nil
}
