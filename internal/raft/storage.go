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

	// SaveSnapshot durably records a snapshot and discards every log entry at
	// or before meta.Index.
	//
	// The snapshot must reach durable storage before any entry is discarded.
	// A crash in the other order leaves a node with neither the entries nor
	// the snapshot that replaced them, which is unrecoverable: the state those
	// entries described exists nowhere.
	SaveSnapshot(SnapshotMeta, []byte) error

	// LoadSnapshot returns the most recent snapshot, or a zero meta and nil
	// data if none was ever taken.
	LoadSnapshot() (SnapshotMeta, []byte, error)

	// Load returns the persisted term, vote, and the log entries that follow
	// the most recent snapshot.
	Load() (HardState, []LogEntry, error)
}

// SnapshotMeta describes where a snapshot sits in the log.
//
// Index and Term identify the last entry the snapshot includes. They are what
// let a compacted log still answer the consistency check: a follower asked
// about an index covered by the snapshot can compare against these instead of
// against an entry it no longer holds.
type SnapshotMeta struct {
	Index Index
	Term  Term
}

// IsEmpty reports whether any snapshot has been taken.
func (m SnapshotMeta) IsEmpty() bool { return m.Index == 0 }

// MemoryStorage is a Storage that keeps everything in memory.
//
// It is what a node with no configured data directory uses, and what the tests
// use. Such a node participates correctly while it runs, but on restart it
// returns having forgotten its term and vote, which is exactly the situation
// HardState exists to prevent. That is acceptable only because the cluster
// treats it as a brand new node, and unacceptable in production.
type MemoryStorage struct {
	mu       sync.Mutex
	state    HardState
	entries  []LogEntry
	snapMeta SnapshotMeta
	snapData []byte
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

	// Entries are addressed by their own Index rather than their position,
	// because compaction makes the slice a suffix of the logical log.
	for i, entry := range s.entries {
		if entry.Index >= index {
			s.entries = s.entries[:i]
			return nil
		}
	}
	return nil
}

func (s *MemoryStorage) SaveSnapshot(meta SnapshotMeta, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if meta.Index < s.snapMeta.Index {
		// An older snapshot would move the log backward and discard entries
		// the newer one already covers.
		return fmt.Errorf("raft: snapshot at index %d is older than the stored one at %d",
			meta.Index, s.snapMeta.Index)
	}

	s.snapMeta = meta
	s.snapData = append([]byte(nil), data...)

	kept := s.entries[:0]
	for _, entry := range s.entries {
		if entry.Index > meta.Index {
			kept = append(kept, entry)
		}
	}
	s.entries = append([]LogEntry(nil), kept...)
	return nil
}

func (s *MemoryStorage) LoadSnapshot() (SnapshotMeta, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.snapMeta, append([]byte(nil), s.snapData...), nil
}

func (s *MemoryStorage) Load() (HardState, []LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, append([]LogEntry(nil), s.entries...), nil
}
