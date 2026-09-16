// Package store provides the key-value storage engine that sits beneath the
// raft-store HTTP API and, from Milestone 4 onward, beneath the Raft state
// machine.
//
// Nothing in this package knows about consensus, replication, or HTTP. That
// separation is deliberate rather than incidental: Raft drives this package by
// applying already-committed log entries to it, so any coupling to transport
// or cluster state would have to be unpicked later. The apply path must also
// be deterministic, because every node in the cluster applies the same entries
// in the same order and must arrive at byte-identical state.
package store

import (
	"errors"
	"sync"
)

// Sentinel errors returned by Store implementations.
//
// Callers must test for these with errors.Is rather than comparing error
// values or strings directly, since later milestones wrap them with context
// as they cross package boundaries.
var (
	// ErrKeyNotFound reports that the requested key is absent from the store.
	ErrKeyNotFound = errors.New("store: key not found")

	// ErrEmptyKey reports an attempt to operate on the empty key. It is
	// rejected so a malformed request path cannot silently create an entry
	// that no well-formed request could ever address.
	ErrEmptyKey = errors.New("store: key must not be empty")
)

// Store is the contract the rest of the system depends on.
//
// It exists as an interface from the outset so that Milestone 4 can introduce
// a Raft-backed implementation, which replicates writes through the consensus
// log before applying them, without the API layer changing at all.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Store interface {
	// Get returns the value stored under key. It returns ErrKeyNotFound if
	// the key is absent, and ErrEmptyKey if key is empty.
	Get(key string) (string, error)

	// Put stores value under key, replacing any previous value. It returns
	// ErrEmptyKey if key is empty. Put is idempotent: storing the same pair
	// twice leaves the store in the same state as storing it once.
	Put(key, value string) error

	// Delete removes key from the store. It returns ErrKeyNotFound if the key
	// is absent, and ErrEmptyKey if key is empty.
	//
	// Reporting absence is the more informative primitive; callers that want
	// idempotent removal can ignore ErrKeyNotFound. The HTTP layer maps it to
	// 404 so clients can tell a real deletion from a no-op.
	Delete(key string) error
}

// MemStore is an in-memory, concurrency-safe implementation of Store.
//
// Reads are served concurrently under a shared lock and writes take the lock
// exclusively, which suits the read-heavy access pattern typical of a
// key-value store.
//
// Values are held as strings rather than []byte deliberately. Go strings are
// immutable, so a value handed back to a caller cannot be mutated behind the
// store's back. A []byte-valued map would need a defensive copy on every read
// and every write to offer the same guarantee, and forgetting one of those
// copies is a data race that no amount of locking here would catch.
//
// The zero value is not usable; call New.
type MemStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// Compile-time assertion that MemStore satisfies Store. This fails the build
// rather than a test if the interface and implementation drift apart.
var _ Store = (*MemStore)(nil)

// New returns an empty, ready-to-use MemStore.
func New() *MemStore {
	return &MemStore{data: make(map[string]string)}
}

// Get returns the value stored under key.
func (s *MemStore) Get(key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return value, nil
}

// Put stores value under key, replacing any previous value.
func (s *MemStore) Put(key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	return nil
}

// Delete removes key from the store.
func (s *MemStore) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return ErrKeyNotFound
	}
	delete(s.data, key)
	return nil
}

// Len returns the number of keys currently held.
//
// It is not part of the Store interface because replicated implementations
// cannot answer it consistently without going through consensus. It exists
// for tests and, later, for reporting local state size.
func (s *MemStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}
