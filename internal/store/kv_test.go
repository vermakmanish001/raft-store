package store_test

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/store"
)

func TestGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    map[string]string
		key     string
		want    string
		wantErr error
	}{
		{
			name: "returns stored value",
			seed: map[string]string{"alpha": "one"},
			key:  "alpha",
			want: "one",
		},
		{
			name: "empty value is a real value, not an absence",
			seed: map[string]string{"alpha": ""},
			key:  "alpha",
			want: "",
		},
		{
			name:    "missing key",
			seed:    map[string]string{"alpha": "one"},
			key:     "beta",
			wantErr: store.ErrKeyNotFound,
		},
		{
			name:    "missing key in empty store",
			key:     "alpha",
			wantErr: store.ErrKeyNotFound,
		},
		{
			name:    "empty key is rejected",
			seed:    map[string]string{"alpha": "one"},
			key:     "",
			wantErr: store.ErrEmptyKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := seeded(t, tc.seed)

			got, err := s.Get(tc.key)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Get(%q) error = %v, want %v", tc.key, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("Get(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

func TestPut(t *testing.T) {
	t.Parallel()

	t.Run("stores a new key", func(t *testing.T) {
		t.Parallel()

		s := store.New()
		if err := s.Put("alpha", "one"); err != nil {
			t.Fatalf("Put() error = %v, want nil", err)
		}

		got, err := s.Get("alpha")
		if err != nil {
			t.Fatalf("Get() after Put error = %v, want nil", err)
		}
		if got != "one" {
			t.Errorf("Get() = %q, want %q", got, "one")
		}
	})

	t.Run("overwrites an existing key", func(t *testing.T) {
		t.Parallel()

		s := seeded(t, map[string]string{"alpha": "one"})
		if err := s.Put("alpha", "two"); err != nil {
			t.Fatalf("Put() error = %v, want nil", err)
		}

		got, _ := s.Get("alpha")
		if got != "two" {
			t.Errorf("Get() after overwrite = %q, want %q", got, "two")
		}
		if n := s.Len(); n != 1 {
			t.Errorf("Len() after overwrite = %d, want 1", n)
		}
	})

	t.Run("is idempotent", func(t *testing.T) {
		t.Parallel()

		s := store.New()
		for range 3 {
			if err := s.Put("alpha", "one"); err != nil {
				t.Fatalf("Put() error = %v, want nil", err)
			}
		}

		got, _ := s.Get("alpha")
		if got != "one" || s.Len() != 1 {
			t.Errorf("after repeated Put: value = %q, Len = %d; want %q, 1", got, s.Len(), "one")
		}
	})

	t.Run("rejects the empty key", func(t *testing.T) {
		t.Parallel()

		s := store.New()
		if err := s.Put("", "one"); !errors.Is(err, store.ErrEmptyKey) {
			t.Fatalf("Put(\"\") error = %v, want %v", err, store.ErrEmptyKey)
		}
		if n := s.Len(); n != 0 {
			t.Errorf("Len() = %d, want 0; rejected Put must not mutate the store", n)
		}
	})
}

func TestDelete(t *testing.T) {
	t.Parallel()

	t.Run("removes an existing key", func(t *testing.T) {
		t.Parallel()

		s := seeded(t, map[string]string{"alpha": "one", "beta": "two"})
		if err := s.Delete("alpha"); err != nil {
			t.Fatalf("Delete() error = %v, want nil", err)
		}

		if _, err := s.Get("alpha"); !errors.Is(err, store.ErrKeyNotFound) {
			t.Errorf("Get() after Delete error = %v, want %v", err, store.ErrKeyNotFound)
		}
		if n := s.Len(); n != 1 {
			t.Errorf("Len() after Delete = %d, want 1", n)
		}
	})

	t.Run("reports a missing key", func(t *testing.T) {
		t.Parallel()

		s := store.New()
		if err := s.Delete("alpha"); !errors.Is(err, store.ErrKeyNotFound) {
			t.Fatalf("Delete() error = %v, want %v", err, store.ErrKeyNotFound)
		}
	})

	t.Run("second delete reports a missing key", func(t *testing.T) {
		t.Parallel()

		s := seeded(t, map[string]string{"alpha": "one"})
		if err := s.Delete("alpha"); err != nil {
			t.Fatalf("first Delete() error = %v, want nil", err)
		}
		if err := s.Delete("alpha"); !errors.Is(err, store.ErrKeyNotFound) {
			t.Errorf("second Delete() error = %v, want %v", err, store.ErrKeyNotFound)
		}
	})

	t.Run("rejects the empty key", func(t *testing.T) {
		t.Parallel()

		s := store.New()
		if err := s.Delete(""); !errors.Is(err, store.ErrEmptyKey) {
			t.Fatalf("Delete(\"\") error = %v, want %v", err, store.ErrEmptyKey)
		}
	})
}

// TestSentinelErrorsSurviveWrapping guards the contract that callers may use
// errors.Is. The HTTP layer maps these errors to status codes, and later
// milestones wrap them as they cross package boundaries.
func TestSentinelErrorsSurviveWrapping(t *testing.T) {
	t.Parallel()

	s := store.New()
	_, err := s.Get("absent")

	wrapped := fmt.Errorf("serving request: %w", err)
	if !errors.Is(wrapped, store.ErrKeyNotFound) {
		t.Errorf("errors.Is(wrapped, ErrKeyNotFound) = false, want true")
	}
}

// TestConcurrentAccess is meaningful only under -race, where it asserts that
// no combination of concurrent readers and writers trips the detector. The
// final-state assertions additionally catch lost updates.
func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	const (
		writers = 8
		keys    = 50
	)

	s := store.New()
	var wg sync.WaitGroup

	// Each writer owns a disjoint key space, so the expected final state is
	// deterministic even though the interleaving is not.
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keys {
				key := strconv.Itoa(w) + ":" + strconv.Itoa(k)
				if err := s.Put(key, strconv.Itoa(k)); err != nil {
					t.Errorf("Put(%q) error = %v, want nil", key, err)
					return
				}
			}
		}()
	}

	// Readers race against the writers over the same key space. They must
	// never observe a torn value: either the key is absent or it holds the
	// value its writer wrote.
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keys {
				key := strconv.Itoa(w) + ":" + strconv.Itoa(k)
				got, err := s.Get(key)
				if errors.Is(err, store.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					t.Errorf("Get(%q) error = %v, want nil", key, err)
					return
				}
				if want := strconv.Itoa(k); got != want {
					t.Errorf("Get(%q) = %q, want %q", key, got, want)
					return
				}
			}
		}()
	}

	wg.Wait()

	if n := s.Len(); n != writers*keys {
		t.Errorf("Len() = %d, want %d; a write was lost", n, writers*keys)
	}
}

// TestConcurrentDelete exercises writers and deleters against the same keys,
// which is the interleaving most likely to expose a missing exclusive lock.
func TestConcurrentDelete(t *testing.T) {
	t.Parallel()

	const goroutines = 8

	s := store.New()
	var wg sync.WaitGroup

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range 100 {
				key := strconv.Itoa(k)
				if err := s.Put(key, "v"); err != nil {
					t.Errorf("Put(%q) error = %v, want nil", key, err)
					return
				}
				// Delete may legitimately lose the race to another goroutine's
				// delete, so ErrKeyNotFound is an acceptable outcome here.
				if err := s.Delete(key); err != nil && !errors.Is(err, store.ErrKeyNotFound) {
					t.Errorf("Delete(%q) error = %v, want nil or %v", key, err, store.ErrKeyNotFound)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// seeded returns a MemStore preloaded with the given pairs.
func seeded(t *testing.T, pairs map[string]string) *store.MemStore {
	t.Helper()

	s := store.New()
	for k, v := range pairs {
		if err := s.Put(k, v); err != nil {
			t.Fatalf("seeding Put(%q) error = %v, want nil", k, err)
		}
	}
	return s
}
