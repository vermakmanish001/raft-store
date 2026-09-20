package fsm_test

import (
	"errors"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/fsm"
	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/store"
)

func TestCommandRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []fsm.Command{
		{Op: fsm.OpPut, Key: "alpha", Value: "one"},
		{Op: fsm.OpPut, Key: "empty-value", Value: ""},
		{Op: fsm.OpDelete, Key: "alpha"},
		{Op: fsm.OpPut, Key: "unicode: 世界", Value: "值"},
		{Op: fsm.OpPut, Key: "quotes\"and\\slashes", Value: "{\"json\": true}"},
	}

	for _, want := range tests {
		t.Run(want.Key, func(t *testing.T) {
			t.Parallel()

			data, err := want.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			got, err := fsm.Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got != want {
				t.Errorf("round trip = %+v, want %+v", got, want)
			}
		})
	}
}

// TestEncodingIsDeterministic guards the property every replica depends on:
// the same command must produce the same bytes everywhere.
func TestEncodingIsDeterministic(t *testing.T) {
	t.Parallel()

	cmd := fsm.Command{Op: fsm.OpPut, Key: "k", Value: "v"}

	first, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	for range 50 {
		again, err := cmd.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if string(again) != string(first) {
			t.Fatalf("encoding varies between calls: %q then %q", first, again)
		}
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"", "not json", "{", "[1,2,3]"} {
		if _, err := fsm.Decode([]byte(input)); err == nil {
			t.Errorf("Decode(%q) succeeded, want an error", input)
		}
	}
}

func TestApply(t *testing.T) {
	t.Parallel()

	t.Run("put writes to the store", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		f := fsm.New(st)

		if err := f.Apply(entryFor(t, fsm.Command{Op: fsm.OpPut, Key: "a", Value: "1"})); err != nil {
			t.Fatalf("Apply: %v", err)
		}

		got, err := st.Get("a")
		if err != nil || got != "1" {
			t.Errorf("store: got %q, err %v; want %q, nil", got, err, "1")
		}
	})

	t.Run("delete removes from the store", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		if err := st.Put("a", "1"); err != nil {
			t.Fatalf("seeding: %v", err)
		}
		f := fsm.New(st)

		if err := f.Apply(entryFor(t, fsm.Command{Op: fsm.OpDelete, Key: "a"})); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if n := st.Len(); n != 0 {
			t.Errorf("Len = %d, want 0", n)
		}
	})

	t.Run("deleting an absent key is a deterministic result", func(t *testing.T) {
		t.Parallel()

		f := fsm.New(store.New())

		err := f.Apply(entryFor(t, fsm.Command{Op: fsm.OpDelete, Key: "missing"}))
		if !errors.Is(err, store.ErrKeyNotFound) {
			t.Errorf("Apply = %v, want %v; every replica must reach the same outcome",
				err, store.ErrKeyNotFound)
		}
	})

	t.Run("no-op entries never reach the store", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		f := fsm.New(st)

		err := f.Apply(raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNoOp})
		if err != nil {
			t.Errorf("Apply = %v, want nil", err)
		}
		if n := st.Len(); n != 0 {
			t.Errorf("Len = %d, want 0; a no-op carries no command", n)
		}
	})

	t.Run("unknown operation fails loudly", func(t *testing.T) {
		t.Parallel()

		f := fsm.New(store.New())

		err := f.Apply(entryFor(t, fsm.Command{Op: "frobnicate", Key: "a"}))
		if !errors.Is(err, fsm.ErrUnknownOp) {
			t.Errorf("Apply = %v, want %v; silently skipping would let replicas diverge",
				err, fsm.ErrUnknownOp)
		}
	})

	t.Run("undecodable payload fails", func(t *testing.T) {
		t.Parallel()

		f := fsm.New(store.New())

		err := f.Apply(raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNormal, Command: []byte("garbage")})
		if err == nil {
			t.Error("Apply succeeded on an undecodable payload, want an error")
		}
	})
}

func entryFor(t *testing.T, cmd fsm.Command) raft.LogEntry {
	t.Helper()

	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryNormal, Command: data}
}
