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

// applyCmd applies one command at the given log index.
func applyCmd(t *testing.T, f *fsm.FSM, index uint64, cmd fsm.Command) error {
	t.Helper()

	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return f.Apply(raft.LogEntry{
		Index: raft.Index(index), Term: 1, Type: raft.EntryNormal, Command: data,
	})
}

// TestRetryDoesNotOverwriteANewerWrite is the reason deduplication exists.
//
// A client whose write commits but whose response is lost cannot tell success
// from failure, so it retries. If another client wrote the same key in the
// interim, applying the retry silently destroys that newer value. The write
// was never lost from the log, and every replica agrees on the result: the
// data is simply wrong.
func TestRetryDoesNotOverwriteANewerWrite(t *testing.T) {
	t.Parallel()

	st := store.New()
	f := fsm.New(st)

	// Client A writes, and its response is lost on the way back.
	if err := applyCmd(t, f, 1, fsm.Command{
		Op: fsm.OpPut, Key: "x", Value: "from A", ClientID: "client-a", Seq: 1,
	}); err != nil {
		t.Fatalf("A's write: %v", err)
	}

	// Client B writes the same key and is told it succeeded.
	if err := applyCmd(t, f, 2, fsm.Command{
		Op: fsm.OpPut, Key: "x", Value: "from B", ClientID: "client-b", Seq: 1,
	}); err != nil {
		t.Fatalf("B's write: %v", err)
	}

	// Client A retries the request it never heard back about.
	if err := applyCmd(t, f, 3, fsm.Command{
		Op: fsm.OpPut, Key: "x", Value: "from A", ClientID: "client-a", Seq: 1,
	}); err != nil {
		t.Fatalf("A's retry: %v", err)
	}

	got, err := st.Get("x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "from B" {
		t.Errorf("x = %q, want %q: A's retry destroyed a newer write that B was told had succeeded", got, "from B")
	}
}

func TestDuplicateReturnsTheOriginalResult(t *testing.T) {
	t.Parallel()

	f := fsm.New(store.New())

	// A delete of an absent key fails deterministically.
	first := applyCmd(t, f, 1, fsm.Command{Op: fsm.OpDelete, Key: "ghost", ClientID: "c", Seq: 1})
	if !errors.Is(first, store.ErrKeyNotFound) {
		t.Fatalf("first delete = %v, want %v", first, store.ErrKeyNotFound)
	}

	// The retry must report the same outcome, not re-run and possibly differ.
	again := applyCmd(t, f, 2, fsm.Command{Op: fsm.OpDelete, Key: "ghost", ClientID: "c", Seq: 1})
	if !errors.Is(again, store.ErrKeyNotFound) {
		t.Errorf("retry = %v, want the original %v", again, store.ErrKeyNotFound)
	}
}

func TestOlderSequenceIsAlsoTreatedAsDuplicate(t *testing.T) {
	t.Parallel()

	st := store.New()
	f := fsm.New(st)

	for seq := uint64(1); seq <= 3; seq++ {
		if err := applyCmd(t, f, seq, fsm.Command{
			Op: fsm.OpPut, Key: "x", Value: "v" + string(rune('0'+seq)), ClientID: "c", Seq: seq,
		}); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
	}

	// A long-delayed duplicate of an earlier request arrives.
	if err := applyCmd(t, f, 4, fsm.Command{
		Op: fsm.OpPut, Key: "x", Value: "stale", ClientID: "c", Seq: 1,
	}); err != nil {
		t.Fatalf("stale retry: %v", err)
	}

	if got, _ := st.Get("x"); got != "v3" {
		t.Errorf("x = %q, want %q; a request older than the last applied is already answered", got, "v3")
	}
}

func TestNewSequenceIsApplied(t *testing.T) {
	t.Parallel()

	st := store.New()
	f := fsm.New(st)

	for seq := uint64(1); seq <= 5; seq++ {
		if err := applyCmd(t, f, seq, fsm.Command{
			Op: fsm.OpPut, Key: "x", Value: "v" + string(rune('0'+seq)), ClientID: "c", Seq: seq,
		}); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
	}

	if got, _ := st.Get("x"); got != "v5" {
		t.Errorf("x = %q, want %q; distinct requests must each apply", got, "v5")
	}
}

// TestUnidentifiedRequestsAreNotDeduplicated documents the at-least-once
// fallback for a client that supplies no ID, such as a plain curl.
func TestUnidentifiedRequestsAreNotDeduplicated(t *testing.T) {
	t.Parallel()

	st := store.New()
	f := fsm.New(st)

	applyCmd(t, f, 1, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "first"})
	applyCmd(t, f, 2, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "second"})
	applyCmd(t, f, 3, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "first"})

	if got, _ := st.Get("x"); got != "first" {
		t.Errorf("x = %q, want %q: without a client ID every command applies", got, "first")
	}
	if n := f.Sessions(); n != 0 {
		t.Errorf("Sessions = %d, want 0; unidentified requests must not consume table space", n)
	}
}

// TestSessionsRebuildOnReplay: the table is derived entirely from the log, so
// a restarting node reconstructs it and a failover leaves it intact.
func TestSessionsRebuildOnReplay(t *testing.T) {
	t.Parallel()

	entries := []fsm.Command{
		{Op: fsm.OpPut, Key: "x", Value: "from A", ClientID: "client-a", Seq: 1},
		{Op: fsm.OpPut, Key: "x", Value: "from B", ClientID: "client-b", Seq: 1},
	}

	// A different node, or the same one after a restart, replays the log into
	// a fresh store and a fresh table.
	st := store.New()
	f := fsm.New(st)
	for i, cmd := range entries {
		if err := applyCmd(t, f, uint64(i+1), cmd); err != nil {
			t.Fatalf("replaying %d: %v", i, err)
		}
	}

	// A's retry reaches the rebuilt node and must still be recognized.
	applyCmd(t, f, 3, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "from A", ClientID: "client-a", Seq: 1})

	if got, _ := st.Get("x"); got != "from B" {
		t.Errorf("x = %q, want %q; the rebuilt session table failed to catch the retry", got, "from B")
	}
}

// TestSessionEvictionIsDeterministic is a correctness requirement, not a
// tidiness one. Every replica applies the same log and must reach the same
// state. If eviction depended on wall-clock time, or on Go's randomized map
// iteration order, two replicas would remember different clients, disagree
// about which retries are duplicates, and diverge.
//
// The check is the one that actually matters: feed an identical log to
// independent state machines and require identical results.
func TestSessionEvictionIsDeterministic(t *testing.T) {
	t.Parallel()

	const (
		clients = 30
		cap     = 8
	)

	// The log: every client writes, then every client retries. Which retries
	// are suppressed depends entirely on which sessions survived eviction.
	var log []fsm.Command
	for i := range clients {
		log = append(log, fsm.Command{
			Op: fsm.OpPut, Key: key(i), Value: "original", ClientID: clientName(i), Seq: 1,
		})
	}
	// Retries run newest client first. Retrying oldest first would evict each
	// surviving session before reaching it, so every retry would apply and the
	// test would compare two uniformly wrong results.
	for i := clients - 1; i >= 0; i-- {
		log = append(log, fsm.Command{
			Op: fsm.OpPut, Key: key(i), Value: "RETRY-APPLIED", ClientID: clientName(i), Seq: 1,
		})
	}

	replay := func() []string {
		st := store.New()
		f := fsm.New(st, fsm.WithMaxSessions(cap))

		for i, cmd := range log {
			applyCmd(t, f, uint64(i+1), cmd)
		}

		values := make([]string, clients)
		for i := range clients {
			v, err := st.Get(key(i))
			if err != nil {
				t.Fatalf("Get(%s): %v", key(i), err)
			}
			values[i] = v
		}
		return values
	}

	first := replay()

	// The test is only meaningful if eviction actually happened, meaning some
	// retries were suppressed and others were not.
	var suppressed, applied int
	for _, v := range first {
		if v == "original" {
			suppressed++
		} else {
			applied++
		}
	}
	if suppressed == 0 || applied == 0 {
		t.Fatalf("every retry behaved the same way (%d suppressed, %d applied); "+
			"the cap did not take effect and the test proves nothing", suppressed, applied)
	}

	for run := range 25 {
		got := replay()
		for i := range got {
			if got[i] != first[i] {
				t.Fatalf("run %d diverged at %s: got %q, first run got %q; "+
					"two replicas applying this log would disagree",
					run, key(i), got[i], first[i])
			}
		}
	}
}

func clientName(i int) string {
	return "client-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

func key(i int) string {
	return "key-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}

// TestEvictionKeepsRecentClients: the client most likely to retry is the one
// that just wrote, so it must not be the one evicted.
func TestEvictionKeepsRecentClients(t *testing.T) {
	t.Parallel()

	st := store.New()
	f := fsm.New(st, fsm.WithMaxSessions(3))

	index := uint64(0)
	next := func(cmd fsm.Command) {
		index++
		applyCmd(t, f, index, cmd)
	}

	next(fsm.Command{Op: fsm.OpPut, Key: "x", Value: "old", ClientID: "oldest", Seq: 1})
	next(fsm.Command{Op: fsm.OpPut, Key: "x", Value: "b", ClientID: "b", Seq: 1})
	next(fsm.Command{Op: fsm.OpPut, Key: "x", Value: "c", ClientID: "c", Seq: 1})

	// A fourth client evicts the least recently used, which is "oldest".
	next(fsm.Command{Op: fsm.OpPut, Key: "x", Value: "d", ClientID: "d", Seq: 1})

	// "d" just wrote, so its retry must still be recognized.
	next(fsm.Command{Op: fsm.OpPut, Key: "x", Value: "d", ClientID: "d", Seq: 1})
	if got, _ := st.Get("x"); got != "d" {
		t.Errorf("x = %q, want %q; a recent client's retry was not deduplicated", got, "d")
	}
}

// TestSnapshotRoundTrip covers the whole state machine, including the part
// most implementations forget.
func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	original := store.New()
	f := fsm.New(original)

	applyCmd(t, f, 1, fsm.Command{Op: fsm.OpPut, Key: "a", Value: "1", ClientID: "client-a", Seq: 1})
	applyCmd(t, f, 2, fsm.Command{Op: fsm.OpPut, Key: "b", Value: "2", ClientID: "client-b", Seq: 1})
	applyCmd(t, f, 3, fsm.Command{Op: fsm.OpDelete, Key: "a", ClientID: "client-a", Seq: 2})

	data, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// A different node restores it into an empty store.
	restoredStore := store.New()
	restored := fsm.New(restoredStore)
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if got, err := restoredStore.Get("b"); err != nil || got != "2" {
		t.Errorf("b = %q, %v; want %q, nil", got, err, "2")
	}
	if _, err := restoredStore.Get("a"); !errors.Is(err, store.ErrKeyNotFound) {
		t.Errorf("a = %v, want %v; the deletion must be captured too", err, store.ErrKeyNotFound)
	}
	if got := restored.Sessions(); got != 2 {
		t.Errorf("Sessions = %d, want 2", got)
	}
}

// TestSnapshotPreservesDeduplication is the bug this test exists to catch.
//
// A snapshot that omits the session table restores a node which has forgotten
// which client requests it already applied. Deduplication then holds right up
// until the first snapshot and silently stops, which is far worse than never
// having had it.
func TestSnapshotPreservesDeduplication(t *testing.T) {
	t.Parallel()

	source := fsm.New(store.New())
	applyCmd(t, source, 1, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "from A", ClientID: "client-a", Seq: 1})
	applyCmd(t, source, 2, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "from B", ClientID: "client-b", Seq: 1})

	data, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// A node restored from that snapshot receives A's retry.
	st := store.New()
	restored := fsm.New(st)
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	applyCmd(t, restored, 3, fsm.Command{Op: fsm.OpPut, Key: "x", Value: "from A", ClientID: "client-a", Seq: 1})

	if got, _ := st.Get("x"); got != "from B" {
		t.Errorf("x = %q, want %q; the restored node re-applied a request it had already answered",
			got, "from B")
	}
}

// TestSnapshotPreservesEvictionOrder: the apply counter orders eviction, so
// losing it would make a restored replica evict different clients from its
// peers and drift apart from them.
func TestSnapshotPreservesEvictionOrder(t *testing.T) {
	t.Parallel()

	source := fsm.New(store.New(), fsm.WithMaxSessions(4))
	for i := range 3 {
		applyCmd(t, source, uint64(i+1), fsm.Command{
			Op: fsm.OpPut, Key: "k", Value: "v", ClientID: clientName(i), Seq: 1,
		})
	}

	data, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	restored := fsm.New(store.New(), fsm.WithMaxSessions(4))
	if err := restored.Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Both continue from the same point with the same further commands.
	extend := func(f *fsm.FSM) int {
		for i := 3; i < 10; i++ {
			applyCmd(t, f, uint64(i+1), fsm.Command{
				Op: fsm.OpPut, Key: "k", Value: "v", ClientID: clientName(i), Seq: 1,
			})
		}
		return f.Sessions()
	}

	if a, b := extend(source), extend(restored); a != b {
		t.Errorf("session counts diverged after restore: source %d, restored %d", a, b)
	}
}

func TestRestoreRejectsGarbage(t *testing.T) {
	t.Parallel()

	f := fsm.New(store.New())
	if err := f.Restore([]byte("not a snapshot")); err == nil {
		t.Error("Restore accepted garbage, want an error")
	}
}

// TestRestoreReplacesRatherThanMerges: a snapshot describes complete state, so
// keys deleted before it must not survive the restore.
func TestRestoreReplacesRatherThanMerges(t *testing.T) {
	t.Parallel()

	source := fsm.New(store.New())
	applyCmd(t, source, 1, fsm.Command{Op: fsm.OpPut, Key: "kept", Value: "1"})
	data, err := source.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// The target already holds a key the snapshot does not mention.
	st := store.New()
	if err := st.Put("stale", "should not survive"); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	if err := fsm.New(st).Restore(data); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	if _, err := st.Get("stale"); !errors.Is(err, store.ErrKeyNotFound) {
		t.Errorf("stale key survived the restore: %v; this replica now disagrees with its peers", err)
	}
	if got, _ := st.Get("kept"); got != "1" {
		t.Errorf("kept = %q, want %q", got, "1")
	}
}
