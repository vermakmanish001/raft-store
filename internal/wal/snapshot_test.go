package wal_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/wal"
)

func seedLog(t *testing.T, w *wal.WAL, count int) {
	t.Helper()

	entries := make([]raft.LogEntry, count)
	for i := range count {
		entries[i] = raft.LogEntry{
			Term: 1, Index: raft.Index(i + 1),
			Type: raft.EntryNormal, Command: []byte("command"),
		}
	}
	if err := w.Append(entries); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)

	if err := w.SaveHardState(raft.HardState{Term: 4, VotedFor: "n2"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	seedLog(t, w, 10)

	payload := []byte(`{"kv":{"a":"1","b":"2"},"sessions":{}}`)
	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 6, Term: 1}, payload); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	w.Close()

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()

	meta, data, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if meta.Index != 6 || meta.Term != 1 {
		t.Errorf("meta = %+v, want index 6 term 1", meta)
	}
	if string(data) != string(payload) {
		t.Errorf("data = %q, want %q", data, payload)
	}

	state, entries, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Term != 4 || state.VotedFor != "n2" {
		t.Errorf("state = %+v, want term 4 voted for n2; the rewrite must carry it across", state)
	}
	if len(entries) != 4 {
		t.Fatalf("got %d entries, want the 4 after the snapshot", len(entries))
	}
	if entries[0].Index != 7 {
		t.Errorf("first entry index = %d, want 7", entries[0].Index)
	}
}

// TestSnapshotReclaimsLogSpace is the point of compaction.
func TestSnapshotReclaimsLogSpace(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)
	seedLog(t, w, 200)

	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 190, Term: 1}, []byte("small")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if after.Size() >= before.Size() {
		t.Errorf("log is %d bytes after compaction, was %d; it should have shrunk",
			after.Size(), before.Size())
	}

	// The remaining entries must still be intact and usable.
	_, entries, err := w.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 10 {
		t.Errorf("got %d entries, want 10", len(entries))
	}
	if err := w.Append([]raft.LogEntry{{Term: 2, Index: 201}}); err != nil {
		t.Fatalf("Append after compaction: %v", err)
	}
	if _, entries, _ := w.Load(); len(entries) != 11 {
		t.Errorf("got %d entries after appending, want 11", len(entries))
	}
}

// TestCrashBetweenSnapshotAndRewrite covers the window the ordering exists to
// make safe.
//
// The snapshot is made durable before the log is reclaimed, so a crash in
// between leaves both. That is merely redundant, not broken: Load discards
// entries the snapshot already covers. Doing it in the other order would leave
// neither.
func TestCrashBetweenSnapshotAndRewrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "raft.wal")

	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.SaveHardState(raft.HardState{Term: 2, VotedFor: "n1"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	seedLog(t, w, 10)

	// Capture the full log as it stands before compaction.
	fullLog, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}

	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 6, Term: 1}, []byte("state at 6")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	w.Close()

	// Simulate the crash: the snapshot landed, the log rewrite did not.
	if err := os.WriteFile(path, fullLog, 0o644); err != nil {
		t.Fatalf("restoring the pre-compaction log: %v", err)
	}

	recovered, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer recovered.Close()

	state, entries, err := recovered.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if state.Term != 2 || state.VotedFor != "n1" {
		t.Errorf("state = %+v, want term 2 voted for n1", state)
	}
	if len(entries) != 4 {
		t.Fatalf("got %d entries, want 4; entries the snapshot covers must be filtered on load", len(entries))
	}
	if entries[0].Index != 7 {
		t.Errorf("first entry index = %d, want 7", entries[0].Index)
	}

	meta, data, err := recovered.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if meta.Index != 6 || string(data) != "state at 6" {
		t.Errorf("snapshot = %+v %q, want index 6 with its data", meta, data)
	}
}

func TestNoSnapshotIsNotAnError(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	meta, data, err := w.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot on a fresh log: %v", err)
	}
	if !meta.IsEmpty() {
		t.Errorf("meta = %+v, want empty", meta)
	}
	if data != nil {
		t.Errorf("data = %q, want nil", data)
	}
}

func TestOlderSnapshotIsRejected(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)
	seedLog(t, w, 20)

	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 15, Term: 1}, []byte("newer")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 5, Term: 1}, []byte("older")); err == nil {
		t.Error("an older snapshot was accepted; it would discard entries the newer one covers")
	}

	meta, data, _ := w.LoadSnapshot()
	if meta.Index != 15 || string(data) != "newer" {
		t.Errorf("snapshot = %+v %q, want the newer one preserved", meta, data)
	}
}

func TestCorruptSnapshotIsDetected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{
			name: "flipped byte in the data",
			damage: func(t *testing.T, path string) {
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("reading: %v", err)
				}
				raw[len(raw)-1] ^= 0xFF
				if err := os.WriteFile(path, raw, 0o644); err != nil {
					t.Fatalf("writing: %v", err)
				}
			},
		},
		{
			name: "truncated file",
			damage: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("stat: %v", err)
				}
				if err := os.Truncate(path, info.Size()-3); err != nil {
					t.Fatalf("truncating: %v", err)
				}
			},
		},
		{
			name: "not a snapshot at all",
			damage: func(t *testing.T, path string) {
				if err := os.WriteFile(path, []byte("somebody else's file entirely"), 0o644); err != nil {
					t.Fatalf("writing: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "raft.wal")

			w, err := wal.Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			seedLog(t, w, 5)
			if err := w.SaveSnapshot(raft.SnapshotMeta{Index: 3, Term: 1}, []byte("state")); err != nil {
				t.Fatalf("SaveSnapshot: %v", err)
			}
			w.Close()

			tc.damage(t, path+".snapshot")

			reopened, err := wal.Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer reopened.Close()

			if _, _, err := reopened.LoadSnapshot(); !errors.Is(err, wal.ErrCorrupt) {
				t.Errorf("LoadSnapshot = %v, want %v; a damaged snapshot must not be silently used",
					err, wal.ErrCorrupt)
			}
		})
	}
}

// TestSnapshotSatisfiesRaftStorage checks the WAL can back a compacting node.
func TestSnapshotSatisfiesRaftStorage(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	var storage raft.Storage = w
	if err := storage.Append([]raft.LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := storage.SaveSnapshot(raft.SnapshotMeta{Index: 1, Term: 1}, []byte("x")); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	_, entries, err := storage.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 1 || entries[0].Index != 2 {
		t.Errorf("entries = %+v, want only index 2", entries)
	}
}
