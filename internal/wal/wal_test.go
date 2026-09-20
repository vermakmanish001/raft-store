package wal_test

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/wal"
)

func tempWAL(t *testing.T) (*wal.WAL, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "raft.wal")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { w.Close() })
	return w, path
}

func TestEmptyLogLoadsCleanly(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	state, entries, err := w.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Term != 0 || state.VotedFor != "" {
		t.Errorf("state = %+v, want zero", state)
	}
	if len(entries) != 0 {
		t.Errorf("got %d entries, want 0", len(entries))
	}
}

// TestSurvivesReopen is the basic durability claim: what was written is still
// there after the process is gone.
func TestSurvivesReopen(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)

	if err := w.SaveHardState(raft.HardState{Term: 7, VotedFor: "n2"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	want := []raft.LogEntry{
		{Term: 5, Index: 1, Type: raft.EntryNormal, Command: []byte(`{"op":"put"}`)},
		{Term: 6, Index: 2, Type: raft.EntryNoOp},
		{Term: 7, Index: 3, Type: raft.EntryNormal, Command: []byte("payload")},
	}
	if err := w.Append(want); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()

	state, entries, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if state.Term != 7 || state.VotedFor != "n2" {
		t.Errorf("state = %+v, want term 7 voted for n2", state)
	}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i := range want {
		if entries[i].Term != want[i].Term || entries[i].Index != want[i].Index ||
			entries[i].Type != want[i].Type || string(entries[i].Command) != string(want[i].Command) {
			t.Errorf("entry %d = %+v, want %+v", i, entries[i], want[i])
		}
	}
}

// TestLastHardStateWins: the file accumulates every term and vote change, and
// only the most recent one describes the node.
func TestLastHardStateWins(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)

	for term := range 20 {
		hs := raft.HardState{Term: raft.Term(term + 1), VotedFor: raft.NodeID("n" + string(rune('0'+term%3)))}
		if err := w.SaveHardState(hs); err != nil {
			t.Fatalf("SaveHardState: %v", err)
		}
	}
	w.Close()

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()

	state, _, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state.Term != 20 {
		t.Errorf("term = %d, want 20", state.Term)
	}
}

func TestTruncationReplays(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)

	if err := w.Append([]raft.LogEntry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3}, {Term: 1, Index: 4},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	// Discard index 3 and everything after it, then append a replacement.
	if err := w.TruncateFrom(3); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	if err := w.Append([]raft.LogEntry{{Term: 2, Index: 3, Command: []byte("replacement")}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()

	_, entries, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("got %d entries, want 3", len(entries))
	}
	if entries[2].Term != 2 || string(entries[2].Command) != "replacement" {
		t.Errorf("entry 3 = %+v, want the replacement from term 2", entries[2])
	}
}

// TestTornFinalRecordIsRecovered is the crash case, and the reason every
// record carries a length and a checksum.
//
// A process killed partway through a write leaves a fragment at the end of the
// file. That fragment was never acknowledged to anyone, so discarding it loses
// nothing, while refusing to start would turn an ordinary crash into an outage
// needing manual repair.
func TestTornFinalRecordIsRecovered(t *testing.T) {
	t.Parallel()

	for _, lost := range []int64{1, 3, 8, 20} {
		t.Run("losing "+string(rune('0'+lost%10))+"ish bytes", func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "raft.wal")

			w, err := wal.Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if err := w.SaveHardState(raft.HardState{Term: 3, VotedFor: "n1"}); err != nil {
				t.Fatalf("SaveHardState: %v", err)
			}
			if err := w.Append([]raft.LogEntry{
				{Term: 3, Index: 1, Command: []byte("durable")},
				{Term: 3, Index: 2, Command: []byte("also durable")},
			}); err != nil {
				t.Fatalf("Append: %v", err)
			}
			good, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}

			// Start another record, then simulate the process dying midway.
			if err := w.Append([]raft.LogEntry{
				{Term: 3, Index: 3, Command: []byte("this write never finished")},
			}); err != nil {
				t.Fatalf("Append: %v", err)
			}
			w.Close()

			full, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat: %v", err)
			}
			cut := full.Size() - lost
			if cut <= good.Size() {
				cut = good.Size() + 1 // still a partial record, just a small one
			}
			if err := os.Truncate(path, cut); err != nil {
				t.Fatalf("truncating to simulate a crash: %v", err)
			}

			reopened, err := wal.Open(path)
			if err != nil {
				t.Fatalf("reopening after a crash: %v", err)
			}
			defer reopened.Close()

			state, entries, err := reopened.Load()
			if err != nil {
				t.Fatalf("Load after a crash: %v; a torn tail must be recoverable", err)
			}

			if !reopened.RecoveredPartialRecord() {
				t.Error("RecoveredPartialRecord() = false, want true")
			}
			if state.Term != 3 || state.VotedFor != "n1" {
				t.Errorf("state = %+v, want term 3 voted for n1; committed state must survive", state)
			}
			if len(entries) != 2 {
				t.Fatalf("got %d entries, want the 2 that were fully written", len(entries))
			}
			if string(entries[0].Command) != "durable" || string(entries[1].Command) != "also durable" {
				t.Errorf("entries = %+v, want both complete ones intact", entries)
			}

			// The recovered log must be usable, not merely readable.
			if err := reopened.Append([]raft.LogEntry{{Term: 4, Index: 3, Command: []byte("after recovery")}}); err != nil {
				t.Fatalf("Append after recovery: %v", err)
			}
			_, after, err := reopened.Load()
			if err != nil {
				t.Fatalf("Load after appending: %v", err)
			}
			if len(after) != 3 || string(after[2].Command) != "after recovery" {
				t.Errorf("after recovery the log is %+v, want 3 entries ending in the new one", after)
			}
		})
	}
}

// TestMidFileCorruptionIsReported: damage before the end of the file hit a
// record that was already acknowledged, so it must not be silently skipped.
func TestMidFileCorruptionIsReported(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "raft.wal")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := w.Append([]raft.LogEntry{
		{Term: 1, Index: 1, Command: []byte("first")},
		{Term: 1, Index: 2, Command: []byte("second")},
		{Term: 1, Index: 3, Command: []byte("third")},
	}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	w.Close()

	// Flip a bit in the middle of the file, well away from the final record.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading: %v", err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("writing: %v", err)
	}

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer reopened.Close()

	if _, _, err := reopened.Load(); !errors.Is(err, wal.ErrCorrupt) {
		t.Errorf("Load = %v, want %v; damage to an acknowledged record must be loud",
			err, wal.ErrCorrupt)
	}
}

func TestRejectsForeignFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		contents []byte
	}{
		{"not a WAL at all", []byte("this is somebody else's file, please do not append to it")},
		{"truncated header", []byte("RAFT")},
		{"wrong version", append([]byte("RAFTWAL\x00"), 0, 0, 0, 99)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "raft.wal")
			if err := os.WriteFile(path, tc.contents, 0o644); err != nil {
				t.Fatalf("writing: %v", err)
			}

			if _, err := wal.Open(path); !errors.Is(err, wal.ErrCorrupt) {
				t.Errorf("Open = %v, want %v; appending to an unrelated file would destroy it",
					err, wal.ErrCorrupt)
			}
		})
	}
}

func TestTruncateFromZeroIsRejected(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	if err := w.TruncateFrom(0); err == nil {
		t.Error("TruncateFrom(0) succeeded, want an error; the log is 1-indexed")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	for range 3 {
		if err := w.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
}

// TestSatisfiesRaftStorage checks the WAL can stand in for the in-memory
// implementation the algorithm is tested against.
func TestSatisfiesRaftStorage(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	var storage raft.Storage = w
	if err := storage.SaveHardState(raft.HardState{Term: 1, VotedFor: "n1"}); err != nil {
		t.Fatalf("SaveHardState: %v", err)
	}
	if err := storage.Append([]raft.LogEntry{{Term: 1, Index: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, entries, err := storage.Load(); err != nil || len(entries) != 1 {
		t.Errorf("Load = %d entries, %v; want 1, nil", len(entries), err)
	}
}

// frameRecord builds a well-formed record so tests can craft files the normal
// API would never produce.
func frameRecord(kind byte, body []byte) []byte {
	framed := append([]byte{kind}, body...)

	out := make([]byte, 8+len(framed))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(framed)))
	binary.BigEndian.PutUint32(out[4:8], crc32.ChecksumIEEE(framed))
	copy(out[8:], framed)
	return out
}

// walWith writes a file containing a valid header followed by raw bytes.
func walWith(t *testing.T, tail []byte) *wal.WAL {
	t.Helper()

	path := filepath.Join(t.TempDir(), "raft.wal")

	// Produce a valid header by letting the package write one.
	w, err := wal.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	w.Close()

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopening to append: %v", err)
	}
	if _, err := f.Write(tail); err != nil {
		t.Fatalf("writing tail: %v", err)
	}
	f.Close()

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

// TestUnknownRecordTypeIsRejected: a record written by a newer build must not
// be skipped, because skipping it would silently drop a state change.
func TestUnknownRecordTypeIsRejected(t *testing.T) {
	t.Parallel()

	w := walWith(t, append(
		frameRecord(1, []byte(`{"term":1,"voted_for":"n1"}`)),
		frameRecord(99, []byte(`{"something":"from the future"}`))...,
	))

	if _, _, err := w.Load(); !errors.Is(err, wal.ErrCorrupt) {
		t.Errorf("Load = %v, want %v", err, wal.ErrCorrupt)
	}
}

func TestImplausibleRecordLengthIsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		length uint32
	}{
		{"zero length", 0},
		{"absurd length", 1 << 30},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// A header claiming a bad length, followed by enough bytes that
			// the file does not simply end early.
			head := make([]byte, 8)
			binary.BigEndian.PutUint32(head[0:4], tc.length)
			binary.BigEndian.PutUint32(head[4:8], 0)

			w := walWith(t, append(head, make([]byte, 64)...))

			if _, _, err := w.Load(); err == nil {
				t.Error("Load succeeded on an implausible record length, want an error")
			}
		})
	}
}

func TestGarbledRecordBodyIsRejected(t *testing.T) {
	t.Parallel()

	// A hard state record whose body is valid JSON of the wrong shape.
	w := walWith(t, append(
		frameRecord(1, []byte(`"not an object"`)),
		frameRecord(1, []byte(`{"term":2,"voted_for":"n1"}`))...,
	))

	if _, _, err := w.Load(); !errors.Is(err, wal.ErrCorrupt) {
		t.Errorf("Load = %v, want %v", err, wal.ErrCorrupt)
	}
}

func TestTruncationToZeroInFileIsRejected(t *testing.T) {
	t.Parallel()

	w := walWith(t, frameRecord(3, []byte(`{"index":0}`)))

	if _, _, err := w.Load(); !errors.Is(err, wal.ErrCorrupt) {
		t.Errorf("Load = %v, want %v", err, wal.ErrCorrupt)
	}
}

func TestEmptyAppendIsANoOp(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)

	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := w.Append(nil); err != nil {
		t.Fatalf("Append(nil): %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	if after.Size() != before.Size() {
		t.Errorf("file grew from %d to %d bytes on an empty append", before.Size(), after.Size())
	}
}

func TestTruncateBeyondTheLogIsANoOp(t *testing.T) {
	t.Parallel()

	w, path := tempWAL(t)

	if err := w.Append([]raft.LogEntry{{Term: 1, Index: 1}}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.TruncateFrom(500); err != nil {
		t.Fatalf("TruncateFrom: %v", err)
	}
	w.Close()

	reopened, err := wal.Open(path)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()

	_, entries, err := reopened.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("got %d entries, want 1; truncating past the end must change nothing", len(entries))
	}
}

// TestLoadIsRepeatable: the driver may load more than once, and replaying the
// same file must not accumulate entries.
func TestLoadIsRepeatable(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	if err := w.Append([]raft.LogEntry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	for i := range 3 {
		_, entries, err := w.Load()
		if err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
		if len(entries) != 2 {
			t.Fatalf("Load %d returned %d entries, want 2", i, len(entries))
		}
	}
}

func TestOpenFailsOnAnUnusablePath(t *testing.T) {
	t.Parallel()

	// A regular file where a directory would have to be.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("writing: %v", err)
	}

	if _, err := wal.Open(filepath.Join(blocker, "nested", "raft.wal")); err == nil {
		t.Error("Open succeeded under a regular file, want an error")
	}
}

func TestOversizedRecordIsRejected(t *testing.T) {
	t.Parallel()

	w, _ := tempWAL(t)

	huge := raft.LogEntry{Term: 1, Index: 1, Command: make([]byte, 65<<20)}
	if err := w.Append([]raft.LogEntry{huge}); err == nil {
		t.Error("Append accepted a record beyond the size limit, want an error")
	}
}
