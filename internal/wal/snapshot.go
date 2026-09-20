package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/vermakmanish001/raft-store/internal/raft"
)

// Snapshot file layout. Fixed-width fields rather than JSON, so the data is
// stored as-is instead of inflated by base64, and so a truncated file is
// detected by size before anything is parsed.
//
//	magic(8) version(4) index(8) term(8) length(8) crc32(4) data
const snapshotHeaderSize = 8 + 4 + 8 + 8 + 8 + 4

const snapshotMagic = "RAFTSNAP"

// snapshotPath returns the snapshot file beside the log.
func (w *WAL) snapshotPath() string {
	return w.path + ".snapshot"
}

// SaveSnapshot durably records a snapshot and discards the log entries it
// covers.
//
// The order is what makes this crash-safe, and it is the opposite of the
// intuitive one. The snapshot is made durable first, and only then is the log
// rewritten. A crash in between leaves both the snapshot and the full log,
// which is merely redundant: Load discards entries the snapshot already covers.
// A crash in the other order would leave neither the entries nor the snapshot
// replacing them, and the state they described would exist nowhere.
func (w *WAL) SaveSnapshot(meta raft.SnapshotMeta, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if meta.Index < w.snapMeta.Index {
		return fmt.Errorf("wal: snapshot at index %d is older than the stored one at %d",
			meta.Index, w.snapMeta.Index)
	}

	if err := w.writeSnapshotFile(meta, data); err != nil {
		return err
	}

	w.snapMeta = meta

	// Reclaiming the log is an optimization, not a correctness requirement, so
	// a failure here is reported without losing the snapshot that already
	// landed.
	return w.rewriteLogLocked(meta.Index)
}

// writeSnapshotFile writes the snapshot through a temporary file and renames
// it into place.
//
// The rename is what makes the replacement atomic. Writing over the existing
// snapshot would leave a window where a crash finds a file that is half the
// old snapshot and half the new one, belonging to no log position at all.
func (w *WAL) writeSnapshotFile(meta raft.SnapshotMeta, data []byte) error {
	final := w.snapshotPath()
	tmp := final + ".tmp"

	buf := make([]byte, snapshotHeaderSize+len(data))
	copy(buf[0:8], snapshotMagic)
	binary.BigEndian.PutUint32(buf[8:12], version)
	binary.BigEndian.PutUint64(buf[12:20], uint64(meta.Index))
	binary.BigEndian.PutUint64(buf[20:28], uint64(meta.Term))
	binary.BigEndian.PutUint64(buf[28:36], uint64(len(data)))
	binary.BigEndian.PutUint32(buf[36:40], crc32.ChecksumIEEE(data))
	copy(buf[snapshotHeaderSize:], data)

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: creating snapshot temp file: %w", err)
	}

	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("wal: writing snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("wal: syncing snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("wal: closing snapshot: %w", err)
	}

	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("wal: installing snapshot: %w", err)
	}

	// The rename itself must be durable, or a crash can leave the directory
	// entry pointing at the old file despite the data being written.
	return syncDir(filepath.Dir(final))
}

// LoadSnapshot returns the stored snapshot, or a zero meta if none exists.
func (w *WAL) LoadSnapshot() (raft.SnapshotMeta, []byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.loadSnapshotLocked()
}

func (w *WAL) loadSnapshotLocked() (raft.SnapshotMeta, []byte, error) {
	raw, err := os.ReadFile(w.snapshotPath())
	if errors.Is(err, os.ErrNotExist) {
		return raft.SnapshotMeta{}, nil, nil // never snapshotted
	}
	if err != nil {
		return raft.SnapshotMeta{}, nil, fmt.Errorf("wal: reading snapshot: %w", err)
	}

	if len(raw) < snapshotHeaderSize {
		return raft.SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot is %d bytes, too short for a header",
			ErrCorrupt, len(raw))
	}
	if string(raw[0:8]) != snapshotMagic {
		return raft.SnapshotMeta{}, nil, fmt.Errorf("%w: not a raft snapshot", ErrCorrupt)
	}
	if got := binary.BigEndian.Uint32(raw[8:12]); got != version {
		return raft.SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot format version %d, this build understands %d",
			ErrCorrupt, got, version)
	}

	meta := raft.SnapshotMeta{
		Index: raft.Index(binary.BigEndian.Uint64(raw[12:20])),
		Term:  raft.Term(binary.BigEndian.Uint64(raw[20:28])),
	}
	length := binary.BigEndian.Uint64(raw[28:36])
	want := binary.BigEndian.Uint32(raw[36:40])

	body := raw[snapshotHeaderSize:]
	if uint64(len(body)) != length {
		// A snapshot is installed by rename, so a short file means the write
		// never completed and the rename never happened, or the file was
		// damaged afterward. Either way it cannot be trusted.
		return raft.SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot claims %d bytes but holds %d",
			ErrCorrupt, length, len(body))
	}
	if got := crc32.ChecksumIEEE(body); got != want {
		return raft.SnapshotMeta{}, nil, fmt.Errorf("%w: snapshot checksum mismatch, want %08x got %08x",
			ErrCorrupt, want, got)
	}

	return meta, body, nil
}

// rewriteLogLocked replaces the log with only the entries after index.
//
// The new log is built in a temporary file and renamed over the old one, for
// the same reason the snapshot is: a partially rewritten log in place would be
// unrecoverable, while a failed rename simply leaves the previous log, which
// is still correct because it is a superset of what is needed.
func (w *WAL) rewriteLogLocked(index raft.Index) error {
	_, entries, err := w.replayLocked()
	if err != nil {
		return err
	}

	kept := entries[:0]
	for _, entry := range entries {
		if entry.Index > index {
			kept = append(kept, entry)
		}
	}

	tmp := w.path + ".rewrite"
	rewritten, err := Open(tmp)
	if err != nil {
		return fmt.Errorf("wal: opening rewrite target: %w", err)
	}

	// The hard state is carried across. It lives in the log rather than the
	// snapshot, so dropping it here would leave a restarted node with no
	// record of its term or vote.
	if err := rewritten.SaveHardState(w.state); err != nil {
		rewritten.Close()
		os.Remove(tmp)
		return err
	}
	if err := rewritten.Append(kept); err != nil {
		rewritten.Close()
		os.Remove(tmp)
		return err
	}
	if err := rewritten.Close(); err != nil {
		os.Remove(tmp)
		return err
	}

	// Swap the file underneath this handle, then reopen so subsequent appends
	// land in the new one.
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: closing log before rewrite: %w", err)
	}
	if err := os.Rename(tmp, w.path); err != nil {
		return fmt.Errorf("wal: installing rewritten log: %w", err)
	}
	if err := syncDir(filepath.Dir(w.path)); err != nil {
		return err
	}

	file, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopening rewritten log: %w", err)
	}
	w.file = file

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat after rewrite: %w", err)
	}
	w.offset = info.Size()
	return nil
}

// discardCompacted drops entries the snapshot already covers.
//
// This runs on every load, which is what makes the rewrite an optimization
// rather than a correctness requirement: a crash between installing the
// snapshot and rewriting the log leaves duplicates that are filtered here.
func discardCompacted(entries []raft.LogEntry, index raft.Index) []raft.LogEntry {
	kept := entries[:0]
	for _, entry := range entries {
		if entry.Index > index {
			kept = append(kept, entry)
		}
	}
	return kept
}

var _ io.Closer = (*WAL)(nil)
