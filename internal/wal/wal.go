// Package wal provides a crash-safe write-ahead log for Raft state.
//
// The file is append-only. Term and vote changes, log appends, and log
// truncations are each written as a record, and the state is reconstructed by
// replaying them in order. Nothing is ever rewritten in place, which is what
// makes a crash at any instant recoverable: the file is always a valid prefix
// of itself plus at most one partial record at the end.
//
// The file grows without bound. Log compaction, which is what stops that, is a
// later milestone and is the reason entries carry their own index rather than
// relying on their position in the file.
package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/vermakmanish001/raft-store/internal/raft"
)

// File format constants.
const (
	// magic identifies the file and catches an attempt to open something that
	// is not a WAL, such as a data directory pointed at the wrong path.
	magic = "RAFTWAL\x00"

	// version guards against a future format change being read by old code,
	// which would otherwise misinterpret records rather than refuse them.
	version uint32 = 1

	headerSize = len(magic) + 4 // magic plus version
	recordSize = 4 + 4          // payload length plus CRC
)

// Record types.
const (
	recHardState uint8 = 1
	recEntry     uint8 = 2
	recTruncate  uint8 = 3
)

// maxRecordBytes bounds a single record. A length field read from a damaged
// file could otherwise ask for an arbitrary allocation.
const maxRecordBytes = 64 << 20 // 64 MiB

// ErrCorrupt reports damage that cannot be safely recovered from.
var ErrCorrupt = errors.New("wal: corrupt")

// WAL is an append-only log of Raft state changes.
//
// It satisfies raft.Storage. Every write is flushed and fsynced before the
// method returns, because a Storage that returns before the data is durable
// defeats the entire purpose.
type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File

	// offset is the end of the last complete, verified record. Appends always
	// start here, so a torn record recovered at open is overwritten rather
	// than left to confuse the next reader.
	offset int64

	// truncatedRecord records that a partial record was discarded at open,
	// which is normal after a crash and worth reporting once.
	truncatedRecord bool
}

var _ raft.Storage = (*WAL)(nil)

// Open opens or creates a write-ahead log at path, recovering whatever a
// previous run left behind.
func Open(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("wal: creating directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: opening %s: %w", path, err)
	}

	w := &WAL{path: path, file: file}
	if err := w.init(); err != nil {
		file.Close()
		return nil, err
	}
	return w, nil
}

// init writes the header for a new file or validates an existing one.
func (w *WAL) init() error {
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat: %w", err)
	}

	if info.Size() == 0 {
		return w.writeHeader()
	}
	if info.Size() < int64(headerSize) {
		return fmt.Errorf("%w: %s is %d bytes, too short to hold a header",
			ErrCorrupt, w.path, info.Size())
	}

	head := make([]byte, headerSize)
	if _, err := w.file.ReadAt(head, 0); err != nil {
		return fmt.Errorf("wal: reading header: %w", err)
	}
	if string(head[:len(magic)]) != magic {
		return fmt.Errorf("%w: %s is not a raft write-ahead log", ErrCorrupt, w.path)
	}
	if got := binary.BigEndian.Uint32(head[len(magic):]); got != version {
		return fmt.Errorf("%w: %s is format version %d, this build understands %d",
			ErrCorrupt, w.path, got, version)
	}

	w.offset = int64(headerSize)
	return nil
}

func (w *WAL) writeHeader() error {
	head := make([]byte, headerSize)
	copy(head, magic)
	binary.BigEndian.PutUint32(head[len(magic):], version)

	if _, err := w.file.WriteAt(head, 0); err != nil {
		return fmt.Errorf("wal: writing header: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: syncing header: %w", err)
	}

	// Sync the directory too. Without it the file's existence is not durable,
	// so a crash can leave a file that was written but never appears.
	if err := syncDir(filepath.Dir(w.path)); err != nil {
		return err
	}

	w.offset = int64(headerSize)
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: opening directory for sync: %w", err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: syncing directory: %w", err)
	}
	return nil
}

// hardStateRecord and truncateRecord are the JSON shapes written to disk.
type hardStateRecord struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

type truncateRecord struct {
	Index uint64 `json:"index"`
}

// SaveHardState durably records the term and vote.
func (w *WAL) SaveHardState(hs raft.HardState) error {
	return w.appendRecord(recHardState, hardStateRecord{
		Term:     uint64(hs.Term),
		VotedFor: string(hs.VotedFor),
	})
}

// Append durably adds entries to the log.
func (w *WAL) Append(entries []raft.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// All the entries are written, then synced once. Syncing per entry would
	// be correct but needlessly slow, and the batch is still atomic from the
	// caller's point of view: a crash partway leaves a torn final record,
	// which recovery discards along with anything after it.
	for _, entry := range entries {
		if err := w.writeLocked(recEntry, entry); err != nil {
			return err
		}
	}
	return w.syncLocked()
}

// TruncateFrom records that the entry at index and everything after it is gone.
//
// The record is appended rather than the file being rewritten. Rewriting in
// place is exactly the operation a crash can leave half-finished, and a log
// whose middle is indeterminate cannot be recovered at all.
func (w *WAL) TruncateFrom(index raft.Index) error {
	if index == 0 {
		return fmt.Errorf("wal: TruncateFrom(0): the log is 1-indexed")
	}
	return w.appendRecord(recTruncate, truncateRecord{Index: uint64(index)})
}

// appendRecord writes one record and syncs.
func (w *WAL) appendRecord(kind uint8, payload any) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writeLocked(kind, payload); err != nil {
		return err
	}
	return w.syncLocked()
}

// writeLocked appends one framed record. The caller holds the lock and syncs.
func (w *WAL) writeLocked(kind uint8, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("wal: encoding record: %w", err)
	}

	// The type byte is inside the checksummed region, so a flipped type bit is
	// caught rather than silently reinterpreting the record.
	framed := make([]byte, 0, 1+len(body))
	framed = append(framed, kind)
	framed = append(framed, body...)

	if len(framed) > maxRecordBytes {
		return fmt.Errorf("wal: record of %d bytes exceeds the %d byte limit",
			len(framed), maxRecordBytes)
	}

	record := make([]byte, recordSize+len(framed))
	binary.BigEndian.PutUint32(record[0:4], uint32(len(framed)))
	binary.BigEndian.PutUint32(record[4:8], crc32.ChecksumIEEE(framed))
	copy(record[recordSize:], framed)

	if _, err := w.file.WriteAt(record, w.offset); err != nil {
		return fmt.Errorf("wal: writing record at offset %d: %w", w.offset, err)
	}
	w.offset += int64(len(record))
	return nil
}

// syncLocked forces everything written so far to durable storage.
func (w *WAL) syncLocked() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: syncing: %w", err)
	}
	return nil
}

// Load replays the file and returns the reconstructed state.
//
// A partial record at the very end is discarded and the file truncated to the
// last complete one. That is the expected outcome of a crash during a write,
// not an error: the record was never acknowledged to anyone, so losing it
// costs nothing. Damage anywhere earlier is reported, because it means a
// record that was acknowledged can no longer be trusted.
func (w *WAL) Load() (raft.HardState, []raft.LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return raft.HardState{}, nil, fmt.Errorf("wal: stat: %w", err)
	}
	size := info.Size()

	if _, err := w.file.Seek(int64(headerSize), io.SeekStart); err != nil {
		return raft.HardState{}, nil, fmt.Errorf("wal: seeking past header: %w", err)
	}

	var (
		reader = bufio.NewReader(w.file)
		state  raft.HardState
		log    []raft.LogEntry
		offset = int64(headerSize)
	)

	for {
		kind, body, consumed, err := readRecord(reader, size-offset)
		if errors.Is(err, errTornRecord) {
			// Expected after a crash: drop the partial tail.
			w.truncatedRecord = true
			break
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return raft.HardState{}, nil, fmt.Errorf("wal: at offset %d: %w", offset, err)
		}

		if err := applyRecord(kind, body, &state, &log); err != nil {
			return raft.HardState{}, nil, fmt.Errorf("wal: at offset %d: %w", offset, err)
		}
		offset += consumed
	}

	// Appends resume after the last good record, overwriting any torn tail.
	w.offset = offset

	if w.truncatedRecord {
		if err := w.file.Truncate(offset); err != nil {
			return raft.HardState{}, nil, fmt.Errorf("wal: truncating partial record: %w", err)
		}
		if err := w.syncLocked(); err != nil {
			return raft.HardState{}, nil, err
		}
	}

	return state, log, nil
}

// errTornRecord marks a record that was being written when the process died.
var errTornRecord = errors.New("wal: incomplete final record")

// readRecord reads one framed record. remaining is how many bytes are left in
// the file, which is what distinguishes a torn tail from real corruption.
func readRecord(r *bufio.Reader, remaining int64) (uint8, []byte, int64, error) {
	if remaining == 0 {
		return 0, nil, 0, io.EOF
	}

	var head [recordSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, errTornRecord
		}
		return 0, nil, 0, err
	}

	length := binary.BigEndian.Uint32(head[0:4])
	want := binary.BigEndian.Uint32(head[4:8])

	if length == 0 || length > maxRecordBytes {
		return 0, nil, 0, fmt.Errorf("%w: implausible record length %d", ErrCorrupt, length)
	}

	total := int64(recordSize) + int64(length)
	if total > remaining {
		// The file ends mid-record: the write was interrupted.
		return 0, nil, 0, errTornRecord
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, 0, errTornRecord
		}
		return 0, nil, 0, err
	}

	if got := crc32.ChecksumIEEE(body); got != want {
		// The record occupies bytes that are all present, so this is not a
		// short write. If it is the final record the bytes may still have
		// landed partially within a sector, which is recoverable; anywhere
		// earlier it is damage to a record that was already acknowledged.
		if total == remaining {
			return 0, nil, 0, errTornRecord
		}
		return 0, nil, 0, fmt.Errorf("%w: checksum mismatch, want %08x got %08x",
			ErrCorrupt, want, got)
	}

	return body[0], body[1:], total, nil
}

// applyRecord folds one record into the state being reconstructed.
func applyRecord(kind uint8, body []byte, state *raft.HardState, log *[]raft.LogEntry) error {
	switch kind {
	case recHardState:
		var rec hardStateRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return fmt.Errorf("%w: decoding hard state: %v", ErrCorrupt, err)
		}
		// The last one wins: each record supersedes the one before it.
		state.Term = raft.Term(rec.Term)
		state.VotedFor = raft.NodeID(rec.VotedFor)
		return nil

	case recEntry:
		var entry raft.LogEntry
		if err := json.Unmarshal(body, &entry); err != nil {
			return fmt.Errorf("%w: decoding log entry: %v", ErrCorrupt, err)
		}
		*log = append(*log, entry)
		return nil

	case recTruncate:
		var rec truncateRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return fmt.Errorf("%w: decoding truncation: %v", ErrCorrupt, err)
		}
		if rec.Index == 0 {
			return fmt.Errorf("%w: truncation to index 0", ErrCorrupt)
		}
		if rec.Index <= uint64(len(*log)) {
			*log = (*log)[:rec.Index-1]
		}
		return nil

	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, kind)
	}
}

// RecoveredPartialRecord reports whether a torn record was discarded when the
// log was opened, which indicates the previous run ended in a crash.
func (w *WAL) RecoveredPartialRecord() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.truncatedRecord
}

// Close flushes and closes the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	if err != nil {
		return fmt.Errorf("wal: closing: %w", err)
	}
	return nil
}
