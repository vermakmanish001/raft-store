package raft

import "fmt"

// This file owns every translation between a Raft log index and a position in
// the backing slice.
//
// Compaction makes that translation non-trivial. After a snapshot at index S,
// the slice holds only entries S+1 and later, so the entry at logical index i
// lives at slice position i-S-1 rather than i-1. Confining that arithmetic
// here is why compaction changes one file instead of producing an off-by-one
// in every function that touches the log.
//
// Two indexes therefore need care everywhere below. snapshotIndex is the last
// entry the snapshot covers and is no longer in the slice, while lastLogIndex
// is the end of the log whether or not the slice is empty.

// lastLogIndex returns the index of the final entry.
//
// With an empty slice this is the snapshot's index rather than zero: the
// entries still exist logically, they are simply folded into the snapshot.
// Returning zero would tell a leader this node holds nothing and restart
// replication from the beginning of time.
func (n *Node) lastLogIndex() Index {
	if len(n.log) == 0 {
		return n.snapshotIndex
	}
	return n.log[len(n.log)-1].Index
}

// lastLogTerm returns the term of the final entry.
func (n *Node) lastLogTerm() Term {
	if len(n.log) == 0 {
		return n.snapshotTerm
	}
	return n.log[len(n.log)-1].Term
}

// entryAt returns the entry stored at a log index.
//
// It reports false for an index the snapshot swallowed, which is not an error:
// the caller's job is to send a snapshot instead of entries.
func (n *Node) entryAt(index Index) (LogEntry, bool) {
	if index <= n.snapshotIndex || index > n.lastLogIndex() {
		return LogEntry{}, false
	}
	return n.log[index-n.snapshotIndex-1], true
}

// termAt returns the term of the entry at a log index, or 0 if unavailable.
//
// The snapshot's own index answers from snapshot metadata. That is precisely
// why the metadata is kept: it lets a compacted log still satisfy the
// consistency check at the boundary, which is the position a leader is most
// likely to ask about right after compaction.
func (n *Node) termAt(index Index) Term {
	if index == n.snapshotIndex {
		return n.snapshotTerm
	}
	entry, ok := n.entryAt(index)
	if !ok {
		return 0
	}
	return entry.Term
}

// entriesFrom returns all entries at or after index, as a copy.
//
// It returns nil for an index the snapshot covers. A leader must notice that
// case and send a snapshot rather than treating the empty result as "this
// follower is already up to date".
//
// The copy is not defensive pedantry. The returned slice is handed to a
// transport that may hold it while the leader continues appending, and
// returning a subslice would let an append that grows the backing array
// silently change what is in flight.
func (n *Node) entriesFrom(index Index) []LogEntry {
	// Index 0 names the position before the first entry, so it means the same
	// as starting at 1. Normalizing here keeps the snapshot comparison below
	// from reading "0 is covered by an empty snapshot" as a reason to
	// withhold the whole log.
	if index == 0 {
		index = 1
	}
	if index <= n.snapshotIndex {
		return nil
	}
	if index > n.lastLogIndex() {
		return nil
	}
	tail := n.log[index-n.snapshotIndex-1:]
	return append([]LogEntry(nil), tail...)
}

// appendEntry adds one entry to the end of the log, assigning it the next
// index. Only a leader calls this; followers append what they are given.
func (n *Node) appendEntry(term Term, typ EntryType, command []byte) LogEntry {
	entry := LogEntry{
		Term:    term,
		Index:   n.lastLogIndex() + 1,
		Type:    typ,
		Command: command,
	}
	n.appendPersisted([]LogEntry{entry})
	return entry
}

// appendPersisted adds entries to the in-memory log and to durable storage.
//
// Both happen here so the two can never disagree. An entry held in memory but
// not on disk would vanish on restart after this node had already told a
// leader it held it, and a leader counting that acknowledgement toward a
// majority would consider an entry committed that a restart could erase.
func (n *Node) appendPersisted(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}

	n.log = append(n.log, entries...)

	if n.fatal != nil {
		return
	}
	if err := n.storage.Append(entries); err != nil {
		n.setFatal(fmt.Errorf("raft: persisting %d entries at index %d: %w",
			len(entries), entries[0].Index, err))
	}
}

// truncateFrom discards the entry at index and everything after it.
//
// Deleting a suffix is safe only because of the conditions under which it is
// called: the entries being discarded conflict with the leader's log, and an
// entry that conflicts with a leader cannot have been committed. A committed
// entry is present on a majority, and the election restriction means any
// leader holds every committed entry, so a leader would never send a
// conflicting entry in its place.
//
// It follows that an index inside the snapshot can never be a legitimate
// target: the snapshot only ever covers committed entries.
func (n *Node) truncateFrom(index Index) {
	if index <= n.snapshotIndex || index > n.lastLogIndex() {
		return
	}
	n.log = n.log[:index-n.snapshotIndex-1]

	if n.fatal != nil {
		return
	}
	if err := n.storage.TruncateFrom(index); err != nil {
		n.setFatal(fmt.Errorf("raft: truncating log from index %d: %w", index, err))
	}
}

// logMatches reports whether the log contains an entry at index with the given
// term, the precondition for accepting entries that follow it (Section 5.3).
func (n *Node) logMatches(index Index, term Term) bool {
	// Index 0 describes the position before the first entry, which every log
	// agrees on trivially.
	if index == 0 {
		return true
	}

	// The snapshot boundary is answered from metadata rather than an entry.
	if index == n.snapshotIndex {
		return term == n.snapshotTerm
	}

	// Below the boundary the entry is gone, but it was committed before being
	// compacted, and a committed entry is identical on every node that holds
	// it. Reporting a match is therefore correct rather than optimistic: no
	// leader could legitimately hold a different entry there.
	if index < n.snapshotIndex {
		return true
	}

	entry, ok := n.entryAt(index)
	return ok && entry.Term == term
}
