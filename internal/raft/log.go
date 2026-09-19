package raft

// This file owns every translation between a Raft log index and a position in
// the backing slice. The log is 1-indexed while the slice is 0-indexed, and
// log compaction will later make the slice a suffix of the logical log rather
// than the whole of it. Confining that arithmetic to one file means compaction
// changes this file alone, instead of every off-by-one across the package.

// lastLogIndex returns the index of the final entry, or 0 for an empty log.
// Zero is a safe sentinel because the log is 1-indexed, so no real entry can
// occupy it.
func (n *Node) lastLogIndex() Index {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// lastLogTerm returns the term of the final entry, or 0 for an empty log.
func (n *Node) lastLogTerm() Term {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

// entryAt returns the entry stored at a log index.
func (n *Node) entryAt(index Index) (LogEntry, bool) {
	if index == 0 || index > n.lastLogIndex() {
		return LogEntry{}, false
	}
	return n.log[index-1], true
}

// termAt returns the term of the entry at a log index, or 0 if absent.
func (n *Node) termAt(index Index) Term {
	entry, ok := n.entryAt(index)
	if !ok {
		return 0
	}
	return entry.Term
}

// entriesFrom returns all entries at or after index, as a copy.
//
// The copy is not defensive pedantry. The returned slice is handed to a
// transport that may hold it while the leader continues appending, and
// returning a subslice would let an append that grows the backing array
// silently change what is in flight.
func (n *Node) entriesFrom(index Index) []LogEntry {
	if index == 0 {
		index = 1
	}
	if index > n.lastLogIndex() {
		return nil
	}
	tail := n.log[index-1:]
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
	n.log = append(n.log, entry)
	return entry
}

// truncateFrom discards the entry at index and everything after it.
//
// Deleting a suffix is safe only because of the conditions under which it is
// called: the entries being discarded conflict with the leader's log, and an
// entry that conflicts with a leader cannot have been committed. A committed
// entry is present on a majority, and the election restriction means any
// leader holds every committed entry, so a leader would never send a
// conflicting entry in its place.
func (n *Node) truncateFrom(index Index) {
	if index == 0 || index > n.lastLogIndex() {
		return
	}
	n.log = n.log[:index-1]
}

// logMatches reports whether the log contains an entry at index with the given
// term, the precondition for accepting entries that follow it (Section 5.3).
func (n *Node) logMatches(index Index, term Term) bool {
	// Index 0 describes the position before the first entry, which every log
	// agrees on trivially.
	if index == 0 {
		return true
	}
	entry, ok := n.entryAt(index)
	return ok && entry.Term == term
}
