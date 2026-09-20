// Package transport carries Raft RPCs between nodes.
//
// The raft package produces and consumes messages but never sends them; this
// package is the only place that touches a socket on consensus's behalf. That
// split is what lets the algorithm be tested against an in-memory harness with
// no network at all.
package transport

import (
	"encoding/json"
	"fmt"

	"github.com/vermakmanish001/raft-store/internal/raft"
)

// Message type tags used on the wire.
//
// They are explicit strings rather than Go type names so that renaming a type
// cannot silently change the protocol. A node speaking to a peer running a
// different build must agree on these exactly.
const (
	tagRequestVote          = "request_vote"
	tagRequestVoteResponse  = "request_vote_response"
	tagAppendEntries        = "append_entries"
	tagAppendEntriesRespons = "append_entries_response"
	tagInstallSnapshot      = "install_snapshot"
	tagInstallSnapshotResp  = "install_snapshot_response"
)

// ErrUnknownMessage reports a wire message this build does not recognize.
var ErrUnknownMessage = fmt.Errorf("transport: unknown message type")

// envelope carries a tagged message. The body stays raw until the tag has been
// read, so decoding never has to guess at a shape.
type envelope struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

// Encode serializes a Raft message for transmission.
func Encode(msg raft.Message) ([]byte, error) {
	var tag string
	switch msg.(type) {
	case raft.RequestVote:
		tag = tagRequestVote
	case raft.RequestVoteResponse:
		tag = tagRequestVoteResponse
	case raft.AppendEntries:
		tag = tagAppendEntries
	case raft.AppendEntriesResponse:
		tag = tagAppendEntriesRespons
	case raft.InstallSnapshot:
		tag = tagInstallSnapshot
	case raft.InstallSnapshotResponse:
		tag = tagInstallSnapshotResp
	default:
		return nil, fmt.Errorf("%w: %T", ErrUnknownMessage, msg)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("transport: encoding %s: %w", tag, err)
	}

	data, err := json.Marshal(envelope{Type: tag, Body: body})
	if err != nil {
		return nil, fmt.Errorf("transport: encoding envelope: %w", err)
	}
	return data, nil
}

// Decode parses a Raft message received from a peer.
//
// Every message on this path arrives from the network and is therefore
// untrusted input. A malformed or unknown message yields an error rather than
// a zero-valued message, because a zero value would carry term 0 and could be
// mistaken for a legitimate RPC from a node at the start of time.
func Decode(data []byte) (raft.Message, error) {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("transport: decoding envelope: %w", err)
	}

	switch env.Type {
	case tagRequestVote:
		return decodeInto[raft.RequestVote](env)
	case tagRequestVoteResponse:
		return decodeInto[raft.RequestVoteResponse](env)
	case tagAppendEntries:
		return decodeInto[raft.AppendEntries](env)
	case tagAppendEntriesRespons:
		return decodeInto[raft.AppendEntriesResponse](env)
	case tagInstallSnapshot:
		return decodeInto[raft.InstallSnapshot](env)
	case tagInstallSnapshotResp:
		return decodeInto[raft.InstallSnapshotResponse](env)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownMessage, env.Type)
	}
}

func decodeInto[T raft.Message](env envelope) (raft.Message, error) {
	var msg T
	if err := json.Unmarshal(env.Body, &msg); err != nil {
		return nil, fmt.Errorf("transport: decoding %s: %w", env.Type, err)
	}
	return msg, nil
}
