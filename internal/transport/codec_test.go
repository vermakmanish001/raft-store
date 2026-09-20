package transport_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/transport"
)

// TestCodecRoundTrip checks that every field survives the wire. A field
// silently dropped here would not fail any test in the raft package, because
// that package never serializes anything, but would break consensus in a real
// cluster.
func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  raft.Message
	}{
		{
			name: "request vote",
			msg: raft.RequestVote{
				Header:       raft.Header{From: "n1", To: "n2", Term: 7},
				LastLogIndex: 42,
				LastLogTerm:  6,
			},
		},
		{
			name: "request vote response",
			msg: raft.RequestVoteResponse{
				Header:      raft.Header{From: "n2", To: "n1", Term: 7},
				VoteGranted: true,
			},
		},
		{
			name: "heartbeat",
			msg: raft.AppendEntries{
				Header:       raft.Header{From: "n1", To: "n2", Term: 7},
				PrevLogIndex: 42,
				PrevLogTerm:  6,
				LeaderCommit: 40,
			},
		},
		{
			name: "append with entries",
			msg: raft.AppendEntries{
				Header:       raft.Header{From: "n1", To: "n2", Term: 7},
				PrevLogIndex: 2,
				PrevLogTerm:  6,
				LeaderCommit: 2,
				Entries: []raft.LogEntry{
					{Term: 7, Index: 3, Type: raft.EntryNormal, Command: []byte(`{"op":"put"}`)},
					{Term: 7, Index: 4, Type: raft.EntryNoOp},
				},
			},
		},
		{
			name: "install snapshot",
			msg: raft.InstallSnapshot{
				Header:            raft.Header{From: "n1", To: "n2", Term: 9},
				LastIncludedIndex: 500,
				LastIncludedTerm:  8,
				Data:              []byte(`{"kv":{"a":"1"},"sessions":{}}`),
			},
		},
		{
			name: "install snapshot response",
			msg: raft.InstallSnapshotResponse{
				Header:     raft.Header{From: "n2", To: "n1", Term: 9},
				MatchIndex: 500,
			},
		},
		{
			name: "append response with conflict hint",
			msg: raft.AppendEntriesResponse{
				Header:        raft.Header{From: "n2", To: "n1", Term: 7},
				Success:       false,
				ConflictIndex: 12,
				ConflictTerm:  5,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := transport.Encode(tc.msg)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			got, err := transport.Decode(data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.msg) {
				t.Errorf("round trip changed the message:\n got %#v\nwant %#v", got, tc.msg)
			}
		})
	}
}

func TestDecodeRejectsBadInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"not json", "hello"},
		{"unknown type tag", `{"type":"frobnicate","body":{}}`},
		{"missing type", `{"body":{}}`},
		{"body of the wrong shape", `{"type":"request_vote","body":"a string"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			msg, err := transport.Decode([]byte(tc.input))
			if err == nil {
				t.Fatalf("Decode(%q) = %#v, want an error", tc.input, msg)
			}
			if msg != nil {
				t.Errorf("Decode returned %#v alongside an error; a zero message could be "+
					"mistaken for a legitimate term 0 RPC", msg)
			}
		})
	}
}

func TestEncodeRejectsUnknownType(t *testing.T) {
	t.Parallel()

	if _, err := transport.Encode(unknownMessage{}); !errors.Is(err, transport.ErrUnknownMessage) {
		t.Errorf("Encode error = %v, want %v", err, transport.ErrUnknownMessage)
	}
}

// unknownMessage satisfies raft.Message only by embedding Header, which is how
// the interface stays closed to the raft package.
type unknownMessage struct{ raft.Header }
