package transport_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/transport"
)

// TestHTTPDelivery sends a message between two transports over a real socket.
func TestHTTPDelivery(t *testing.T) {
	t.Parallel()

	// n2 listens; its handler feeds its inbound channel.
	receiver := transport.NewHTTP("n2", nil, nil)
	defer receiver.Close()

	mux := http.NewServeMux()
	mux.Handle("POST "+transport.Path, receiver.Handler())
	srv := httptest.NewServer(mux)
	defer srv.Close()

	sender := transport.NewHTTP("n1", map[raft.NodeID]string{"n2": srv.URL}, nil)
	sender.Start()
	defer sender.Close()

	want := raft.AppendEntries{
		Header:       raft.Header{From: "n1", To: "n2", Term: 3},
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		Entries: []raft.LogEntry{
			{Term: 3, Index: 6, Type: raft.EntryNormal, Command: []byte("hello")},
		},
		LeaderCommit: 5,
	}
	sender.Send(want)

	select {
	case got := <-receiver.Inbound():
		ae, ok := got.(raft.AppendEntries)
		if !ok {
			t.Fatalf("got %T, want AppendEntries", got)
		}
		if ae.Term != want.Term || ae.PrevLogIndex != want.PrevLogIndex {
			t.Errorf("header changed in transit: got %+v", ae)
		}
		if len(ae.Entries) != 1 || string(ae.Entries[0].Command) != "hello" {
			t.Errorf("entries changed in transit: got %+v", ae.Entries)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("message never arrived")
	}
}

// TestSendToUnknownPeerIsDropped: a message for a node not in the peer map must
// not panic or block, since membership can legitimately lag behind.
func TestSendToUnknownPeerIsDropped(t *testing.T) {
	t.Parallel()

	tr := transport.NewHTTP("n1", map[raft.NodeID]string{}, nil)
	tr.Start()
	defer tr.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		tr.Send(raft.RequestVote{Header: raft.Header{From: "n1", To: "ghost", Term: 1}})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on an unknown peer; it must never block")
	}
}

// TestSendToUnreachablePeerDoesNotBlock is the property that keeps one dead
// node from stalling the whole consensus loop.
func TestSendToUnreachablePeerDoesNotBlock(t *testing.T) {
	t.Parallel()

	// Port 1 on loopback refuses connections immediately.
	tr := transport.NewHTTP("n1", map[raft.NodeID]string{"n2": "http://127.0.0.1:1"}, nil)
	tr.Start()
	defer tr.Close()

	start := time.Now()
	for range 100 {
		tr.Send(raft.RequestVote{Header: raft.Header{From: "n1", To: "n2", Term: 1}})
	}

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("100 sends to a dead peer took %v; sends must be asynchronous", elapsed)
	}
}

func TestReceiveRejectsMalformedMessages(t *testing.T) {
	t.Parallel()

	tr := transport.NewHTTP("n1", nil, nil)
	defer tr.Close()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, transport.Path, strings.NewReader("not a message"))
	tr.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	select {
	case msg := <-tr.Inbound():
		t.Errorf("malformed input produced an inbound message: %#v", msg)
	default:
	}
}

// TestCloseIsIdempotent: shutdown paths call Close from more than one place.
func TestCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	tr := transport.NewHTTP("n1", map[raft.NodeID]string{"n2": "http://127.0.0.1:1"}, nil)
	tr.Start()

	for range 3 {
		if err := tr.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}
}
