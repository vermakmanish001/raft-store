package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/vermakmanish001/raft-store/internal/raft"
)

// Path is the endpoint peers post Raft messages to.
const Path = "/raft/message"

// maxMessageBytes caps an inbound message. Raft messages are small, and an
// unbounded read from an untrusted peer is a trivial way to exhaust memory.
const maxMessageBytes = 8 << 20 // 8 MiB

// queueDepth is how many outbound messages may be pending per peer.
//
// When the queue is full, messages are dropped rather than blocking the
// caller. That is safe and intentional: Raft assumes an unreliable network and
// recovers from loss on its own, retrying a dropped AppendEntries on the next
// heartbeat. Blocking instead would stall the consensus loop behind a slow or
// dead peer, converting one unreachable node into a cluster-wide outage.
const queueDepth = 256

// HTTP carries Raft messages between nodes over HTTP.
//
// Sends are asynchronous. Each peer has its own goroutine and queue, so one
// unresponsive node cannot delay traffic to the others, and Send never blocks
// the caller's consensus loop.
type HTTP struct {
	self   raft.NodeID
	peers  map[raft.NodeID]string // node ID to base URL
	client *http.Client
	logger *slog.Logger

	queues  map[raft.NodeID]chan raft.Message
	inbound chan raft.Message

	wg   sync.WaitGroup
	done chan struct{}
	stop sync.Once
}

// NewHTTP returns a transport for the given peers, keyed by node ID with base
// URLs such as "http://127.0.0.1:9001".
func NewHTTP(self raft.NodeID, peers map[raft.NodeID]string, logger *slog.Logger) *HTTP {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	t := &HTTP{
		self:   self,
		peers:  peers,
		logger: logger,
		client: &http.Client{
			// Shorter than an election timeout on purpose. A request that
			// outlived one would pile up behind a partitioned peer and deliver
			// answers to elections that have already been decided.
			Timeout: 500 * time.Millisecond,
		},
		queues:  make(map[raft.NodeID]chan raft.Message, len(peers)),
		inbound: make(chan raft.Message, queueDepth),
		done:    make(chan struct{}),
	}

	for id := range peers {
		t.queues[id] = make(chan raft.Message, queueDepth)
	}
	return t
}

// Start launches one sender goroutine per peer.
func (t *HTTP) Start() {
	for id, queue := range t.queues {
		t.wg.Add(1)
		go t.sendLoop(id, queue)
	}
}

// Send queues a message for delivery. It never blocks.
func (t *HTTP) Send(msg raft.Message) {
	to := raft.Recipient(msg)

	queue, ok := t.queues[to]
	if !ok {
		t.logger.Warn("dropping message for unknown peer", slog.String("peer", string(to)))
		return
	}

	select {
	case queue <- msg:
	case <-t.done:
	default:
		// See queueDepth: dropping is the correct response to a backed-up
		// peer, and Raft will retry.
		t.logger.Debug("outbound queue full, dropping message",
			slog.String("peer", string(to)),
			slog.String("type", fmt.Sprintf("%T", msg)),
		)
	}
}

// Inbound returns the channel of messages received from peers.
func (t *HTTP) Inbound() <-chan raft.Message { return t.inbound }

// sendLoop delivers one peer's queued messages in order.
func (t *HTTP) sendLoop(peer raft.NodeID, queue chan raft.Message) {
	defer t.wg.Done()

	for {
		select {
		case <-t.done:
			return
		case msg := <-queue:
			t.post(peer, msg)
		}
	}
}

// post delivers a single message, logging failures without retrying.
//
// Retrying here would duplicate what Raft already does, and worse, would
// deliver a stale message after the term it belonged to had passed.
func (t *HTTP) post(peer raft.NodeID, msg raft.Message) {
	body, err := Encode(msg)
	if err != nil {
		t.logger.Error("encoding message failed",
			slog.String("peer", string(peer)), slog.Any("error", err))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.client.Timeout)
	defer cancel()

	url := t.peers[peer] + Path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.logger.Error("building request failed",
			slog.String("peer", string(peer)), slog.Any("error", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		// A peer being unreachable is an ordinary condition in a distributed
		// system, not an error worth alarming on. It is logged at debug so a
		// rolling restart does not fill the log with noise.
		t.logger.Debug("delivery failed",
			slog.String("peer", string(peer)), slog.Any("error", err))
		return
	}
	defer resp.Body.Close()

	// The body is drained so the connection can be reused rather than torn
	// down and redialed on every message.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusAccepted {
		t.logger.Debug("peer rejected message",
			slog.String("peer", string(peer)), slog.Int("status", resp.StatusCode))
	}
}

// Handler serves messages posted by peers. The caller mounts it at Path.
//
// It is returned unmounted so the caller can route peer traffic separately
// from client traffic. That matters in practice: heartbeats arrive many times
// a second, and running them through the client request logger would bury
// every real request under them.
func (t *HTTP) Handler() http.Handler {
	return http.HandlerFunc(t.receive)
}

func (t *HTTP) receive(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMessageBytes))
	if err != nil {
		http.Error(w, "could not read message", http.StatusBadRequest)
		return
	}

	msg, err := Decode(body)
	if err != nil {
		// Malformed input from a peer is rejected rather than tolerated. A
		// zero-valued message would carry term 0 and could be mistaken for a
		// legitimate RPC.
		t.logger.Warn("rejecting malformed message", slog.Any("error", err))
		http.Error(w, "malformed message", http.StatusBadRequest)
		return
	}

	select {
	case t.inbound <- msg:
		// Accepted, not OK: the message has been queued for the consensus
		// loop, which has not processed it yet. Raft replies with its own
		// message rather than in this response body.
		w.WriteHeader(http.StatusAccepted)
	case <-t.done:
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
	default:
		// The consensus loop is behind. Dropping is safe for the same reason
		// it is on the outbound side.
		t.logger.Debug("inbound queue full, dropping message")
		w.WriteHeader(http.StatusAccepted)
	}
}

// Close stops the sender goroutines and waits for them to finish.
func (t *HTTP) Close() error {
	t.stop.Do(func() { close(t.done) })
	t.wg.Wait()
	t.client.CloseIdleConnections()
	return nil
}

// ErrNoPeers reports a cluster configuration with nothing to talk to. It is
// not returned by this package but is shared by callers parsing peer lists.
var ErrNoPeers = errors.New("transport: no peers configured")
