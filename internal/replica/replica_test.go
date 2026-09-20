package replica_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/vermakmanish001/raft-store/internal/raft"
	"github.com/vermakmanish001/raft-store/internal/replica"
	"github.com/vermakmanish001/raft-store/internal/store"
)

// memNet routes messages between replicas in memory, with the ability to
// partition a node away from the rest.
//
// Unlike the harness inside the raft package, these tests run real goroutines
// against a real clock, because the point here is the wiring rather than the
// algorithm. Timings are kept short so the suite stays fast.
type memNet struct {
	mu      sync.RWMutex
	inboxes map[raft.NodeID]chan raft.Message
	down    map[raft.NodeID]bool
}

func newMemNet() *memNet {
	return &memNet{
		inboxes: make(map[raft.NodeID]chan raft.Message),
		down:    make(map[raft.NodeID]bool),
	}
}

func (n *memNet) transport(id raft.NodeID) *memTransport {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.inboxes[id] = make(chan raft.Message, 1024)
	return &memTransport{net: n, id: id}
}

func (n *memNet) isolate(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = true
}

// isDown reads partition state under the lock. Replica goroutines read the
// same map concurrently, so the test must not peek at it directly.
func (n *memNet) isDown(id raft.NodeID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.down[id]
}

type memTransport struct {
	net *memNet
	id  raft.NodeID
}

func (t *memTransport) Send(msg raft.Message) {
	t.net.mu.RLock()
	defer t.net.mu.RUnlock()

	from, to := raft.Sender(msg), raft.Recipient(msg)
	if t.net.down[from] || t.net.down[to] {
		return // partitioned: dropped, as a real network would
	}

	select {
	case t.net.inboxes[to] <- msg:
	default: // full queue drops, which Raft tolerates
	}
}

func (t *memTransport) Inbound() <-chan raft.Message {
	t.net.mu.RLock()
	defer t.net.mu.RUnlock()
	return t.net.inboxes[t.id]
}

// testCluster is a set of running replicas wired to one another.
type testCluster struct {
	t        *testing.T
	net      *memNet
	replicas map[raft.NodeID]*replica.Replica
	ids      []raft.NodeID
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()

	ids := make([]raft.NodeID, size)
	for i := range size {
		ids[i] = raft.NodeID(fmt.Sprintf("n%d", i))
	}

	c := &testCluster{
		t:        t,
		net:      newMemNet(),
		replicas: make(map[raft.NodeID]*replica.Replica, size),
		ids:      ids,
	}

	for _, id := range ids {
		peers := make([]raft.NodeID, 0, size-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		r, err := replica.New(replica.Config{
			ID:        id,
			Peers:     peers,
			Transport: c.net.transport(id),
			Store:     store.New(),

			// Short but not degenerate: a heartbeat every 5ms and an election
			// timeout between 40ms and 80ms.
			TickInterval:       5 * time.Millisecond,
			ElectionTimeoutMin: 8,
			ElectionTimeoutMax: 16,
			HeartbeatInterval:  1,
			WriteTimeout:       3 * time.Second,
		})
		if err != nil {
			t.Fatalf("replica.New(%s): %v", id, err)
		}
		c.replicas[id] = r
		r.Start()
	}

	t.Cleanup(func() {
		for _, r := range c.replicas {
			r.Close()
		}
	})
	return c
}

// leader blocks until exactly one replica reports itself leader.
func (c *testCluster) leader() *replica.Replica {
	c.t.Helper()

	var found *replica.Replica
	if !eventually(3*time.Second, func() bool {
		var leaders []*replica.Replica
		for _, id := range c.ids {
			if c.net.isDown(id) {
				continue
			}
			if c.replicas[id].Status().Role == "leader" {
				leaders = append(leaders, c.replicas[id])
			}
		}
		if len(leaders) == 1 {
			found = leaders[0]
			return true
		}
		return false
	}) {
		c.t.Fatalf("no single leader elected\n%s", c.dump())
	}
	return found
}

func (c *testCluster) dump() string {
	out := "cluster state:\n"
	for _, id := range c.ids {
		s := c.replicas[id].Status()
		out += fmt.Sprintf("  %s role=%-9s term=%d leader=%-4q commit=%d down=%v\n",
			id, s.Role, s.Term, s.Leader, s.CommitIndex, c.net.isDown(id))
	}
	return out
}

func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestSingleNodeServesWrites(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 1)
	leader := c.leader()

	if err := leader.Put("alpha", "one"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := leader.Get("alpha")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "one" {
		t.Errorf("Get = %q, want %q", got, "one")
	}

	if err := leader.Delete("alpha"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := leader.Get("alpha"); !errors.Is(err, store.ErrKeyNotFound) {
		t.Errorf("Get after Delete = %v, want %v", err, store.ErrKeyNotFound)
	}
}

func TestWritesReplicateToFollowers(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 3)
	leader := c.leader()

	if err := leader.Put("alpha", "replicated"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// The write returned, so it is committed. Every follower must converge on
	// the same commit index.
	want := leader.Status().CommitIndex
	if !eventually(3*time.Second, func() bool {
		for _, id := range c.ids {
			if c.replicas[id].Status().CommitIndex < want {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("followers did not reach commit index %d\n%s", want, c.dump())
	}
}

// TestFollowerRejectsWrites: a write must not be accepted by a node that
// cannot replicate it.
func TestFollowerRejectsWrites(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 3)
	leader := c.leader()

	for _, id := range c.ids {
		r := c.replicas[id]
		if r == leader {
			continue
		}

		if err := r.Put("alpha", "one"); !errors.Is(err, raft.ErrNotLeader) {
			t.Errorf("%s Put = %v, want %v", id, err, raft.ErrNotLeader)
		}
		if _, err := r.Get("alpha"); !errors.Is(err, raft.ErrNotLeader) {
			t.Errorf("%s Get = %v, want %v", id, err, raft.ErrNotLeader)
		}
	}
}

// TestCommittedDataSurvivesFailover is the claim that justifies the whole
// project: once a write returns, a leader change must not lose it.
func TestCommittedDataSurvivesFailover(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 3)
	leader := c.leader()

	for k, v := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if err := leader.Put(k, v); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}

	c.net.isolate(leader.Status().ID)

	fresh := c.leader()
	if fresh.Status().ID == leader.Status().ID {
		t.Fatal("the isolated node is still reported as leader")
	}

	// The new leader must serve every committed write. Its own election
	// appends a no-op, and reads are only served once that has been applied.
	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		var got string
		if !eventually(3*time.Second, func() bool {
			v, err := fresh.Get(k)
			got = v
			return err == nil && v == want
		}) {
			t.Errorf("after failover, Get(%s) = %q, want %q\n%s", k, got, want, c.dump())
		}
	}
}

// TestWritesContinueAfterFailover checks liveness, not just durability.
func TestWritesContinueAfterFailover(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 3)
	old := c.leader()
	c.net.isolate(old.Status().ID)

	fresh := c.leader()
	if err := fresh.Put("after", "failover"); err != nil {
		t.Fatalf("Put on the new leader: %v", err)
	}

	got, err := fresh.Get("after")
	if err != nil || got != "failover" {
		t.Errorf("Get = %q, err = %v; want %q, nil", got, err, "failover")
	}
}

// TestIsolatedLeaderCannotCommit: a leader cut off from its followers must not
// acknowledge writes, since it cannot replicate them to a majority.
func TestIsolatedLeaderCannotCommit(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 3)
	leader := c.leader()
	c.net.isolate(leader.Status().ID)

	err := leader.Put("ghost", "value")
	if err == nil {
		t.Fatal("Put succeeded on a partitioned leader; the write reached no majority")
	}
	if !errors.Is(err, replica.ErrTimeout) && !errors.Is(err, raft.ErrNotLeader) {
		t.Errorf("Put = %v, want a timeout or not-leader error", err)
	}
}

func TestEmptyKeyRejectedBeforeReachingTheLog(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 1)
	leader := c.leader()

	before := leader.Status().CommitIndex

	if err := leader.Put("", "value"); !errors.Is(err, store.ErrEmptyKey) {
		t.Errorf("Put = %v, want %v", err, store.ErrEmptyKey)
	}
	if err := leader.Delete(""); !errors.Is(err, store.ErrEmptyKey) {
		t.Errorf("Delete = %v, want %v", err, store.ErrEmptyKey)
	}

	if after := leader.Status().CommitIndex; after != before {
		t.Errorf("commit index moved from %d to %d; a rejected write must not reach the log",
			before, after)
	}
}

func TestCloseStopsTheReplica(t *testing.T) {
	t.Parallel()

	c := newTestCluster(t, 1)
	leader := c.leader()

	if err := leader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Shutdown paths can call Close more than once.
	if err := leader.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	if err := leader.Put("a", "1"); !errors.Is(err, replica.ErrShuttingDown) {
		t.Errorf("Put after Close = %v, want %v", err, replica.ErrShuttingDown)
	}
	if s := leader.Status(); s.Role != "stopped" {
		t.Errorf("Status().Role = %q, want %q", s.Role, "stopped")
	}
}

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	t.Run("transport is required", func(t *testing.T) {
		t.Parallel()

		if _, err := replica.New(replica.Config{ID: "n1", Store: store.New()}); err == nil {
			t.Error("New succeeded without a transport, want an error")
		}
	})

	t.Run("store is required", func(t *testing.T) {
		t.Parallel()

		net := newMemNet()
		if _, err := replica.New(replica.Config{ID: "n1", Transport: net.transport("n1")}); err == nil {
			t.Error("New succeeded without a store, want an error")
		}
	})

	t.Run("invalid raft config is rejected", func(t *testing.T) {
		t.Parallel()

		net := newMemNet()
		_, err := replica.New(replica.Config{
			ID:        "n1",
			Peers:     []raft.NodeID{"n1"}, // listing itself
			Transport: net.transport("n1"),
			Store:     store.New(),
		})
		if !errors.Is(err, raft.ErrInvalidConfig) {
			t.Errorf("New error = %v, want %v", err, raft.ErrInvalidConfig)
		}
	})
}

// startReplica builds and starts a single-node replica against the given
// storage, so a test can stop it and start another over the same state.
func startReplica(t *testing.T, id raft.NodeID, storage raft.Storage) *replica.Replica {
	t.Helper()

	net := newMemNet()
	r, err := replica.New(replica.Config{
		ID:                 id,
		Transport:          net.transport(id),
		Store:              store.New(), // deliberately fresh: rebuilt by replaying the log
		Storage:            storage,
		TickInterval:       5 * time.Millisecond,
		ElectionTimeoutMin: 4,
		ElectionTimeoutMax: 8,
		HeartbeatInterval:  1,
		WriteTimeout:       3 * time.Second,
	})
	if err != nil {
		t.Fatalf("replica.New: %v", err)
	}
	r.Start()
	return r
}

// TestStateSurvivesRestart is the point of durable storage. The replacement
// replica is given an empty store, so every value it returns was rebuilt by
// replaying the persisted log.
func TestStateSurvivesRestart(t *testing.T) {
	t.Parallel()

	storage := raft.NewMemoryStorage()

	first := startReplica(t, "n0", storage)
	if !eventually(3*time.Second, func() bool { return first.Status().Role == "leader" }) {
		t.Fatal("no leader elected")
	}

	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range want {
		if err := first.Put(k, v); err != nil {
			t.Fatalf("Put(%s): %v", k, err)
		}
	}
	if err := first.Delete("b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	delete(want, "b")

	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// A new process, an empty store, the same log.
	second := startReplica(t, "n0", storage)
	t.Cleanup(func() { second.Close() })

	if !eventually(3*time.Second, func() bool { return second.Status().Role == "leader" }) {
		t.Fatalf("restarted replica never became leader: %+v", second.Status())
	}

	for k, v := range want {
		var got string
		if !eventually(3*time.Second, func() bool {
			value, err := second.Get(k)
			got = value
			return err == nil && value == v
		}) {
			t.Errorf("after restart Get(%s) = %q, want %q", k, got, v)
		}
	}

	// The deletion must have survived too. Replaying a log that forgot it
	// would resurrect the key.
	if _, err := second.Get("b"); !errors.Is(err, store.ErrKeyNotFound) {
		t.Errorf("Get(b) after restart = %v, want %v; the delete was replayed away",
			err, store.ErrKeyNotFound)
	}
}

// TestTermSurvivesRestart: a node that forgot its term would restart at term
// 0 and be free to vote again in a term it had already voted in.
func TestTermSurvivesRestart(t *testing.T) {
	t.Parallel()

	storage := raft.NewMemoryStorage()

	first := startReplica(t, "n0", storage)
	if !eventually(3*time.Second, func() bool { return first.Status().Role == "leader" }) {
		t.Fatal("no leader elected")
	}
	before := first.Status().Term
	first.Close()

	second := startReplica(t, "n0", storage)
	t.Cleanup(func() { second.Close() })

	if !eventually(3*time.Second, func() bool { return second.Status().Term >= before }) {
		t.Errorf("term after restart = %d, want at least %d", second.Status().Term, before)
	}
}

// brokenStorage fails writes after the first n of them, standing in for a disk
// that fills up while the node is running.
type brokenStorage struct {
	raft.Storage
	mu        sync.Mutex
	allowed   int
	failWith  error
	attempted int
}

func (s *brokenStorage) SaveHardState(hs raft.HardState) error {
	s.mu.Lock()
	s.attempted++
	over := s.attempted > s.allowed
	s.mu.Unlock()

	if over {
		return s.failWith
	}
	return s.Storage.SaveHardState(hs)
}

// TestReplicaStopsWhenStorageFails: a node that cannot persist must stop and
// say so, rather than accept writes it can never make durable.
func TestReplicaStopsWhenStorageFails(t *testing.T) {
	t.Parallel()

	diskFull := errors.New("no space left on device")
	storage := &brokenStorage{
		Storage:  raft.NewMemoryStorage(),
		allowed:  0, // fail from the very first persist
		failWith: diskFull,
	}

	r := startReplica(t, "n0", storage)
	t.Cleanup(func() { r.Close() })

	// The first campaign must persist a term and self-vote, which fails.
	if !eventually(3*time.Second, func() bool { return r.Status().Role == "stopped" }) {
		t.Fatalf("replica kept running with failed storage: %+v", r.Status())
	}

	if err := r.Err(); !errors.Is(err, diskFull) {
		t.Errorf("Err = %v, want it to wrap %v", err, diskFull)
	}

	err := r.Put("a", "1")
	if !errors.Is(err, replica.ErrStorageFailed) && !errors.Is(err, replica.ErrShuttingDown) {
		t.Errorf("Put after storage failure = %v, want a storage or shutdown error", err)
	}
}
