package raft

import (
	"fmt"
	"math/rand"
	"testing"
)

// cluster is a deterministic in-memory simulation of a Raft cluster.
//
// It replaces the network and the clock entirely. Time advances only when a
// test calls advance, and messages move only when the harness delivers them,
// so a scenario such as "the leader is partitioned for exactly 30 ticks"
// is expressed directly and replays identically on every run.
type cluster struct {
	t     *testing.T
	ids   []NodeID // iteration order, kept stable so runs are reproducible
	nodes map[NodeID]*Node
	queue []Message

	// isolated nodes still experience time, and so still time out and
	// campaign, but neither send nor receive. That models a network partition
	// rather than a crash: the node is healthy and unaware anything is wrong,
	// which is the case most likely to violate safety.
	isolated map[NodeID]bool
}

// newCluster builds a cluster of size n with ids "n0", "n1", and so on.
//
// Each node is seeded differently but deterministically, so election timeouts
// differ between nodes, as randomization requires, while the whole run stays
// reproducible.
func newCluster(t *testing.T, size int) *cluster {
	t.Helper()

	ids := make([]NodeID, size)
	for i := range size {
		ids[i] = NodeID(fmt.Sprintf("n%d", i))
	}

	c := &cluster{
		t:        t,
		ids:      ids,
		nodes:    make(map[NodeID]*Node, size),
		isolated: make(map[NodeID]bool),
	}

	for i, id := range ids {
		peers := make([]NodeID, 0, size-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		node, err := NewNode(Config{
			ID:                 id,
			Peers:              peers,
			ElectionTimeoutMin: 10,
			ElectionTimeoutMax: 20,
			HeartbeatInterval:  3,
			Rand:               rand.New(rand.NewSource(int64(i) + 1)),
		})
		if err != nil {
			t.Fatalf("NewNode(%s): %v", id, err)
		}
		c.nodes[id] = node
	}
	return c
}

// advance moves logical time forward by the given number of ticks, delivering
// all resulting messages to quiescence after each one.
func (c *cluster) advance(ticks int) {
	c.t.Helper()

	for range ticks {
		for _, id := range c.ids {
			c.enqueue(c.nodes[id].Tick())
		}
		c.deliverAll()
	}
}

// advanceUntil ticks until cond holds, returning false if it never does.
// Tests assert on the boolean rather than looping forever, so a regression
// fails with a clear message instead of hanging CI.
func (c *cluster) advanceUntil(maxTicks int, cond func() bool) bool {
	c.t.Helper()

	for range maxTicks {
		if cond() {
			return true
		}
		c.advance(1)
	}
	return cond()
}

func (c *cluster) enqueue(msgs []Message) {
	c.queue = append(c.queue, msgs...)
}

// deliverAll drains the message queue, feeding each message to its recipient
// and queueing whatever that produces, until nothing is left to deliver.
func (c *cluster) deliverAll() {
	c.t.Helper()

	for round := 0; len(c.queue) > 0; round++ {
		// Raft settles within a bounded number of exchanges per tick. An
		// unbounded loop here would mean two nodes are answering each other
		// forever, so failing loudly beats hanging.
		if round > 100 {
			c.t.Fatalf("message storm: %d rounds in a single tick, %d still queued",
				round, len(c.queue))
		}

		batch := c.queue
		c.queue = nil

		for _, msg := range batch {
			h := msg.header()
			if c.isolated[h.From] || c.isolated[h.To] {
				continue // partitioned: silently dropped, as a real network would
			}
			if node, ok := c.nodes[h.To]; ok {
				c.enqueue(node.Step(msg))
			}
		}
	}
}

// isolate partitions a node away from the rest of the cluster.
func (c *cluster) isolate(id NodeID) {
	c.isolated[id] = true

	// Messages already in flight to or from the node are dropped, matching a
	// partition that takes effect immediately.
	kept := c.queue[:0]
	for _, m := range c.queue {
		h := m.header()
		if !c.isolated[h.From] && !c.isolated[h.To] {
			kept = append(kept, m)
		}
	}
	c.queue = kept
}

// heal reconnects every isolated node.
func (c *cluster) heal() {
	c.isolated = make(map[NodeID]bool)
}

// leaders returns every node currently believing itself leader.
func (c *cluster) leaders() []NodeID {
	var found []NodeID
	for _, id := range c.ids {
		if c.nodes[id].Role() == Leader {
			found = append(found, id)
		}
	}
	return found
}

// requireSingleLeader asserts exactly one leader exists and returns it.
func (c *cluster) requireSingleLeader() NodeID {
	c.t.Helper()

	found := c.leaders()
	if len(found) != 1 {
		c.t.Fatalf("want exactly 1 leader, got %d %v\n%s", len(found), found, c.dump())
	}
	return found[0]
}

// checkNoSplitBrain asserts the central safety property of leader election:
// no two nodes may believe they lead the same term (Section 5.2).
//
// Two leaders in *different* terms is legal and expected during a partition,
// since a deposed leader does not learn it was replaced until it can
// communicate again. Only same-term duplication is a violation.
func (c *cluster) checkNoSplitBrain() {
	c.t.Helper()

	byTerm := make(map[Term]NodeID)
	for _, id := range c.ids {
		node := c.nodes[id]
		if node.Role() != Leader {
			continue
		}
		if other, exists := byTerm[node.Term()]; exists {
			c.t.Fatalf("two leaders in term %d: %s and %s\n%s",
				node.Term(), other, id, c.dump())
		}
		byTerm[node.Term()] = id
	}
}

// dump renders cluster state for failure messages.
func (c *cluster) dump() string {
	out := "cluster state:\n"
	for _, id := range c.ids {
		n := c.nodes[id]
		out += fmt.Sprintf("  %s role=%-9s term=%d votedFor=%-4q leader=%-4q isolated=%v\n",
			id, n.Role(), n.Term(), n.VotedFor(), n.Leader(), c.isolated[id])
	}
	return out
}
