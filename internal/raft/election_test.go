package raft

import (
	"math/rand"
	"testing"
)

func TestSingleNodeElectsItself(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 1)

	// A lone node has a majority of one, so it wins without sending anything.
	if !c.advanceUntil(50, func() bool { return len(c.leaders()) == 1 }) {
		t.Fatalf("no leader after 50 ticks\n%s", c.dump())
	}

	leader := c.nodes["n0"]
	if leader.Term() != 1 {
		t.Errorf("term = %d, want 1; the first election should advance exactly one term", leader.Term())
	}
	if leader.Leader() != "n0" {
		t.Errorf("Leader() = %q, want %q", leader.Leader(), "n0")
	}
}

func TestClusterElectsExactlyOneLeader(t *testing.T) {
	t.Parallel()

	for _, size := range []int{3, 5, 7} {
		t.Run(sizeName(size), func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, size)
			if !c.advanceUntil(100, func() bool { return len(c.leaders()) == 1 }) {
				t.Fatalf("no single leader after 100 ticks\n%s", c.dump())
			}

			leader := c.requireSingleLeader()
			c.checkNoSplitBrain()

			// Every other node must agree who won, and agree on the term.
			term := c.nodes[leader].Term()
			for _, id := range c.ids {
				n := c.nodes[id]
				if n.Term() != term {
					t.Errorf("%s term = %d, want %d", id, n.Term(), term)
				}
				if n.Leader() != leader {
					t.Errorf("%s Leader() = %q, want %q", id, n.Leader(), leader)
				}
				if id != leader && n.Role() != Follower {
					t.Errorf("%s role = %s, want follower", id, n.Role())
				}
			}
		})
	}
}

// TestLeaderRetainsLeadership verifies that heartbeats suppress elections. A
// healthy cluster must not churn through terms while its leader is alive.
func TestLeaderRetainsLeadership(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	if !c.advanceUntil(100, func() bool { return len(c.leaders()) == 1 }) {
		t.Fatalf("no leader elected\n%s", c.dump())
	}

	leader := c.requireSingleLeader()
	term := c.nodes[leader].Term()

	// Far longer than several election timeouts. If heartbeats were missing or
	// too slow, a follower would have campaigned well inside this window.
	c.advance(500)

	if got := c.requireSingleLeader(); got != leader {
		t.Errorf("leader changed from %s to %s while healthy", leader, got)
	}
	if got := c.nodes[leader].Term(); got != term {
		t.Errorf("term advanced from %d to %d while healthy; the cluster is churning", term, got)
	}
}

// TestNewLeaderElectedWhenLeaderPartitioned is the failover case: the cluster
// must recover on its own once the leader stops being reachable.
func TestNewLeaderElectedWhenLeaderPartitioned(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	if !c.advanceUntil(100, func() bool { return len(c.leaders()) == 1 }) {
		t.Fatalf("no initial leader\n%s", c.dump())
	}

	old := c.requireSingleLeader()
	oldTerm := c.nodes[old].Term()
	c.isolate(old)

	// The surviving two are a majority of three, so they can elect.
	elected := c.advanceUntil(200, func() bool {
		for _, id := range c.ids {
			if id != old && c.nodes[id].Role() == Leader {
				return true
			}
		}
		return false
	})
	if !elected {
		t.Fatalf("no new leader after partitioning %s\n%s", old, c.dump())
	}

	var fresh NodeID
	for _, id := range c.ids {
		if id != old && c.nodes[id].Role() == Leader {
			fresh = id
		}
	}

	if c.nodes[fresh].Term() <= oldTerm {
		t.Errorf("new leader term = %d, want greater than old term %d",
			c.nodes[fresh].Term(), oldTerm)
	}

	// The isolated node still believes it leads its old term. That is correct:
	// it has no way to learn otherwise, and its stale term is exactly what
	// causes its writes to be refused if it ever reconnects.
	if c.nodes[old].Role() != Leader {
		t.Errorf("partitioned node role = %s, want it to still believe it leads", c.nodes[old].Role())
	}
	c.checkNoSplitBrain()
}

// TestDeposedLeaderStepsDownAfterHealing covers the rejoin case: a leader that
// was partitioned must yield once it discovers a newer term, without
// disturbing the cluster that moved on without it.
func TestDeposedLeaderStepsDownAfterHealing(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 3)
	if !c.advanceUntil(100, func() bool { return len(c.leaders()) == 1 }) {
		t.Fatalf("no initial leader\n%s", c.dump())
	}

	old := c.requireSingleLeader()
	c.isolate(old)

	elected := c.advanceUntil(200, func() bool {
		for _, id := range c.ids {
			if id != old && c.nodes[id].Role() == Leader {
				return true
			}
		}
		return false
	})
	if !elected {
		t.Fatalf("no new leader while %s was partitioned\n%s", old, c.dump())
	}

	c.heal()
	c.advance(50)

	if role := c.nodes[old].Role(); role != Follower {
		t.Errorf("rejoined node role = %s, want follower\n%s", role, c.dump())
	}
	c.requireSingleLeader()
	c.checkNoSplitBrain()
}

// TestNoSplitBrainUnderRepeatedPartitions hammers the safety property through
// a long randomized sequence of partitions and heals. The schedule is seeded,
// so a failure reproduces exactly rather than appearing once in CI and never
// again.
func TestNoSplitBrainUnderRepeatedPartitions(t *testing.T) {
	t.Parallel()

	c := newCluster(t, 5)
	rng := rand.New(rand.NewSource(42))

	for round := range 200 {
		// Isolate a minority. Partitioning a majority away would legitimately
		// stall progress, which is liveness, not the safety property under
		// test here.
		c.heal()
		for range rng.Intn(3) { // 0, 1, or 2 of 5 nodes
			c.isolate(c.ids[rng.Intn(len(c.ids))])
		}

		c.advance(1 + rng.Intn(30))

		c.checkNoSplitBrain()

		if t.Failed() {
			t.Fatalf("safety violated in round %d", round)
		}
	}

	// With a majority reachable throughout, the cluster must still be able to
	// settle on a leader once fully healed.
	c.heal()
	if !c.advanceUntil(300, func() bool { return len(c.leaders()) == 1 }) {
		t.Fatalf("cluster could not settle after healing\n%s", c.dump())
	}
}

// TestElectionTimeoutsAreRandomized guards the property that prevents endless
// split votes. Without randomization, nodes campaign in lockstep forever.
func TestElectionTimeoutsAreRandomized(t *testing.T) {
	t.Parallel()

	n, err := NewNode(Config{
		ID:                 "n0",
		Peers:              []NodeID{"n1", "n2"},
		ElectionTimeoutMin: 10,
		ElectionTimeoutMax: 20,
		HeartbeatInterval:  3,
		Rand:               rand.New(rand.NewSource(1)),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	seen := make(map[int]bool)
	for range 100 {
		n.resetElectionTimer()

		if n.electionTimeout < 10 || n.electionTimeout >= 20 {
			t.Fatalf("timeout %d outside [10, 20)", n.electionTimeout)
		}
		seen[n.electionTimeout] = true
	}

	if len(seen) < 5 {
		t.Errorf("only %d distinct timeouts in 100 resets, want spread across the range", len(seen))
	}
}

func sizeName(n int) string {
	return string(rune('0'+n)) + "-node"
}
