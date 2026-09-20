# raft-store

A distributed, strongly-consistent key-value store written in Go, built on a
from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf).

No consensus libraries are used. The Raft implementation here — leader election,
log replication, persistence, and snapshotting — is written directly against the
paper, because the point of this project is the algorithm, not the glue around it.

## Status

Built incrementally. Each milestone is independently runnable and tested.

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Single-node KV store with HTTP API | **Complete** |
| 2 | Raft core: leader election | **Complete** |
| 3 | Log replication | **Complete** |
| 4 | Replicated KV store over a real cluster | **Complete** |
| 5 | Durable persistence and crash recovery | **Complete** |
| 6 | Request dedup and linearizable reads | **Complete** |
| 7 | Snapshots and log compaction | **Complete** |
| 8 | Cluster membership and observability | Next |

A cluster replicates writes, elects leaders, survives the loss of any minority
of its nodes, and survives losing all of them: state reaches a crash-safe log
before any RPC is answered. Reads are linearizable, a client that identifies
its requests gets exactly-once writes, and the log is compacted so it does not
grow without bound.

What remains is operational rather than foundational: changing cluster
membership without a restart, and metrics.

## Quickstart

Requires Go 1.23 or later. There are no third-party dependencies.

Start a single node:

```bash
make run
```

Or a three-node cluster on ports 8181 through 8183:

```bash
make cluster                 # build and launch three nodes
make cluster-status          # each node's role, term, and commit index
make cluster-kill-leader     # stop the leader and watch failover
make cluster-restart         # kill every node, restart from their logs
make cluster-stop
```

Writes go to the leader. Addressing a follower returns a 307 redirect, which
`curl -L` follows while preserving the method and body. To watch a write
survive the loss of the node that accepted it:

```bash
scripts/cluster.sh put durable 'survives a crash'
make cluster-kill-leader
scripts/cluster.sh get durable
```

The value comes back from a node that was never the one you wrote to.

To watch it survive the loss of the *entire* cluster, which is what durable
storage buys:

```bash
scripts/cluster.sh put survivor 'outlives the whole cluster'
make cluster-restart
scripts/cluster.sh get survivor
```

Every node is killed with SIGKILL and restarted from its write-ahead log. With
no surviving member there is nobody to replicate from, so anything that comes
back was read off disk.

To see deduplication, run the same write twice with the same headers, with
another client's write in between:

```bash
BASE=http://127.0.0.1:8181
curl -sL -X PUT -d 'from A' -H 'X-Client-ID: a' -H 'X-Request-Seq: 1' $BASE/kv/x
curl -sL -X PUT -d 'from B' -H 'X-Client-ID: b' -H 'X-Request-Seq: 1' $BASE/kv/x
curl -sL -X PUT -d 'from A' -H 'X-Client-ID: a' -H 'X-Request-Seq: 1' $BASE/kv/x
curl -sL $BASE/kv/x
```

The value is `from B`. Drop the headers and repeat, and it is `from A`: the
retry has destroyed a write that another client was told had succeeded.

Those helpers pick whichever node is reachable rather than a fixed port, which
matters more than it sounds: the leader is whichever node wins the election,
so the node `kill-leader` stops differs from run to run. A hardcoded address
lands on the dead node about a third of the time, and `curl -s` hides the
connection error, making a healthy cluster look like data loss.

Then, from a second terminal:

```bash
curl -X PUT -d 'hello raft' http://127.0.0.1:8081/kv/greeting
curl http://127.0.0.1:8081/kv/greeting
curl -X DELETE http://127.0.0.1:8081/kv/greeting
```

The middle command prints `{"key":"greeting","value":"hello raft"}`. The other
two return `204` with no body. Add `-i` to any request to see its status line
and headers.

To use a different port, pass it to both:

```bash
make run ADDR=:9999
curl http://127.0.0.1:9999/health
```

The `raftkv` binary defaults to `:8080`; the Makefile uses `:8081` because 8080
is often already occupied. If the port you pick is taken, the node says so and
exits rather than starting in a broken state.

## API

The request body of a `PUT` is the raw value, which keeps the API usable from
curl without JSON quoting. All responses are JSON. Keys are a single path
segment, so a key containing a slash does not match a route.

Writes may carry two headers that make them exactly-once:

| Header | Meaning |
|--------|---------|
| `X-Client-ID` | stable identifier for the client, reused across requests |
| `X-Request-Seq` | increments once per operation, held constant across retries |

Both are required together; a partial pair is treated as absent. Without them
a write is at-least-once, which is fine for a one-shot command typed at a
terminal and not fine for a client that retries.

| Method | Path | Success | Failure |
|--------|------|---------|---------|
| `GET` | `/kv/{key}` | `200` with `{"key","value"}` | `404` if absent |
| `PUT` | `/kv/{key}` | `204`, no body | `413` if the value exceeds 1 MiB |
| `DELETE` | `/kv/{key}` | `204`, no body | `404` if absent |
| `GET` | `/health` | `200` with `{"status":"ok"}` | |
| `GET` | `/status` | `200` with role, term, leader, commit and snapshot index | |
| `POST` | `/raft/message` | `202`, peer RPC, not for clients | |

`PUT` is idempotent and returns `204` for both a create and an overwrite,
because the store does not distinguish the two. An empty body stores an empty
value, which is a real value and distinct from an absent key.

`/health` reports process liveness only. It deliberately says nothing about
cluster state: once Raft exists, a node's role and whether it can reach a
quorum belong on a separate status endpoint, so a liveness probe is never
conflated with readiness to serve reads.

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `:8080` | host:port for the HTTP API |
| `-log-level` | `info` | `debug`, `info`, `warn`, or `error` |
| `-log-json` | `false` | emit JSON logs instead of text |
| `-id` | `node1` | unique node ID within the cluster |
| `-peers` | empty | other nodes, as `id=url[,id=url...]` |
| `-advertise` | derived | base URL peers use to reach this node |
| `-data-dir` | empty | write-ahead log directory; empty keeps state in memory |
| `-snapshot-threshold` | `1024` | compact once this many uncompacted entries accumulate; 0 disables |

## Design

```
cmd/raftkv/         node entrypoint: flags, wiring, graceful shutdown
internal/raft/      consensus state machine: no goroutines, no clock, no I/O
internal/replica/   drives consensus: owns the loop, clock, and pending writes
internal/transport/ Raft RPCs over HTTP, plus the wire codec
internal/fsm/       turns committed log entries into store mutations
internal/store/     storage engine, concurrency-safe, knows nothing of Raft
internal/api/       client HTTP: routing, status codes, leader redirection
internal/wal/       crash-safe write-ahead log and snapshot store
internal/config/    cluster configuration parsing
```

The dependency arrows all point one way. The store knows nothing of Raft, the
raft package knows nothing of HTTP or the store, and `replica` is the only
place all three meet. That is why swapping the in-memory store for a
replicated one required no change to the API layer at all: `Replica` satisfies
the same `store.Store` interface that `MemStore` does, so the HTTP handlers
never learned that replication exists.

One goroutine owns the consensus node. It is the only thing permitted to touch
that state, which is what makes the raft package's complete absence of
internal locking safe rather than reckless. Client writes reach it through a
channel and wait on a per-entry registry keyed by log index.

A pending write records the term its entry was appended in. If a different
entry commits at that index, this node was deposed and the client's write
never happened, so it is reported as a failure rather than a success. Without
that check a client would be told its write succeeded when another leader's
entry had taken the slot.

The consensus core is a state machine with no goroutines, no timers, and no
network access. Callers drive it with `Tick`, which advances logical time by
one unit, and `Step`, which delivers one message. Both return the messages the
caller should send. The package opens nothing itself: durability arrives as an
injected `Storage` interface, so the tests run against an in-memory
implementation with no disk at all while a real node is handed a write-ahead
log.

That is a testing decision above all. Consensus bugs are ordering bugs, and
they appear only under interleavings that are rare on a healthy network: a vote
arriving after the term moved on, two candidates campaigning at once, a leader
deposed mid-broadcast. Driving the algorithm with an explicit clock lets a test
construct those interleavings exactly and replay them identically every run.
The test suite partitions leaders, heals partitions, and asserts that two nodes
never lead the same term, across hundreds of randomized but seeded rounds, in
under a second and with no `time.Sleep` anywhere.

Timeouts are counted in ticks rather than durations, so the caller decides what
a tick means. Production drives it from a ticker; tests drive it in a loop and
resolve a full election in microseconds.

The subtlest requirement in Raft is the commit rule of Section 5.4.2, shown in
Figure 8 of the paper: a leader may only mark an entry committed by counting
replicas if that entry belongs to the leader's own term. An entry from an
earlier term can sit on a majority and still be legitimately overwritten by a
future leader, so committing it on replica count alone can destroy data that
was already applied and acknowledged. Entries from previous terms become
committed indirectly, carried along when an entry from the current term
commits above them.

That rule is worth calling out because deleting it leaves every other test in
the suite passing. Only `TestFigure8CommitRule` fails, which is exactly why it
exists.

## Durability

Term, vote, and log entries reach disk before the node answers any RPC that
depends on them. The ordering is the whole point rather than a detail. A node
that replies to a vote request and *then* records the vote can crash in
between, restart having forgotten it, and vote a second time in the same term.
Two candidates then each assemble a majority containing that node, both become
leader for one term, and both can commit conflicting entries at the same
index. Every safety argument in Raft rests on that being impossible.

The log is append-only and never rewritten in place, because rewriting is
precisely the operation a crash can leave half-finished. Truncations are
recorded as records rather than by rewinding the file. Each record carries a
length and a CRC32, so a process killed mid-write leaves a fragment that
recovery recognizes and discards: that record was never acknowledged to
anyone, so dropping it costs nothing. Damage anywhere earlier in the file is
reported as corruption instead, because it means a record that *was*
acknowledged can no longer be trusted.

A node that cannot persist stops participating rather than continuing. It then
looks like a failed node to its peers, which is a situation the cluster
already knows how to survive, as opposed to a node answering RPCs it may not
remember having answered.

## Exactly-once writes and linearizable reads

A client whose write commits but whose response is lost, because the leader
crashed in between, cannot tell success from failure and has to retry. Applying
that retry a second time is harmless for a repeated write of the same value,
and quietly destructive otherwise: if another client wrote the same key in the
interim, the late duplicate overwrites a newer value that its author was told
had succeeded. Nothing is lost from the log and every replica agrees on the
result. The data is simply wrong.

Requests that carry a client ID and sequence number are therefore recorded in a
session table as they apply, and a repeat returns the original outcome without
touching the store. That table is built from the log alone, so every replica
constructs the same one, a restarting node rebuilds it by replaying, and it
survives failover. It is capped, and eviction is driven by a counter of applied
commands rather than by a clock, because two replicas evicting different
clients would start disagreeing about which retries are duplicates. An evicted
client's retry can still apply twice, which is the honest cost of a bounded
table; production systems resolve it with sessions that clients renew.

Reads do not go through the log, which would cost a round trip and an entry to
change nothing. Instead a leader clears a barrier before answering. It waits
to hear from a majority that it still leads, because a leader partitioned away
from its cluster keeps believing it leads until its election timeout elapses
and would otherwise answer from state the real leader has moved past. It also
waits until its own state machine has applied everything committed when the
read arrived, since even a legitimate leader can hold entries it has not yet
applied. Only then does it read.

The confirmation rides on a heartbeat rather than a new RPC, and depends on no
assumption about clock drift, which is what separates this from the faster
lease-based alternative.

## Snapshots and compaction

Left alone, the log records every write forever. A long-running cluster would
replay an ever-longer file at startup and keep entries whose effects were
superseded years earlier. Once a threshold of uncompacted entries accumulates,
a node serializes its state machine, records it as a snapshot, and discards
every entry the snapshot covers.

Three things make this delicate.

The snapshot is taken at `lastApplied`, not at the commit index. An entry that
is committed but not yet applied is not reflected in the state machine, so a
snapshot claiming to cover it would silently lose its effect.

The snapshot must be durable before any entry is discarded. A crash in that
order leaves both the snapshot and the full log, which is merely redundant
because loading filters entries the snapshot already covers. A crash in the
other order leaves neither, and the state those entries described exists
nowhere. Both the snapshot file and the rewritten log are installed by rename,
so a crash mid-write leaves the previous version rather than half of each.

The snapshot includes the session table from the previous section, and
omitting it would be a silent correctness bug rather than a missed
optimization. A node restored without it has forgotten which client requests
it already applied, so deduplication would hold right up until the first
snapshot and then quietly stop.

Compaction also breaks the ordinary repair path. A leader normally walks a
lagging follower's index backward until their logs agree, but there is nothing
to walk back to once that prefix has been folded into a snapshot. Such a
follower is sent the snapshot instead, through `InstallSnapshot`, and adopts
its boundary as its own. A follower that already holds the entry the snapshot
ends at keeps everything after it rather than refetching.

Indexing is the other cost. After a snapshot the slice holds a suffix, so the
entry at logical index `i` lives at position `i-S-1` rather than `i-1`. That
arithmetic is confined to [internal/raft/log.go](internal/raft/log.go), which
is why this step changed one file instead of producing an off-by-one in every
function that touches the log.

The key-value store has no knowledge of Raft. Replication is layered on top of
it rather than woven into it, and store errors are sentinels tested with
`errors.Is`. The function `errorStatus` in the API package is the single place
where a storage failure becomes an HTTP status code, so replication-specific
failures such as "this node is not the leader" extend one function rather than
every handler.

Store errors are sentinels tested with `errors.Is`, and `errorStatus` in the
API package is the single place where a storage failure becomes an HTTP status
code. Replication adds new failure modes, such as "this node is not the
leader", by extending that one function rather than every handler.

## Development

```bash
make check     # gofmt, go vet, and tests under the race detector
make test      # tests only
make race      # tests under the race detector
make cover     # coverage report, written to coverage.html
```

`make check` is the gate a commit is expected to pass.

There is also an end-to-end smoke test that drives a real node over a real
socket, which catches wiring mistakes that in-process tests cannot see. Start a
node in one terminal and run the script from another:

```bash
make run       # terminal 1
make smoke     # terminal 2
```

It asserts the status code of all fifteen request cases, prints a pass or fail
line for each, cleans up the keys it wrote, and exits non-zero if any case
fails, so it can be wired into CI later.

Coverage by package: store and config at 100%, raft 92%, fsm 92%, api 91%,
replica 88%, transport 86%, wal 79%.

## License

MIT
