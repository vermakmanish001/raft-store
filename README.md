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
| 5 | Durable persistence and crash recovery | Next |
| 6 | Leader redirect, request dedup, linearizable reads | Planned |
| 7 | Snapshots and log compaction | Planned |
| 8 | Cluster membership and observability | Planned |

A cluster replicates writes, elects leaders, and survives the loss of any
minority of its nodes. Three limitations remain, each addressed by a later
step and each stated plainly rather than hidden:

- **No durability.** State lives in memory. A node that restarts rejoins with
  an empty log and is refilled by the leader, so a cluster survives losing a
  minority, but not a simultaneous restart of a majority.
- **Writes are at-least-once.** A write interrupted by a leader change is
  reported as failed, and a client retry applies it twice. Fixing this needs
  request deduplication held in replicated state.
- **Reads are not linearizable.** They are served from the leader's applied
  state, which can lag or come from a leader that has already been deposed
  without noticing.

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

| Method | Path | Success | Failure |
|--------|------|---------|---------|
| `GET` | `/kv/{key}` | `200` with `{"key","value"}` | `404` if absent |
| `PUT` | `/kv/{key}` | `204`, no body | `413` if the value exceeds 1 MiB |
| `DELETE` | `/kv/{key}` | `204`, no body | `404` if absent |
| `GET` | `/health` | `200` with `{"status":"ok"}` | |
| `GET` | `/status` | `200` with role, term, leader, commit index | |
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

## Design

```
cmd/raftkv/         node entrypoint: flags, wiring, graceful shutdown
internal/raft/      consensus state machine: no goroutines, no clock, no I/O
internal/replica/   drives consensus: owns the loop, clock, and pending writes
internal/transport/ Raft RPCs over HTTP, plus the wire codec
internal/fsm/       turns committed log entries into store mutations
internal/store/     storage engine, concurrency-safe, knows nothing of Raft
internal/api/       client HTTP: routing, status codes, leader redirection
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

The consensus core is a pure state machine. It has no goroutines, no timers,
and no network or disk access. Callers drive it with `Tick`, which advances
logical time by one unit, and `Step`, which delivers one message. Both return
the messages the caller should send. Nothing inside the package blocks, sleeps,
or opens a socket.

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

Coverage by package: store and config at 100%, raft 97%, fsm 94%, replica 92%,
api 91%, transport 85%.

## License

MIT
