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
| 3 | Log replication | Next |
| 4 | Replicated KV store over a real cluster | Planned |
| 5 | Durable persistence and crash recovery | Planned |
| 6 | Leader redirect, request dedup, linearizable reads | Planned |
| 7 | Snapshots and log compaction | Planned |
| 8 | Cluster membership and observability | Planned |

Leader election is implemented and tested as a library, but is not yet wired
into the running node. The `raftkv` binary still serves a single node from
memory with no replication and no durability. Connecting the two happens once
log replication exists, since a leader with no way to replicate writes would
offer nothing a single node does not already do.

## Quickstart

Requires Go 1.23 or later. There are no third-party dependencies.

Start a node:

```bash
make run
```

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
internal/store/     storage engine, concurrency-safe, knows nothing of Raft
internal/api/       HTTP transport: routing, status codes, encoding
internal/raft/      consensus state machine: no goroutines, no clock, no I/O
```

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

Current coverage: 100% of `internal/store`, 99% of `internal/raft`, 84% of
`internal/api`.

## License

MIT
