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
| 1 | Single-node KV store with HTTP API | In progress |
| 2 | Raft core: leader election | Planned |
| 3 | Log replication | Planned |
| 4 | Replicated KV store over a real cluster | Planned |
| 5 | Durable persistence and crash recovery | Planned |
| 6 | Leader redirect, request dedup, linearizable reads | Planned |
| 7 | Snapshots and log compaction | Planned |
| 8 | Cluster membership and observability | Planned |

## Design

The consensus core (`internal/raft`) performs no I/O. It communicates through
`transport` and `storage` interfaces, which makes elections and replication
testable deterministically, without sleeps or real sockets. The key-value store
(`internal/store`) has no knowledge of Raft; replication is layered on top of it
rather than woven into it.

## Requirements

Go 1.23 or later. No third-party dependencies.

## License

MIT
# raft-store
