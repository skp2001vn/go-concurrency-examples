# Go Concurrency Examples

A Go 1.22+ module that implements and tests a set of practical concurrency patterns and coordination primitives.

The codebase is organized as small, focused introductory examples such as duplicate request suppression, inventory, bank accounts, semaphores, connection pools, worker pools, pipelines, rate limiters, bounded queues, barriers, pub/sub brokers, task groups, actors, lazy initialization, and atomic counters. Each example demonstrates a concurrency technique using the Go standard library and is covered by automated tests.

The repository also includes an [AGENTS.md](AGENTS.md) guide to keep AI-assisted and human contributions consistent across examples, tests, and documentation.

## Requirements

- Go 1.22+

## Build And Test

```bash
go test ./...
```

## Implemented Examples

| Example | What it demonstrates |
| --- | --- |
| [`singleflight`](singleflight/) | Suppressing duplicate requests so concurrent callers share one expensive result by key, using `sync.Mutex`, an in-flight map, and `sync.WaitGroup` |
| [`inventory`](inventory/) | Keeping stock counts correct when many buyers purchase limited inventory, using `sync.Mutex` to guard shared map state |
| [`bankaccount`](bankaccount/) | Keeping balances correct during deposits, withdrawals, and transfers, using `sync.Mutex`, deterministic lock ordering, and `sync/atomic` |
| [`semaphore`](semaphore/) | Limiting how many callers use a shared resource at once, using a buffered channel as a counting semaphore and `context.Context` cancellation |
| [`connectionpool`](connectionpool/) | Borrowing and returning reusable connections under high demand, using `sync.Mutex`, FIFO waiter channels, and `context.Context` timeouts |
| [`workerpool`](workerpool/) | Processing independent jobs without overwhelming a system, using goroutines, job channels, `sync.WaitGroup`, and `context.Context` cancellation |
| [`pipeline`](pipeline/) | Validating, filtering, transforming, and collecting a cancellable batch, using staged channels, channel ownership, and `context.Context` cancellation |
| [`ratelimiter`](ratelimiter/) | Limiting how often callers perform work over time, using `time.Ticker`, token channels, and `context.Context` cancellation |
| [`boundedqueue`](boundedqueue/) | Coordinating producers and consumers through a fixed-size job queue, using `sync.Mutex`, `sync.Cond`, and close signaling |
| [`barrier`](barrier/) | Coordinating workers so all finish one phase before any start the next, using `sync.Mutex`, `sync.Cond`, and generation counters |
| [`pubsub`](pubsub/) | Broadcasting each published message to multiple subscribers, using `sync.RWMutex`, per-subscriber channels, and channel ownership |
| [`taskgroup`](taskgroup/) | Running related tasks concurrently and canceling the group on first failure, using goroutines, `sync.WaitGroup`, `context.WithCancel`, and first-error capture |
| [`actor`](actor/) | Managing shared state through a single owner goroutine, using command channels, reply channels, and event-loop serialization |
| [`lazyinit`](lazyinit/) | Initializing an expensive shared resource exactly once for concurrent callers, using `sync.Once` and cached result/error |
| [`atomiccounter`](atomiccounter/) | Tracking high-frequency request metrics from many goroutines, using `sync/atomic` counters and compare-and-swap updates |

## Agent Workflow

This repository includes [AGENTS.md](AGENTS.md) to guide AI-assisted and human contributions when adding or updating examples.

Its purpose is to keep the repository consistent as it grows by defining:
- how new examples should be structured
- expectations for Go doc comments
- testing requirements
- README maintenance rules
- general code quality and naming conventions

If you add a new example, check `AGENTS.md` before making changes.

## Project Structure

Each example is a Go package. Package code and package tests live in the same directory, with tests named `*_test.go`.

- `<example>/` contains the example implementation and its tests
- `README.md` provides the project overview and implemented example list
