# Go Concurrency Examples

A Go 1.22+ module that implements and tests a set of practical concurrency patterns and coordination primitives.

The codebase is organized as small, focused introductory examples such as duplicate request suppression, inventory, bank accounts, semaphores, connection pools, worker pools, and pipelines. Each example demonstrates a concurrency technique using the Go standard library and is covered by automated tests.

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
| [`singleflight`](singleflight/) | Duplicate request suppression using a mutex-protected in-flight map and `sync.WaitGroup` result sharing |
| [`inventory`](inventory/) | Oversell prevention using a mutex to protect shared map state and atomic read-modify-write stock changes |
| [`bankaccount`](bankaccount/) | Account invariants using mutexes, ordered multi-account locking, and atomic internal identity |
| [`semaphore`](semaphore/) | Bounded concurrency using a buffered channel as a counting semaphore with blocking, try, and cancellable acquisition |
| [`connectionpool`](connectionpool/) | Resource borrowing using mutex-protected pool state, FIFO per-waiter channels, wait limits, and context timeouts |
| [`workerpool`](workerpool/) | Fixed-concurrency batch execution using worker goroutines, job channels, result collection, and context cancellation |
| [`pipeline`](pipeline/) | Cancellable staged processing that communicates by passing values through channels instead of coordinating shared memory |

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
