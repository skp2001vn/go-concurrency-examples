# Go Concurrency Examples

A Go 1.22+ module that implements and tests a set of practical concurrency patterns and coordination primitives.

The codebase is organized as small, focused introductory examples such as inventory, bank accounts, semaphores, connection pools, and worker pools. Each example demonstrates a concurrency technique using the Go standard library and is covered by automated tests.

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
| [`inventory`](inventory/) | Concurrent-safe stock changes with shared map protection and oversell prevention |
| [`bankaccount`](bankaccount/) | Concurrent-safe deposits, withdrawals, and deadlock-free transfers with business invariants |
| [`semaphore`](semaphore/) | Bounded concurrency with blocking acquisition, non-blocking attempts, and context cancellation |
| [`connectionpool`](connectionpool/) | Connection acquisition and release with context timeout, FIFO waiter order, and wait limits |
| [`workerpool`](workerpool/) | Fixed-concurrency task execution with result collection and context cancellation |

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
