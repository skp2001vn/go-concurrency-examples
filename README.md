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

| Example | Business scenario | Technique |
| --- | --- | --- |
| [`singleflight`](singleflight/) | Suppress duplicate requests so concurrent callers share one expensive result by key | in-flight map + wait group |
| [`inventory`](inventory/) | Keep stock counts correct when many buyers try to purchase limited inventory | mutex-protected map |
| [`bankaccount`](bankaccount/) | Keep account balances correct during deposits, withdrawals, and transfers | ordered locks |
| [`semaphore`](semaphore/) | Limit how many callers may use a shared resource at the same time | buffered channel |
| [`connectionpool`](connectionpool/) | Borrow and return a limited set of reusable connections under high demand | mutex + waiter channels |
| [`workerpool`](workerpool/) | Process a batch of independent jobs without overwhelming a system | workers + job channel |
| [`pipeline`](pipeline/) | Validate, filter, transform, and collect a batch while allowing cancellation | channel pipeline |

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
