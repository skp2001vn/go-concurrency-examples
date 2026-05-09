# Go Concurrency Examples

Small, focused Go examples for practicing concurrency patterns and building blocks.

The first example is `connectionpool`, a fixed-size pool that demonstrates guarded shared state, FIFO waiter coordination, context-based timeout/cancellation, and a maximum waiter limit.

## Requirements

- Go 1.22+

## Run

```bash
go test ./...
go run ./cmd/connectionpool
```

## Implemented Examples

| Example | What it demonstrates |
| --- | --- |
| `connectionpool` | Connection acquisition and release with context timeout, FIFO waiter order, and wait limits |

## Project Structure

```text
connectionpool/        reusable example package and tests
cmd/connectionpool/    runnable demo for the example
```

## Naming

The repository name `go-concurrency-examples` is a good fit: it is lowercase, readable, specific, and matches the module path:

```text
github.com/skp2001vn/go-concurrency-examples
```
