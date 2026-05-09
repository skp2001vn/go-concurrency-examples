# Go Concurrency Examples

Small, focused Go examples for practicing concurrency patterns and building blocks.

The examples cover practical patterns such as resource pooling, bounded worker execution, context cancellation, and deterministic coordination tests.

## Requirements

- Go 1.22+

## Run

```bash
go test ./...
```

## Implemented Examples

| Example | What it demonstrates |
| --- | --- |
| `connectionpool` | Connection acquisition and release with context timeout, FIFO waiter order, and wait limits |
| `workerpool` | Fixed-concurrency task execution with result collection and context cancellation |

## Project Structure

```text
connectionpool/        reusable example package and tests
workerpool/            reusable example package and tests
```

## Naming

The repository name `go-concurrency-examples` is a good fit: it is lowercase, readable, specific, and matches the module path:

```text
github.com/skp2001vn/go-concurrency-examples
```
