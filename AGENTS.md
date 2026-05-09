# AGENTS.md

## Purpose

This repository contains small, focused Go concurrency examples for practice, learning, and interview preparation.

Each example should remain:
- self-contained
- easy to read
- correct under concurrent access
- backed by automated tests

## Tech Stack

- Go 1.22+
- Standard library first

## Project Structure

- `<example>/`
  - reusable package code and package tests
- `cmd/<example>/`
  - runnable demo for the matching example, when useful
- `README.md`
  - high-level project overview and example list

## Implementation Guidelines

- Keep each example package focused on one problem or concurrency pattern.
- Prefer clear standard-library concurrency primitives such as:
  - goroutines
  - channels
  - `context`
  - `sync`
  - `sync/atomic`
- Favor correctness and readability over cleverness.
- Keep APIs minimal and idiomatic for Go.
- Use descriptive package names without underscores or hyphens.
- Return errors instead of panicking for normal failure paths.
- Avoid unnecessary dependencies for small examples.

## Testing Guidelines

- Every new example should include dedicated `go test` coverage.
- Tests should validate normal behavior, edge cases, and concurrency coordination behavior.
- Prefer deterministic tests over timing-sensitive tests.
- Use short timeout-based guards only to prevent a hung test.
- After changes, run:

```bash
go test ./...
```

## README Maintenance

- Update `README.md` when adding a new example.
- Keep the implemented example table in sync with the repository.
