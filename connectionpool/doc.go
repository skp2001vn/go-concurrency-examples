// Package connectionpool shares a limited set of reusable connections.
//
// Use it when connections are expensive or limited, such as database or service
// connections, and callers need to borrow one, wait briefly, or fail fast when
// demand is already too high. The example teaches combining mutex-protected
// state with per-waiter channels for FIFO handoff, wait limits, and context
// cancellation.
package connectionpool
