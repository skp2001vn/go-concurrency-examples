// Package connectionpool shares a limited set of reusable connections under
// high demand.
//
// The business logic is a small resource pool: callers borrow connections,
// return them after use, wait when all connections are busy, time out when they
// cannot wait forever, and fail fast when too many callers are already waiting.
//
// The example uses a mutex because idle connections and the wait queue are
// shared pool state. Each waiting caller gets its own channel so Release can
// hand a connection directly to the next caller in FIFO order. Context
// cancellation lets callers leave the wait queue when their request is no
// longer useful. These techniques protect scarce resources while keeping
// waiting behavior explicit and predictable.
package connectionpool
