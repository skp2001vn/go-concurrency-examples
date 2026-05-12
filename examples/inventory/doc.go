// Package inventory keeps item stock correct when many buyers act at the same
// time.
//
// The business logic is a small store inventory: callers add stock, remove
// stock for purchases, and check availability while the store prevents empty
// item IDs, invalid quantities, overselling, and stock overflow.
//
// The example uses a mutex because the stock map is shared state. Add and
// Remove need read-modify-write sequences to behave as one atomic operation;
// without the lock, concurrent purchases could both see the same available
// stock and oversell. This technique keeps the API simple while preserving the
// store's stock invariants under concurrent access.
package inventory
