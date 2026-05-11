// Package bankaccount keeps account balances correct under concurrent access.
//
// Use it for examples where the important behavior is a business rule, such as
// preventing overdrafts and moving money atomically between accounts, rather
// than a raw concurrency primitive. The example teaches protecting object
// invariants with mutexes, ordering multi-object locks to avoid deadlock, and
// using atomic counters for internal identity.
package bankaccount
