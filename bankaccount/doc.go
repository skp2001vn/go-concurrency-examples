// Package bankaccount keeps account balances correct during deposits,
// withdrawals, and transfers.
//
// The business logic is a small banking model: callers can open accounts,
// deposit funds, withdraw funds, and transfer money between accounts while
// preserving rules such as no overdrafts, no partial transfers, and no balance
// overflow.
//
// The example uses mutexes because each account balance is shared state that
// must be read and updated atomically. Transfers lock both accounts in a stable
// order so concurrent transfers cannot deadlock. An atomic counter gives each
// account an internal ordering identity without needing a global lock. Together,
// these techniques keep the API safe for concurrent callers while preserving
// the business invariants.
package bankaccount
