package bankaccount

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

var (
	// ErrEmptyID means an account was created without a caller-visible ID.
	ErrEmptyID = errors.New("account id is empty")

	// ErrUninitialized means an account was used before New created it.
	ErrUninitialized = errors.New("account is not initialized")

	// ErrInvalidAmount means a deposit, withdrawal, or transfer amount was not
	// positive.
	ErrInvalidAmount = errors.New("amount must be positive")

	// ErrInsufficientFunds means an account does not have enough balance for the
	// requested withdrawal or transfer.
	ErrInsufficientFunds = errors.New("insufficient funds")

	// ErrBalanceOverflow means a deposit or transfer would exceed the largest
	// supported account balance.
	ErrBalanceOverflow = errors.New("balance would overflow")

	// ErrNilAccount means an operation was requested with a missing account.
	ErrNilAccount = errors.New("account is nil")

	// ErrSameAccount means a transfer used the same account as both sender and
	// receiver.
	ErrSameAccount = errors.New("cannot transfer to the same account")
)

var nextSequence atomic.Uint64

// Account stores one bank account balance in integer cents.
//
// Use an Account to apply deposits, withdrawals, and transfers without losing
// updates when multiple goroutines operate on the same account. Balance reports
// a consistent value at the moment it is called.
//
// An Account is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for deposits, withdrawals, or transfers. Call New
// before using an Account.
type Account struct {
	id      string
	seq     uint64
	mu      sync.Mutex
	balance int64
}

// New creates an account with id and openingBalanceCents.
//
// New returns an error when id is empty or openingBalanceCents is negative.
func New(id string, openingBalanceCents int64) (*Account, error) {
	if id == "" {
		return nil, ErrEmptyID
	}
	if openingBalanceCents < 0 {
		return nil, fmt.Errorf("opening balance must be non-negative: %d", openingBalanceCents)
	}

	return &Account{
		id:      id,
		seq:     nextSequence.Add(1),
		balance: openingBalanceCents,
	}, nil
}

// ID reports the caller-visible account ID.
func (a *Account) ID() string {
	if a == nil {
		return ""
	}

	return a.id
}

// Balance reports the current account balance in integer cents.
func (a *Account) Balance() int64 {
	if a == nil || !a.ready() {
		return 0
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	return a.balance
}

// Deposit adds amountCents to the account balance.
//
// Deposit returns ErrInvalidAmount when amountCents is not positive and
// ErrBalanceOverflow when the deposit would exceed the largest supported
// balance.
func (a *Account) Deposit(amountCents int64) error {
	if amountCents <= 0 {
		return ErrInvalidAmount
	}
	if a == nil {
		return ErrNilAccount
	}
	if !a.ready() {
		return ErrUninitialized
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if wouldOverflow(a.balance, amountCents) {
		return ErrBalanceOverflow
	}

	a.balance += amountCents
	return nil
}

// Withdraw subtracts amountCents from the account balance.
//
// Withdraw returns ErrInvalidAmount when amountCents is not positive and
// ErrInsufficientFunds when the account does not have enough balance.
func (a *Account) Withdraw(amountCents int64) error {
	if amountCents <= 0 {
		return ErrInvalidAmount
	}
	if a == nil {
		return ErrNilAccount
	}
	if !a.ready() {
		return ErrUninitialized
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.balance < amountCents {
		return ErrInsufficientFunds
	}

	a.balance -= amountCents
	return nil
}

// Transfer moves amountCents from one account to another.
//
// Transfer locks both accounts before changing either balance, so callers never
// observe a partial transfer. Concurrent transfers between the same accounts are
// ordered consistently to avoid deadlock.
func Transfer(from *Account, to *Account, amountCents int64) error {
	if amountCents <= 0 {
		return ErrInvalidAmount
	}
	if from == nil || to == nil {
		return ErrNilAccount
	}
	if !from.ready() || !to.ready() {
		return ErrUninitialized
	}
	if from == to {
		return ErrSameAccount
	}

	first, second := lockOrder(from, to)
	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()

	if from.balance < amountCents {
		return ErrInsufficientFunds
	}
	if wouldOverflow(to.balance, amountCents) {
		return ErrBalanceOverflow
	}

	from.balance -= amountCents
	to.balance += amountCents
	return nil
}

func lockOrder(a *Account, b *Account) (*Account, *Account) {
	if a.seq < b.seq {
		return a, b
	}

	return b, a
}

func (a *Account) ready() bool {
	return a.seq != 0 && a.id != ""
}

func wouldOverflow(current int64, delta int64) bool {
	return delta > math.MaxInt64-current
}
