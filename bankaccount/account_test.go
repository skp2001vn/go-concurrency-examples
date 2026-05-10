package bankaccount

import (
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

// TestDepositAndWithdraw verifies that normal account activity updates the
// caller-visible balance.
func TestDepositAndWithdraw(t *testing.T) {
	account := mustAccount(t, "checking", 1_000)

	if err := account.Deposit(250); err != nil {
		t.Fatalf("deposit: %v", err)
	}
	if err := account.Withdraw(400); err != nil {
		t.Fatalf("withdraw: %v", err)
	}

	if got := account.Balance(); got != 850 {
		t.Fatalf("balance = %d, want 850", got)
	}
}

// TestWithdrawRejectsOverdraft verifies that withdrawals cannot make the
// account balance negative.
func TestWithdrawRejectsOverdraft(t *testing.T) {
	account := mustAccount(t, "checking", 100)

	err := account.Withdraw(101)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("withdraw error = %v, want %v", err, ErrInsufficientFunds)
	}
	if got := account.Balance(); got != 100 {
		t.Fatalf("balance = %d, want 100", got)
	}
}

// TestRejectsInvalidAmounts verifies that zero and negative money movements are
// rejected before balances are changed.
func TestRejectsInvalidAmounts(t *testing.T) {
	account := mustAccount(t, "checking", 100)

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "deposit zero", run: func() error { return account.Deposit(0) }},
		{name: "deposit negative", run: func() error { return account.Deposit(-1) }},
		{name: "withdraw zero", run: func() error { return account.Withdraw(0) }},
		{name: "withdraw negative", run: func() error { return account.Withdraw(-1) }},
		{name: "transfer zero", run: func() error { return Transfer(account, mustAccount(t, "savings", 0), 0) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if !errors.Is(err, ErrInvalidAmount) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidAmount)
			}
			if got := account.Balance(); got != 100 {
				t.Fatalf("balance = %d, want 100", got)
			}
		})
	}
}

// TestConcurrentDepositsPreserveAllMoney verifies that simultaneous deposits
// do not lose updates.
func TestConcurrentDepositsPreserveAllMoney(t *testing.T) {
	const (
		deposits = 100
		amount   = 25
	)

	account := mustAccount(t, "checking", 0)
	var wg sync.WaitGroup

	for i := 0; i < deposits; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := account.Deposit(amount); err != nil {
				t.Errorf("deposit: %v", err)
			}
		}()
	}

	wg.Wait()

	if got, want := account.Balance(), int64(deposits*amount); got != want {
		t.Fatalf("balance = %d, want %d", got, want)
	}
}

// TestConcurrentWithdrawalsNeverOverdraw verifies that competing withdrawals
// cannot spend more money than the account holds.
func TestConcurrentWithdrawalsNeverOverdraw(t *testing.T) {
	const withdrawals = 10

	account := mustAccount(t, "checking", 500)
	errs := make(chan error, withdrawals)
	var wg sync.WaitGroup

	for i := 0; i < withdrawals; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- account.Withdraw(100)
		}()
	}

	wg.Wait()
	close(errs)

	var succeeded int
	var failed int
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrInsufficientFunds):
			failed++
		default:
			t.Fatalf("withdraw error = %v", err)
		}
	}

	if succeeded != 5 {
		t.Fatalf("successful withdrawals = %d, want 5", succeeded)
	}
	if failed != 5 {
		t.Fatalf("failed withdrawals = %d, want 5", failed)
	}
	if got := account.Balance(); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

// TestTransferMovesMoneyAtomically verifies that a transfer changes both
// balances as one operation.
func TestTransferMovesMoneyAtomically(t *testing.T) {
	checking := mustAccount(t, "checking", 1_000)
	savings := mustAccount(t, "savings", 200)

	if err := Transfer(checking, savings, 300); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	if got := checking.Balance(); got != 700 {
		t.Fatalf("checking balance = %d, want 700", got)
	}
	if got := savings.Balance(); got != 500 {
		t.Fatalf("savings balance = %d, want 500", got)
	}
}

// TestTransferRejectsOverdraft verifies that a failed transfer leaves both
// accounts unchanged.
func TestTransferRejectsOverdraft(t *testing.T) {
	checking := mustAccount(t, "checking", 100)
	savings := mustAccount(t, "savings", 200)

	err := Transfer(checking, savings, 101)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("transfer error = %v, want %v", err, ErrInsufficientFunds)
	}

	if got := checking.Balance(); got != 100 {
		t.Fatalf("checking balance = %d, want 100", got)
	}
	if got := savings.Balance(); got != 200 {
		t.Fatalf("savings balance = %d, want 200", got)
	}
}

// TestDepositRejectsOverflow verifies that deposits cannot wrap the account
// balance around to a negative value.
func TestDepositRejectsOverflow(t *testing.T) {
	account := mustAccount(t, "checking", math.MaxInt64)

	err := account.Deposit(1)
	if !errors.Is(err, ErrBalanceOverflow) {
		t.Fatalf("deposit error = %v, want %v", err, ErrBalanceOverflow)
	}
	if got := account.Balance(); got != math.MaxInt64 {
		t.Fatalf("balance = %d, want %d", got, int64(math.MaxInt64))
	}
}

// TestTransferRejectsOverflow verifies that a failed transfer leaves both
// accounts unchanged when the destination cannot accept more money.
func TestTransferRejectsOverflow(t *testing.T) {
	checking := mustAccount(t, "checking", 100)
	savings := mustAccount(t, "savings", math.MaxInt64)

	err := Transfer(checking, savings, 1)
	if !errors.Is(err, ErrBalanceOverflow) {
		t.Fatalf("transfer error = %v, want %v", err, ErrBalanceOverflow)
	}
	if got := checking.Balance(); got != 100 {
		t.Fatalf("checking balance = %d, want 100", got)
	}
	if got := savings.Balance(); got != math.MaxInt64 {
		t.Fatalf("savings balance = %d, want %d", got, int64(math.MaxInt64))
	}
}

// TestConcurrentOppositeTransfersDoNotDeadlock verifies that transfers lock
// accounts in a consistent order.
func TestConcurrentOppositeTransfersDoNotDeadlock(t *testing.T) {
	checking := mustAccount(t, "checking", 1_000)
	savings := mustAccount(t, "savings", 1_000)

	done := make(chan error, 2)
	go func() {
		done <- Transfer(checking, savings, 100)
	}()
	go func() {
		done <- Transfer(savings, checking, 200)
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("transfer failed: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for transfers")
		}
	}

	if got := checking.Balance() + savings.Balance(); got != 2_000 {
		t.Fatalf("total balance = %d, want 2000", got)
	}
}

// TestConcurrentTransfersPreserveTotalBalance verifies that many transfers
// between accounts do not create or lose money.
func TestConcurrentTransfersPreserveTotalBalance(t *testing.T) {
	checking := mustAccount(t, "checking", 10_000)
	savings := mustAccount(t, "savings", 5_000)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := Transfer(checking, savings, 10); err != nil {
				t.Errorf("checking to savings: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := Transfer(savings, checking, 5); err != nil {
				t.Errorf("savings to checking: %v", err)
			}
		}()
	}

	wg.Wait()

	if got := checking.Balance() + savings.Balance(); got != 15_000 {
		t.Fatalf("total balance = %d, want 15000", got)
	}
}

// TestNewRejectsInvalidAccount verifies that account creation rejects invalid
// caller data.
func TestNewRejectsInvalidAccount(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		balance int64
		want    error
	}{
		{name: "empty id", id: "", balance: 0, want: ErrEmptyID},
		{name: "negative balance", id: "checking", balance: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, err := New(tt.id, tt.balance)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if account != nil {
				t.Fatalf("account = %v, want nil", account)
			}
		})
	}
}

// TestUninitializedAccountReturnsError verifies that callers get a clear error
// when they bypass New.
func TestUninitializedAccountReturnsError(t *testing.T) {
	var account Account
	ready := mustAccount(t, "ready", 100)

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "deposit", run: func() error { return account.Deposit(1) }},
		{name: "withdraw", run: func() error { return account.Withdraw(1) }},
		{name: "transfer from", run: func() error { return Transfer(&account, ready, 1) }},
		{name: "transfer to", run: func() error { return Transfer(ready, &account, 1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if !errors.Is(err, ErrUninitialized) {
				t.Fatalf("error = %v, want %v", err, ErrUninitialized)
			}
		})
	}
	if got := account.Balance(); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func mustAccount(t *testing.T, id string, openingBalanceCents int64) *Account {
	t.Helper()

	account, err := New(id, openingBalanceCents)
	if err != nil {
		t.Fatalf("new account: %v", err)
	}

	return account
}
