package inventory

import (
	"errors"
	"math"
	"sync"
	"testing"
)

// TestAddAndRemoveStock verifies that normal stock changes update caller-visible
// availability.
func TestAddAndRemoveStock(t *testing.T) {
	store := NewStore()

	if err := store.Add("book", 10); err != nil {
		t.Fatalf("add stock: %v", err)
	}
	if err := store.Remove("book", 3); err != nil {
		t.Fatalf("remove stock: %v", err)
	}

	if got := store.Available("book"); got != 7 {
		t.Fatalf("available stock = %d, want 7", got)
	}
}

// TestRemoveRejectsOversell verifies that buyers cannot remove more stock than
// the store has available.
func TestRemoveRejectsOversell(t *testing.T) {
	store := NewStore()
	if err := store.Add("book", 2); err != nil {
		t.Fatalf("add stock: %v", err)
	}

	err := store.Remove("book", 3)
	if !errors.Is(err, ErrInsufficientStock) {
		t.Fatalf("remove error = %v, want %v", err, ErrInsufficientStock)
	}
	if got := store.Available("book"); got != 2 {
		t.Fatalf("available stock = %d, want 2", got)
	}
}

// TestRejectsInvalidInputs verifies that bad caller input is rejected before
// stock changes.
func TestRejectsInvalidInputs(t *testing.T) {
	store := NewStore()

	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{name: "empty sku add", run: func() error { return store.Add("", 1) }, want: ErrEmptySKU},
		{name: "empty sku remove", run: func() error { return store.Remove("", 1) }, want: ErrEmptySKU},
		{name: "zero add", run: func() error { return store.Add("book", 0) }, want: ErrInvalidQuantity},
		{name: "negative add", run: func() error { return store.Add("book", -1) }, want: ErrInvalidQuantity},
		{name: "zero remove", run: func() error { return store.Remove("book", 0) }, want: ErrInvalidQuantity},
		{name: "negative remove", run: func() error { return store.Remove("book", -1) }, want: ErrInvalidQuantity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if got := store.Available("book"); got != 0 {
				t.Fatalf("available stock = %d, want 0", got)
			}
		})
	}
}

// TestAddRejectsOverflow verifies that stock cannot wrap around to a negative
// quantity.
func TestAddRejectsOverflow(t *testing.T) {
	store := NewStore()
	if err := store.Add("book", math.MaxInt); err != nil {
		t.Fatalf("add max stock: %v", err)
	}

	err := store.Add("book", 1)
	if !errors.Is(err, ErrStockOverflow) {
		t.Fatalf("add error = %v, want %v", err, ErrStockOverflow)
	}
	if got := store.Available("book"); got != math.MaxInt {
		t.Fatalf("available stock = %d, want %d", got, math.MaxInt)
	}
}

// TestConcurrentAddPreservesAllStock verifies that simultaneous restocks do not
// lose updates.
func TestConcurrentAddPreservesAllStock(t *testing.T) {
	const (
		restocks = 100
		quantity = 3
	)

	store := NewStore()
	var wg sync.WaitGroup

	for i := 0; i < restocks; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := store.Add("book", quantity); err != nil {
				t.Errorf("add stock: %v", err)
			}
		}()
	}

	wg.Wait()

	if got, want := store.Available("book"), restocks*quantity; got != want {
		t.Fatalf("available stock = %d, want %d", got, want)
	}
}

// TestConcurrentRemoveDoesNotOversell verifies that competing buyers cannot
// consume more items than the store has available.
func TestConcurrentRemoveDoesNotOversell(t *testing.T) {
	const (
		stock  = 5
		buyers = 20
	)

	store := NewStore()
	if err := store.Add("book", stock); err != nil {
		t.Fatalf("add stock: %v", err)
	}

	errs := make(chan error, buyers)
	var wg sync.WaitGroup
	for i := 0; i < buyers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.Remove("book", 1)
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
		case errors.Is(err, ErrInsufficientStock):
			failed++
		default:
			t.Fatalf("remove error = %v", err)
		}
	}

	if succeeded != stock {
		t.Fatalf("successful removes = %d, want %d", succeeded, stock)
	}
	if failed != buyers-stock {
		t.Fatalf("failed removes = %d, want %d", failed, buyers-stock)
	}
	if got := store.Available("book"); got != 0 {
		t.Fatalf("available stock = %d, want 0", got)
	}
}

// TestNilStoreReturnsError verifies that callers get a clear error when they
// use a missing or uninitialized store.
func TestNilStoreReturnsError(t *testing.T) {
	var nilStore *Store
	var zero Store

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "nil add", run: func() error { return nilStore.Add("book", 1) }},
		{name: "nil remove", run: func() error { return nilStore.Remove("book", 1) }},
		{name: "zero add", run: func() error { return zero.Add("book", 1) }},
		{name: "zero remove", run: func() error { return zero.Remove("book", 1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if !errors.Is(err, ErrNilStore) {
				t.Fatalf("error = %v, want %v", err, ErrNilStore)
			}
		})
	}
	if got := nilStore.Available("book"); got != 0 {
		t.Fatalf("nil store available stock = %d, want 0", got)
	}
	if got := zero.Available("book"); got != 0 {
		t.Fatalf("zero store available stock = %d, want 0", got)
	}
}
