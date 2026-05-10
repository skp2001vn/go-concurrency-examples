package inventory

import (
	"errors"
	"math"
	"sync"
)

var (
	// ErrNilStore means an operation was requested with a missing store.
	ErrNilStore = errors.New("store is nil")

	// ErrEmptySKU means an operation was requested without an item identifier.
	ErrEmptySKU = errors.New("sku is empty")

	// ErrInvalidQuantity means stock was added or removed with a non-positive
	// quantity.
	ErrInvalidQuantity = errors.New("quantity must be positive")

	// ErrInsufficientStock means the store does not have enough available stock
	// for the requested removal.
	ErrInsufficientStock = errors.New("insufficient stock")

	// ErrStockOverflow means adding stock would exceed the largest supported
	// quantity for one item.
	ErrStockOverflow = errors.New("stock would overflow")
)

// Store tracks available stock by SKU.
//
// Use a Store to add stock, remove stock for purchases, and check current
// availability without allowing concurrent callers to oversell an item.
//
// A Store is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call NewStore before using a Store.
type Store struct {
	mu    sync.Mutex
	stock map[string]int
}

// NewStore creates an empty inventory store.
func NewStore() *Store {
	return &Store{
		stock: make(map[string]int),
	}
}

// Add increases available stock for sku by quantity.
//
// Add returns ErrEmptySKU when sku is empty, ErrInvalidQuantity when quantity is
// not positive, and ErrStockOverflow when the new stock count would be too
// large.
func (s *Store) Add(sku string, quantity int) error {
	if err := validateChange(s, sku, quantity); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if quantity > math.MaxInt-s.stock[sku] {
		return ErrStockOverflow
	}

	s.stock[sku] += quantity
	return nil
}

// Remove decreases available stock for sku by quantity.
//
// Remove is used for purchases or other stock-consuming work. It returns
// ErrInsufficientStock when the requested quantity is larger than the available
// stock, and leaves the stock unchanged.
func (s *Store) Remove(sku string, quantity int) error {
	if err := validateChange(s, sku, quantity); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stock[sku] < quantity {
		return ErrInsufficientStock
	}

	s.stock[sku] -= quantity
	return nil
}

// Available reports how many units of sku are available now.
func (s *Store) Available(sku string) int {
	if s == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.stock[sku]
}

func validateChange(s *Store, sku string, quantity int) error {
	if s == nil {
		return ErrNilStore
	}
	if s.stock == nil {
		return ErrNilStore
	}
	if sku == "" {
		return ErrEmptySKU
	}
	if quantity <= 0 {
		return ErrInvalidQuantity
	}

	return nil
}
