package keyedlock

import (
	"errors"
	"sync"
)

var (
	// ErrUninitialized means the locker was used before New created it.
	ErrUninitialized = errors.New("keyedlock: locker is not initialized")

	// ErrEmptyKey means a lock was requested without a business key.
	ErrEmptyKey = errors.New("keyedlock: key is empty")
)

// Unlock releases a key lock acquired by Lock.
//
// Unlock is safe to call more than once.
type Unlock func()

// Locker provides a separate mutex for each active business key.
//
// Lock blocks only when another caller already holds the same key. Work for
// different keys can proceed at the same time.
//
// A Locker is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Locker.
type Locker struct {
	mu    sync.Mutex
	locks map[string]*entry
}

type entry struct {
	mu   sync.Mutex
	refs int
}

// New creates an empty keyed lock registry.
func New() *Locker {
	return &Locker{
		locks: make(map[string]*entry),
	}
}

// Lock acquires the lock for key and returns a release function.
//
// Lock returns ErrEmptyKey when key is empty and ErrUninitialized when the
// locker was not created with New.
func (l *Locker) Lock(key string) (Unlock, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}
	if l == nil || l.locks == nil {
		return nil, ErrUninitialized
	}

	e := l.acquireEntry(key)
	e.mu.Lock()

	var once sync.Once
	return func() {
		once.Do(func() {
			e.mu.Unlock()
			l.releaseEntry(key, e)
		})
	}, nil
}

// ActiveKeys reports how many keys are currently held or waiting.
func (l *Locker) ActiveKeys() int {
	if l == nil || l.locks == nil {
		return 0
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.locks)
}

func (l *Locker) acquireEntry(key string) *entry {
	l.mu.Lock()
	defer l.mu.Unlock()

	e := l.locks[key]
	if e == nil {
		e = &entry{}
		l.locks[key] = e
	}
	e.refs++
	return e
}

func (l *Locker) releaseEntry(key string, e *entry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	e.refs--
	if e.refs == 0 && l.locks[key] == e {
		delete(l.locks, key)
	}
}
