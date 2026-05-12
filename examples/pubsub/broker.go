package pubsub

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrUninitialized means the broker was used before New created it.
	ErrUninitialized = errors.New("pubsub: broker is not initialized")

	// ErrClosed means the broker is closed and no longer accepts new messages or
	// subscribers.
	ErrClosed = errors.New("pubsub: broker is closed")
)

// Unsubscribe removes a subscriber from a Broker.
//
// Unsubscribe is safe to call more than once. After it returns, the subscriber
// channel is closed and will not receive future messages.
type Unsubscribe func()

// Broker broadcasts each published message to all active subscribers.
//
// Subscribe creates a subscriber channel owned by the Broker. Publish sends the
// message to every active subscriber and blocks until each subscriber channel
// accepts the message. Close closes all subscriber channels and prevents future
// publications.
//
// A Broker is safe for concurrent use by multiple goroutines.
//
// The zero value is not ready for use. Call New before calling methods on a
// Broker.
type Broker[T any] struct {
	mu          sync.RWMutex
	subscribers map[int]*subscriber[T]
	nextID      int
	buffer      int
	done        chan struct{}
	closed      bool
}

type subscriber[T any] struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	ch     chan T
	done   chan struct{}
	closed bool
}

// New creates a broker whose subscriber channels use buffer slots.
//
// New returns an error when buffer is negative.
func New[T any](buffer int) (*Broker[T], error) {
	if buffer < 0 {
		return nil, fmt.Errorf("buffer must be non-negative: %d", buffer)
	}

	return &Broker[T]{
		subscribers: make(map[int]*subscriber[T]),
		buffer:      buffer,
		done:        make(chan struct{}),
	}, nil
}

// Subscribe registers a new subscriber for future messages.
//
// Subscribe returns the subscriber's receive-only channel and an Unsubscribe
// function. It returns ErrClosed if the broker is closed.
func (b *Broker[T]) Subscribe() (<-chan T, Unsubscribe, error) {
	if err := b.ready(); err != nil {
		return nil, nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, nil, ErrClosed
	}

	id := b.nextID
	b.nextID++
	sub := &subscriber[T]{
		ch:   make(chan T, b.buffer),
		done: make(chan struct{}),
	}
	b.subscribers[id] = sub

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.unsubscribe(id)
		})
	}

	return sub.ch, unsubscribe, nil
}

// Publish sends message to every active subscriber.
//
// Publish blocks until each subscriber channel accepts the message. It returns
// ErrClosed when the broker is closed.
func (b *Broker[T]) Publish(message T) error {
	subscribers, err := b.snapshot()
	if err != nil {
		return err
	}

	for _, sub := range subscribers {
		if !sub.send(message, b.done) {
			if b.isClosed() {
				return ErrClosed
			}
			continue
		}
	}

	return nil
}

// Close closes all subscriber channels and prevents future publications.
//
// Close is safe to call more than once.
func (b *Broker[T]) Close() {
	if b == nil || b.subscribers == nil {
		return
	}

	var subscribers []*subscriber[T]
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}

	b.closed = true
	close(b.done)
	for id, sub := range b.subscribers {
		delete(b.subscribers, id)
		subscribers = append(subscribers, sub)
	}
	b.mu.Unlock()

	for _, sub := range subscribers {
		sub.close()
	}
}

// SubscriberCount reports how many subscribers are currently active.
func (b *Broker[T]) SubscriberCount() int {
	if b == nil || b.subscribers == nil {
		return 0
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.subscribers)
}

func (b *Broker[T]) snapshot() ([]*subscriber[T], error) {
	if err := b.ready(); err != nil {
		return nil, err
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return nil, ErrClosed
	}

	subscribers := make([]*subscriber[T], 0, len(b.subscribers))
	for _, sub := range b.subscribers {
		subscribers = append(subscribers, sub)
	}

	return subscribers, nil
}

func (b *Broker[T]) unsubscribe(id int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub, ok := b.subscribers[id]
	if !ok {
		return
	}

	delete(b.subscribers, id)
	sub.close()
}

func (b *Broker[T]) ready() error {
	if b == nil || b.subscribers == nil {
		return ErrUninitialized
	}

	return nil
}

func (b *Broker[T]) isClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.closed
}

func (s *subscriber[T]) send(message T, brokerDone <-chan struct{}) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()

	select {
	case <-s.done:
		return false
	case <-brokerDone:
		return false
	case s.ch <- message:
		return true
	}
}

func (s *subscriber[T]) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}

	s.closed = true
	close(s.done)
	s.mu.Unlock()

	s.wg.Wait()
	close(s.ch)
}
