package pubsub

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestPublishBroadcastsToAllSubscribers verifies that one message is delivered
// to every active subscriber.
func TestPublishBroadcastsToAllSubscribers(t *testing.T) {
	b := mustBroker[string](t, 1)
	defer b.Close()

	first, unsubscribeFirst, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe first: %v", err)
	}
	defer unsubscribeFirst()

	second, unsubscribeSecond, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe second: %v", err)
	}
	defer unsubscribeSecond()

	if err := b.Publish("event"); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	if got := receiveValue(t, first); got != "event" {
		t.Fatalf("first subscriber received %q, want %q", got, "event")
	}
	if got := receiveValue(t, second); got != "event" {
		t.Fatalf("second subscriber received %q, want %q", got, "event")
	}
}

// TestSubscribeReceivesOnlyFutureMessages verifies that new subscribers do not
// receive messages published before they joined.
func TestSubscribeReceivesOnlyFutureMessages(t *testing.T) {
	b := mustBroker[int](t, 1)
	defer b.Close()

	if err := b.Publish(1); err != nil {
		t.Fatalf("publish without subscribers: %v", err)
	}

	ch, unsubscribe, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer unsubscribe()

	select {
	case value := <-ch:
		t.Fatalf("subscriber received old message %d", value)
	default:
	}

	if err := b.Publish(2); err != nil {
		t.Fatalf("publish future message: %v", err)
	}
	if got := receiveValue(t, ch); got != 2 {
		t.Fatalf("received value = %d, want 2", got)
	}
}

// TestUnsubscribeClosesSubscriberChannel verifies that leaving the broker ends
// the subscriber stream and stops future delivery.
func TestUnsubscribeClosesSubscriberChannel(t *testing.T) {
	b := mustBroker[int](t, 1)
	defer b.Close()

	ch, unsubscribe, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if got := b.SubscriberCount(); got != 1 {
		t.Fatalf("subscriber count = %d, want 1", got)
	}

	unsubscribe()
	unsubscribe()

	expectClosed(t, ch)
	if got := b.SubscriberCount(); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
	if err := b.Publish(1); err != nil {
		t.Fatalf("publish after unsubscribe: %v", err)
	}
}

// TestCloseClosesAllSubscriberChannels verifies that closing the broker wakes
// every subscriber.
func TestCloseClosesAllSubscriberChannels(t *testing.T) {
	b := mustBroker[int](t, 1)

	first, _, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe first: %v", err)
	}
	second, _, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe second: %v", err)
	}

	b.Close()
	b.Close()

	expectClosed(t, first)
	expectClosed(t, second)
	if got := b.SubscriberCount(); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}

// TestPublishAfterCloseReturnsError verifies that a closed broker rejects new
// messages.
func TestPublishAfterCloseReturnsError(t *testing.T) {
	b := mustBroker[int](t, 1)
	b.Close()

	err := b.Publish(1)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("publish error = %v, want %v", err, ErrClosed)
	}
}

// TestSubscribeAfterCloseReturnsError verifies that a closed broker rejects new
// subscribers.
func TestSubscribeAfterCloseReturnsError(t *testing.T) {
	b := mustBroker[int](t, 1)
	b.Close()

	ch, unsubscribe, err := b.Subscribe()
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe error = %v, want %v", err, ErrClosed)
	}
	if ch != nil {
		t.Fatalf("channel = %v, want nil", ch)
	}
	if unsubscribe != nil {
		t.Fatal("unsubscribe = non-nil, want nil")
	}
}

// TestPublishBlocksForSlowSubscriber verifies the explicit backpressure policy:
// publishing waits until each subscriber can accept the message.
func TestPublishBlocksForSlowSubscriber(t *testing.T) {
	b := mustBroker[int](t, 0)
	defer b.Close()

	ch, unsubscribe, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer unsubscribe()

	done := make(chan error, 1)
	go func() {
		done <- b.Publish(1)
	}()

	select {
	case err := <-done:
		t.Fatalf("publish finished before subscriber received message: %v", err)
	default:
	}

	if got := receiveValue(t, ch); got != 1 {
		t.Fatalf("received value = %d, want 1", got)
	}
	if err := receiveError(t, done); err != nil {
		t.Fatalf("publish error = %v, want nil", err)
	}
}

// TestUnsubscribeReleasesBlockedPublish verifies that removing a slow
// subscriber lets an in-flight publish finish without closing the broker.
func TestUnsubscribeReleasesBlockedPublish(t *testing.T) {
	b := mustBroker[int](t, 0)
	defer b.Close()

	ch, unsubscribe, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- b.Publish(1)
	}()

	select {
	case err := <-done:
		t.Fatalf("publish finished before unsubscribe: %v", err)
	default:
	}

	unsubscribe()
	expectClosed(t, ch)
	if err := receiveError(t, done); err != nil {
		t.Fatalf("publish error = %v, want nil", err)
	}
	if err := b.Publish(2); err != nil {
		t.Fatalf("publish after unsubscribe: %v", err)
	}
}

// TestCloseReleasesBlockedPublish verifies that closing the broker wakes a
// publisher waiting on a slow subscriber.
func TestCloseReleasesBlockedPublish(t *testing.T) {
	b := mustBroker[int](t, 0)

	ch, _, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- b.Publish(1)
	}()

	select {
	case err := <-done:
		t.Fatalf("publish finished before close: %v", err)
	default:
	}

	b.Close()
	expectClosed(t, ch)

	err = receiveError(t, done)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("publish error = %v, want %v", err, ErrClosed)
	}
}

// TestConcurrentPublishersBroadcastMessages verifies that multiple publishers
// can safely broadcast through the same broker.
func TestConcurrentPublishersBroadcastMessages(t *testing.T) {
	const messages = 5
	b := mustBroker[int](t, messages)
	defer b.Close()

	first, unsubscribeFirst, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe first: %v", err)
	}
	defer unsubscribeFirst()

	second, unsubscribeSecond, err := b.Subscribe()
	if err != nil {
		t.Fatalf("subscribe second: %v", err)
	}
	defer unsubscribeSecond()

	var wg sync.WaitGroup
	errs := make(chan error, messages)
	for i := 0; i < messages; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			errs <- b.Publish(value)
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("publish error: %v", err)
		}
	}

	firstMessages := receiveValues(t, first, messages)
	secondMessages := receiveValues(t, second, messages)
	if !sameSet(firstMessages, secondMessages) {
		t.Fatalf("subscriber messages differ: first=%v second=%v", firstMessages, secondMessages)
	}
	if !sameSet(firstMessages, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("messages = %v, want values 0 through 4", firstMessages)
	}
}

// TestBrokerRejectsUninitializedUse verifies that callers get clear errors
// instead of panics when using a zero-value broker.
func TestBrokerRejectsUninitializedUse(t *testing.T) {
	var b Broker[int]

	if err := b.Publish(1); !errors.Is(err, ErrUninitialized) {
		t.Fatalf("publish error = %v, want %v", err, ErrUninitialized)
	}
	ch, unsubscribe, err := b.Subscribe()
	if !errors.Is(err, ErrUninitialized) {
		t.Fatalf("subscribe error = %v, want %v", err, ErrUninitialized)
	}
	if ch != nil {
		t.Fatalf("channel = %v, want nil", ch)
	}
	if unsubscribe != nil {
		t.Fatal("unsubscribe = non-nil, want nil")
	}
	if got := b.SubscriberCount(); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
	b.Close()
}

// TestNewRejectsInvalidBuffer verifies that callers cannot create subscribers
// with negative channel capacity.
func TestNewRejectsInvalidBuffer(t *testing.T) {
	b, err := New[int](-1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if b != nil {
		t.Fatalf("broker = %v, want nil", b)
	}
}

func mustBroker[T any](t *testing.T, buffer int) *Broker[T] {
	t.Helper()

	b, err := New[T](buffer)
	if err != nil {
		t.Fatalf("new broker: %v", err)
	}

	return b
}

func receiveValue[T any](t *testing.T, ch <-chan T) T {
	t.Helper()

	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before value was received")
		}
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for value")
		var zero T
		return zero
	}
}

func receiveValues[T any](t *testing.T, ch <-chan T, count int) []T {
	t.Helper()

	values := make([]T, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, receiveValue(t, ch))
	}

	return values
}

func receiveError(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}

func expectClosed[T any](t *testing.T, ch <-chan T) {
	t.Helper()

	select {
	case value, ok := <-ch:
		if ok {
			t.Fatalf("received value %v, want closed channel", value)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for channel to close")
	}
}

func sameSet[T comparable](a []T, b []T) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[T]int, len(a))
	for _, value := range a {
		counts[value]++
	}
	for _, value := range b {
		counts[value]--
		if counts[value] < 0 {
			return false
		}
	}

	return reflect.DeepEqual(counts, zeroCounts(counts))
}

func zeroCounts[T comparable](counts map[T]int) map[T]int {
	zeros := make(map[T]int, len(counts))
	for value := range counts {
		zeros[value] = 0
	}

	return zeros
}
