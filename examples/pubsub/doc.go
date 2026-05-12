// Package pubsub broadcasts messages from publishers to multiple subscribers.
//
// The business logic is a small notification broker: callers subscribe to
// receive future messages, publishers send each message to every active
// subscriber, subscribers can leave, and closing the broker wakes all remaining
// subscribers.
//
// The example uses sync.RWMutex because the subscriber registry is shared state
// with many read-only lookups and occasional writes. Publish takes a read lock
// only long enough to snapshot the current subscribers, while Subscribe,
// Unsubscribe, and Close take the write lock to change membership. Publish then
// sends messages outside the registry lock so a slow subscriber does not block
// subscription changes. Each subscriber receives messages on its own channel,
// and the broker owns closing those channels. These techniques support
// one-to-many fan-out, dynamic subscriber lifecycles, backpressure, and explicit
// channel ownership.
package pubsub
