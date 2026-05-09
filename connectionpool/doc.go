// Package connectionpool shares a limited set of reusable connections.
//
// Use it when connections are expensive or limited, such as database or service
// connections, and callers need to borrow one, wait briefly, or fail fast when
// demand is already too high.
package connectionpool
