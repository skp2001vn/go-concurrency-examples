// Package inventory keeps item stock correct under concurrent buyers.
//
// Use it for examples where many callers compete for a limited quantity of the
// same item and the store must avoid overselling.
package inventory
