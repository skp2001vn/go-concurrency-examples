// Package pipeline shows how to process a batch through cancellable channel
// stages.
//
// Use it for work such as validating records, transforming accepted items, and
// collecting the final results while allowing the caller to stop the whole
// pipeline with a context.
package pipeline
