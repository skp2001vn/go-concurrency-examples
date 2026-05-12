package batcher

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrNilInput means batching was requested without an input stream.
var ErrNilInput = errors.New("batcher: input channel is nil")

// Batch groups items from input into output batches.
//
// Batch owns and closes the returned output channel. The caller owns input and
// should close it when no more items will arrive. A pending batch is emitted
// when it reaches maxSize, when maxDelay passes after the first pending item,
// or when input closes. If ctx is canceled, batching stops without emitting more
// batches. A nil context is treated as context.Background.
//
// Batch returns an error when input is nil, maxSize is not positive, or
// maxDelay is not positive.
func Batch[T any](ctx context.Context, input <-chan T, maxSize int, maxDelay time.Duration) (<-chan []T, error) {
	if input == nil {
		return nil, ErrNilInput
	}
	if maxSize <= 0 {
		return nil, fmt.Errorf("max size must be positive: %d", maxSize)
	}
	if maxDelay <= 0 {
		return nil, fmt.Errorf("max delay must be positive: %s", maxDelay)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	output := make(chan []T)
	go run(ctx, input, output, maxSize, maxDelay)

	return output, nil
}

func run[T any](ctx context.Context, input <-chan T, output chan<- []T, maxSize int, maxDelay time.Duration) {
	defer close(output)

	timer := time.NewTimer(maxDelay)
	stopTimer(timer)
	defer timer.Stop()

	var timerC <-chan time.Time
	batch := make([]T, 0, maxSize)

	startTimer := func() {
		if timerC != nil {
			return
		}
		timer.Reset(maxDelay)
		timerC = timer.C
	}

	stopActiveTimer := func() {
		if timerC == nil {
			return
		}
		stopTimer(timer)
		timerC = nil
	}

	flush := func() bool {
		if len(batch) == 0 {
			return true
		}

		items := append([]T(nil), batch...)
		batch = batch[:0]
		stopActiveTimer()

		select {
		case output <- items:
			return true
		case <-ctx.Done():
			return false
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-input:
			if !ok {
				flush()
				return
			}

			batch = append(batch, item)
			if len(batch) == 1 {
				startTimer()
			}
			if len(batch) >= maxSize && !flush() {
				return
			}
		case <-timerC:
			timerC = nil
			if !flush() {
				return
			}
		}
	}
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}

	select {
	case <-timer.C:
	default:
	}
}
