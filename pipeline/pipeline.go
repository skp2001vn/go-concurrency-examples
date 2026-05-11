package pipeline

import "context"

// Source streams values to the first stage of a pipeline.
//
// Source owns and closes the returned channel. It stops sending early when ctx
// is canceled. A nil context is treated as context.Background.
func Source(ctx context.Context, values []int) <-chan int {
	ctx = backgroundIfNil(ctx)
	out := make(chan int)

	go func() {
		defer close(out)

		for _, value := range values {
			select {
			case <-ctx.Done():
				return
			case out <- value:
			}
		}
	}()

	return out
}

// Filter forwards values that match keep.
//
// Filter owns and closes the returned channel. It stops when the input channel
// closes or ctx is canceled. A nil keep function accepts every value. A nil
// context is treated as context.Background.
func Filter(ctx context.Context, in <-chan int, keep func(int) bool) <-chan int {
	ctx = backgroundIfNil(ctx)
	out := make(chan int)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-in:
				if !ok {
					return
				}
				if keep != nil && !keep(value) {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case out <- value:
				}
			}
		}
	}()

	return out
}

// Map transforms each value from the input channel.
//
// Map owns and closes the returned channel. It stops when the input channel
// closes or ctx is canceled. A nil mapper returns values unchanged. A nil
// context is treated as context.Background.
func Map(ctx context.Context, in <-chan int, mapper func(int) int) <-chan int {
	ctx = backgroundIfNil(ctx)
	out := make(chan int)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-in:
				if !ok {
					return
				}
				if mapper != nil {
					value = mapper(value)
				}

				select {
				case <-ctx.Done():
					return
				case out <- value:
				}
			}
		}
	}()

	return out
}

// Collect reads values until the input channel closes or ctx is canceled.
//
// Collect does not close the input channel. If ctx is canceled before the input
// channel closes, Collect returns the values received so far and the
// cancellation error. A nil context is treated as context.Background.
func Collect(ctx context.Context, in <-chan int) ([]int, error) {
	ctx = backgroundIfNil(ctx)
	values := []int{}

	for {
		select {
		case <-ctx.Done():
			return values, ctx.Err()
		case value, ok := <-in:
			if !ok {
				if err := ctx.Err(); err != nil {
					return values, err
				}
				return values, nil
			}
			values = append(values, value)
		}
	}
}

// Run processes a batch by filtering positive values and squaring them.
//
// Run demonstrates a complete pipeline from input values to collected results.
// Each stage communicates by receiving values from one channel and sending
// values to the next, so callers can learn a channel-first alternative to
// shared-memory coordination. It returns accepted results in input order. If
// ctx is canceled, Run returns the results collected so far and the
// cancellation error. A nil context is treated as context.Background.
func Run(ctx context.Context, values []int) ([]int, error) {
	ctx = backgroundIfNil(ctx)
	if err := ctx.Err(); err != nil {
		return []int{}, err
	}

	source := Source(ctx, values)
	positive := Filter(ctx, source, func(value int) bool {
		return value > 0
	})
	squared := Map(ctx, positive, func(value int) int {
		return value * value
	})

	return Collect(ctx, squared)
}

func backgroundIfNil(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}

	return ctx
}
