package diningphilosophers

import (
	"context"
	"fmt"
	"sync"
)

// Result reports how many rounds one worker completed.
type Result struct {
	// Philosopher identifies the worker at the table.
	Philosopher int

	// Rounds is the number of completed work rounds.
	Rounds int
}

// Run lets philosophers complete rounds of work without deadlocking.
//
// Run creates one shared resource per philosopher. Each philosopher needs its
// left and right resources before completing a round. A semaphore allows at
// most philosophers-1 workers to compete for resources at the same time, which
// avoids circular wait. Run returns context cancellation errors if ctx is
// canceled before every worker finishes. A nil context is treated as
// context.Background.
func Run(ctx context.Context, philosophers int, rounds int) ([]Result, error) {
	if philosophers < 2 {
		return nil, fmt.Errorf("philosophers must be at least 2: %d", philosophers)
	}
	if rounds < 0 {
		return nil, fmt.Errorf("rounds must be non-negative: %d", rounds)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		results := make([]Result, philosophers)
		for i := range results {
			results[i].Philosopher = i
		}
		return results, err
	}

	results := make([]Result, philosophers)
	for i := range results {
		results[i].Philosopher = i
	}
	if rounds == 0 {
		return results, nil
	}

	forks := make([]sync.Mutex, philosophers)
	seats := make(chan struct{}, philosophers-1)
	errs := make(chan error, philosophers)

	var wg sync.WaitGroup
	wg.Add(philosophers)
	for id := 0; id < philosophers; id++ {
		go func(id int) {
			defer wg.Done()
			errs <- dine(ctx, id, rounds, forks, seats, &results[id])
		}(id)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return results, err
		}
	}

	return results, nil
}

func dine(ctx context.Context, id int, rounds int, forks []sync.Mutex, seats chan struct{}, result *Result) error {
	left := id
	right := (id + 1) % len(forks)

	for i := 0; i < rounds; i++ {
		if err := sit(ctx, seats); err != nil {
			return err
		}

		forks[left].Lock()
		forks[right].Lock()

		result.Rounds++

		forks[right].Unlock()
		forks[left].Unlock()
		leave(seats)
	}

	return nil
}

func sit(ctx context.Context, seats chan<- struct{}) error {
	select {
	case seats <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func leave(seats <-chan struct{}) {
	<-seats
}
