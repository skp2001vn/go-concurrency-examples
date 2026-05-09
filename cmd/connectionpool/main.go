package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/skp2001vn/go-concurrency-examples/connectionpool"
)

func main() {
	pool, err := connectionpool.New(2, 3)
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for workerID := 1; workerID <= 5; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			conn, err := pool.Acquire(ctx)
			if err != nil {
				fmt.Printf("worker %d could not acquire connection: %v\n", workerID, err)
				return
			}
			defer func() {
				if releaseErr := pool.Release(conn); releaseErr != nil {
					fmt.Printf("worker %d could not release connection %d: %v\n", workerID, conn.ID(), releaseErr)
				}
			}()

			fmt.Printf("worker %d acquired connection %d\n", workerID, conn.ID())
			time.Sleep(150 * time.Millisecond)
		}(workerID)
	}

	wg.Wait()
}
