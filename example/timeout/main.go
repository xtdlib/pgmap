package main

import (
	"fmt"
	"time"

	"github.com/xtdlib/pgmap"
)

func main() {
	kv, err := pgmap.New[int, string]("", "timeout_test")
	if err != nil {
		panic(err)
	}

	kv.Clear()

	// Insert 100,000 rows to force batched fetching
	fmt.Println("Inserting 100,000 rows...")
	for i := 0; i < 100000; i++ {
		kv.Set(i, fmt.Sprintf("value_%d", i))
	}

	fmt.Println("Starting iteration with slow processing (100ms per row, 3 sec timeout)...")
	count := 0
	startTime := time.Now()
	for k, v := range kv.All {
		if count%100 == 0 {
			elapsed := time.Since(startTime)
			fmt.Printf("Row %d: k=%d v=%s elapsed=%v\n", count, k, v, elapsed)
		}
		time.Sleep(100 * time.Millisecond) // 100ms per row = 10 seconds total for 100 rows
		count++
		if count >= 100 {
			break
		}
	}
	fmt.Printf("Processed %d rows in %v\n", count, time.Since(startTime))
}
