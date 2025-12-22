package main

import (
	"fmt"

	pgmap "github.com/xtdlib/pgmap"
)

func main() {
	kv := pgmap.New[string, int]("lock_example")
	defer kv.Purge()

	// Initialize some data
	kv.Set("counter1", 0)
	kv.Set("counter2", 0)

	// Use WithLock to atomically update multiple keys
	kv.WithLock(pgmap.LockExclusive, func(tx *pgmap.Tx[string, int]) error {
		// Read current values
		c1 := tx.Get("counter1")
		c2 := tx.Get("counter2")

		// Update both atomically
		tx.Set("counter1", c1+10)
		tx.Set("counter2", c2+20)

		fmt.Printf("Inside transaction: counter1=%d, counter2=%d\n", c1+10, c2+20)
		return nil // commit
	})

	// Verify the changes
	fmt.Printf("After commit: counter1=%d, counter2=%d\n", kv.Get("counter1"), kv.Get("counter2"))
	fmt.Println("--------")

	// Example with rollback (return error)
	err := kv.TryWithLock(pgmap.LockExclusive, func(tx *pgmap.Tx[string, int]) error {
		tx.Set("counter1", 999)
		tx.Set("counter2", 999)
		return fmt.Errorf("simulated error") // rollback
	})
	fmt.Printf("Transaction error: %v\n", err)
	fmt.Printf("After rollback: counter1=%d, counter2=%d\n", kv.Get("counter1"), kv.Get("counter2"))

	// Example using iterators inside transaction
	kv.Set("item1", 100)
	kv.Set("item2", 200)
	kv.Set("item3", 300)

	kv.WithLock(pgmap.LockShare, func(tx *pgmap.Tx[string, int]) error {
		fmt.Println("All items in transaction:")
		for k, v := range tx.All {
			fmt.Printf("  %s = %d\n", k, v)
		}
		return nil
	})

}
