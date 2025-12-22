package pgmap

import (
	"sync"
	"testing"

	"github.com/xtdlib/rat"
)

// TestAddRatConcurrent tests concurrent AddRat operations for atomicity
func TestAddRatConcurrent(t *testing.T) {
	kv := New[string, *rat.Rational]("test_addrat_concurrent")
	kv.Clear()

	// Initialize with 0
	kv.Set("counter", rat.Rat(0))

	// Run 100 concurrent additions
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kv.AddRat("counter", rat.Rat(1))
		}()
	}
	wg.Wait()

	// Should be exactly 100 due to atomic operations
	result := kv.Get("counter")
	expected := rat.Rat(100)
	if !result.Equal(expected) {
		t.Fatalf("Expected %s, got %s", expected, result)
	}

	kv.Purge()
}

// TestAddRatConcurrentFractional tests concurrent fractional additions
func TestAddRatConcurrentFractional(t *testing.T) {
	kv := New[string, *rat.Rational]("test_addrat_fractional")
	kv.Clear()

	kv.Set("sum", rat.Rat(0))

	// Add 3/10 (0.3) one hundred times concurrently
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kv.AddRat("sum", "3/10") // 0.3
		}()
	}
	wg.Wait()

	// Should be exactly 30 (100 * 3/10)
	result := kv.Get("sum")
	expected := rat.Rat(30)
	if !result.Equal(expected) {
		t.Fatalf("Expected %s, got %s", expected, result)
	}

	kv.Purge()
}

// TestAddRatConcurrentMixed tests concurrent mixed operations
func TestAddRatConcurrentMixed(t *testing.T) {
	kv := New[string, *rat.Rational]("test_addrat_mixed")
	kv.Clear()

	kv.Set("balance", rat.Rat(1000))

	var wg sync.WaitGroup

	// 50 goroutines adding 1
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kv.AddRat("balance", rat.Rat(1))
		}()
	}

	// 50 goroutines subtracting 1
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kv.AddRat("balance", rat.Rat(-1))
		}()
	}

	wg.Wait()

	// Should be exactly 1000 (net zero change)
	result := kv.Get("balance")
	expected := rat.Rat(1000)
	if !result.Equal(expected) {
		t.Fatalf("Expected %s, got %s", expected, result)
	}

	kv.Purge()
}
