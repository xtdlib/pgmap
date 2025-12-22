package pgmap

import (
	"log"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	kv := New[time.Time, string]("time1")
	kv.Clear()

	now := time.Now()
	kv.Set(now, "2")

	if kv.Get(now) != "2" {
		t.Fatal()
	}

	for k := range kv.Keys {
		_ = k
		if !k.Equal(now) {
			log.Println(k)
			log.Println(now)
			t.Fatal("time keys not equal")
		}
	}

	kv.Purge()
}

func TestTimePointer(t *testing.T) {
	kv := New[*time.Time, string]("timep")
	kv.Purge()
	kv = New[*time.Time, string]("timep")

	now := time.Now()
	kv.Set(&now, "3")
	if kv.Get(&now) != "3" {
		t.Fatal()
	}

	for k := range kv.Keys {
		_ = k
		if !k.Equal(now) {
			t.Fatal()
		}
	}

	// kv.Purge()
}

func TestFloat64(t *testing.T) {
	kv := New[float64, string]("float64_test")
	kv.Clear()

	kv.Set(3.14159, "pi")
	kv.Set(2.71828, "e")
	kv.Set(-1.23, "negative")

	if kv.Get(3.14159) != "pi" {
		t.Fatal("failed to get pi")
	}
	if kv.Get(2.71828) != "e" {
		t.Fatal("failed to get e")
	}
	if kv.Get(-1.23) != "negative" {
		t.Fatal("failed to get negative")
	}

	keys := []float64{}
	for k := range kv.Keys {
		keys = append(keys, k)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}

	kv.Purge()
}

func TestFloat32(t *testing.T) {
	kv := New[float32, string]("float32_test")
	kv.Clear()

	kv.Set(3.14, "pi")
	kv.Set(2.71, "e")

	if kv.Get(3.14) != "pi" {
		t.Fatal("failed to get pi")
	}
	if kv.Get(2.71) != "e" {
		t.Fatal("failed to get e")
	}

	keys := []float32{}
	for k := range kv.Keys {
		keys = append(keys, k)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	kv.Purge()
}
