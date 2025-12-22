package main

import (
	"log"
	"time"

	"github.com/xtdlib/pgmap"
)

func main() {
	kv := pgmap.New[string, string]("ttl_example")

	kv.Clear()

	// Set a value with TTL of 2 seconds
	kv.SetWithTTL("temp", "expires soon", 2*time.Second)
	log.Println("Set 'temp' with 2s TTL")

	// Set a value without TTL (persists indefinitely)
	kv.Set("permanent", "never expires")
	log.Println("Set 'permanent' without TTL")

	// Read both values immediately
	log.Println("temp:", kv.Get("temp"))
	log.Println("permanent:", kv.Get("permanent"))

	// Wait for TTL to expire
	log.Println("Waiting 3 seconds for TTL to expire...")
	time.Sleep(3 * time.Second)

	// temp should no longer exist
	log.Println("Has 'temp':", kv.Has("temp"))         // false
	log.Println("Has 'permanent':", kv.Has("permanent")) // true

	// Keys iterator skips expired entries
	log.Println("All keys:")
	for k := range kv.Keys {
		log.Println(" -", k)
	}
}
