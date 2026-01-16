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
	kv.SetWithTTL("temp", "expires soon", time.Second)
	log.Println("Set 'temp' with 2s TTL")

	if kv.Get("temp") != "expires soon" {
		panic("Expected value 'expires soon'")
	}
	time.Sleep(time.Second)
	if kv.Has("temp") == true {
		panic("Expected 'temp' to still exist after 1 second")
	}

	kv.SetWithTTL("temp", "expires soon", time.Second)
	kv.Set("temp", "expires never")
	time.Sleep(time.Second * 2)
	if kv.Has("temp") != true {
		panic("Expected 'temp' to exist after updating without TTL")
	}
	log.Println(kv.Get("temp"))
}
