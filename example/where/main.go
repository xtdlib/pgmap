package main

import (
	"log"

	"github.com/xtdlib/pgmap"
)

func main() {
	kv := pgmap.New[int, string]("where_example")
	kv.Clear()

	// Insert some data
	kv.Set(1, "apple")
	kv.Set(5, "banana")
	kv.Set(10, "cherry")
	kv.Set(15, "date")
	kv.Set(20, "elderberry")

	for k, v := range kv.AllWhere("key > 10") {
		log.Printf("  %d: %s\n", k, v)
	}

	log.Println("-")

	for k, v := range kv.AllWhere("key BETWEEN 5 AND 15") {
		log.Printf("  %d: %s\n", k, v)
	}

	log.Println("-")
	for k := range kv.KeysWhere("key < 10") {
		log.Printf("  %d\n", k)
	}
}
