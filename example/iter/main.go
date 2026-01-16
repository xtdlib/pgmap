package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgmap.New[string, *rat.Rational]("rat")
	kv.Clear()

	kv.Set("x", rat.Rat(3))
	for k, v := range kv.All {
		log.Println(k, v)
	}

	for k, v := range kv.Map() {
		log.Println(k, v)
	}

}
