package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgmap.New[string, *rat.Rational]("rat")

	kv.Clear()

	log.Println(kv.Set("x", rat.Rat("0.1")))
	log.Println(kv.Set("y", rat.Rat("0.1")))
	log.Println(kv.Set("z", rat.Rat("0.1")))

	for k, v := range kv.All {
		if k == "y" {
			kv.Set(k, rat.Rat("0.2"))
		}
		log.Println(k, v)
	}

	for k, v := range kv.All {
		log.Println(k, v)
	}

}
