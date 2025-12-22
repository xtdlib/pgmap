package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgmap.New[string, *rat.Rational]("", "xxx2")

	key := rat.Rat(0)
	_ = key

	kv.Clear()

	kv.AddRat("k1", "1/3")
	kv.AddRat("k2", "2/4")
	kv.AddRat("k3", "3/5")

	for k, v := range kv.AllWhere("value::rational > 2") {
		log.Println(k, v)
	}

}
