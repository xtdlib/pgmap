package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgmap.New[string, *rat.Rational]("rat")

	kv.Clear()

	log.Println(kv.Set("x", rat.Rat("0.4")))
}
