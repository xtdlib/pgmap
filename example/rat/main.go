package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	log.Println("start")
	var sellBook = pgmap.New[*rat.Rational, *rat.Rational]("tmp")
	sellBook.Set(rat.Rat(1), rat.Rat("1/3"))
}
