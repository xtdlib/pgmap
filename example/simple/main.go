package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgmap.New[string, *rat.Rational]("samplxxxxxx")
	kv.Set("wer", rat.Rat(0))
	log.Println(kv.Get("wer"))
	kv.Purge()
}
