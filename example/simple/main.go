package main

import (
	"log"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv := pgmap.New[*rat.Rational, *rat.Rational]("sellbook2", pgmap.DSN("postgres://postgres:postgres@oci-aca-001:5432/postgres"))
	kv.Set(rat.Rat(1), rat.Rat(0))
	log.Println(kv.Get(rat.Rat(1)))
	kv.Purge()
}
