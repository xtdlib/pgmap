package main

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/xtdlib/pgmap"
)

type Human struct {
	Name string
	Age  int
}

func main() {
	kv := pgmap.New[string, *Human]("structtest", pgmap.DSN("postgres://postgres:postgres@oci-aca-001:5432/postgres"))

	// kv.Clear()

	// kv.Set("a", Human{Name: "Alice", Age: 30})
	// kv.Set("b", Human{Name: "Brian", Age: 30})

	h := kv.Get("a")
	// if h.Name != "Alice" || h.Age != 30 {
	// 	panic("Get failed")
	// }

	spew.Dump(h)
}
