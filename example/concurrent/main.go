package main

import (
	"log"
	"sync"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	m := pgmap.New[string, *rat.Rational]("test")

	m.Clear()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.AddRat("counter", rat.Rat(1))
		}()
	}
	wg.Wait()

	log.Println(m.Get("counter"))
}
