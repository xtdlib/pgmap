package main

import (
	"context"
	"log"
	"sync"

	"github.com/xtdlib/pgmap"
	"github.com/xtdlib/rat"
)

func main() {
	kv, err := pgmap.New[string, *rat.Rational]("", "sample")
	if err != nil {
		panic(err)
	}

	kv.Set("wer", rat.Rat(0))
	log.Println(kv.Get("wer"))

	var wg sync.WaitGroup
	for i := 0; i < 1001; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// _, err := kv.Add(ctx, "wer", 1)
			// if err != nil {
			// 	panic(err)
			// }
			// kv.Update(ctx, "wer", func(old *rat.Rational) *rat.Rational {
			// 	return old.Add(0.3)
			// })

		}()
	}
	wg.Wait()

	// it should be 1000
	ctx := context.Background()
	log.Println(kv.Get(ctx, "wer"))
}
