package main

import (
	"fmt"
	"time"
)

type temp struct {
	chn <-chan time.Time
	out chan interface{}
}

func main() {

	t := &temp{chn: make(chan time.Time),
		out: make(chan interface{}),
	}

	t.chn = time.After(5 * time.Second)
	go t.run()
	fmt.Print("maiani")
	// <-t.chn
	<-t.out
}

func (t *temp) run() {
	for {
		select {
		case <-t.chn:
			fmt.Print("deepak")
			t.out <- nil
		}
	}
}
