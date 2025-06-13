package main

import (
	"fmt"
	"math/rand/v2"
	"paxos/paxos"
	"strings"
	"sync"
	"time"
)

type acceptor struct {
	a paxos.Acceptor
	m []string
}

func main() {
	n := 3

	// make 3 servers
	acceptors := make([]acceptor, n)
	for i := 0; i < n; i++ {
		i := i
		acceptors[i] = acceptor{
			a: paxos.NewAcceptor(),
			m: make([]string, 0),
		}
		acceptors[i].a.Subscribe(0, func(logId paxos.LogId, value paxos.Value) {
			acceptors[i].m = append(acceptors[i].m, fmt.Sprintf("%v", value))
		})
	}

	// define rpc communication -
	// drop 80% of requests and responses
	// in total, 0.96% of requests don't go through
	dropRate := 0.80
	rpcs := make([]paxos.RPC, n)
	for i := 0; i < n; i++ {
		i := i
		rpcs[i] = func(req paxos.Request, resCh chan<- paxos.Response) {
			go func() {
				if rand.Float64() < dropRate {
					resCh <- nil
					return
				}
				res := acceptors[i].a.Handle(req)
				if rand.Float64() < dropRate {
					resCh <- nil
					return
				}
				resCh <- res
			}()
		}
	}

	// send updates at the same time
	wg := sync.WaitGroup{}
	for j := 0; j < 20; j++ {
		wg.Add(1)
		j := j
		go func() {
			defer wg.Done()
			i := j % n
			v := fmt.Sprintf("value%d", j)
			// 1. update the acceptor
			// 2. get a new logId
			// 3. try to write the value to logId
			// 4. if failed, go back to 1
			for {
				paxos.Update(acceptors[i].a, rpcs)
				logId := acceptors[i].a.Next()
				ok := paxos.Write(acceptors[i].a, paxos.NodeId(i), logId, v, rpcs)
				if ok {
					break
				}

				time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	// update the servers
	dropRate = 0.0
	for i := 0; i < n; i++ {
		paxos.Update(acceptors[i].a, rpcs)
	}
	// check the committed values
	// it should print the same 3 lines
	for i := 0; i < n; i++ {
		fmt.Println(strings.Join(acceptors[i].m, "_"))
	}

	// new subscriber from 13
	for i := 0; i < n; i++ {
		acceptors[i].a.Subscribe(13, func(logId paxos.LogId, value paxos.Value) {
			fmt.Printf("%v", value)
		})
		fmt.Println()
	}

	return
}
