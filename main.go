package main

import (
	"fmt"
	"math/rand/v2"
	"paxos/paxos"
	"strings"
	"sync"
	"time"
)

func main() {
	n := 3

	// make 3 servers
	acceptorList := make([]paxos.Acceptor, n)
	for i := 0; i < n; i++ {
		i := i
		acceptorList[i] = paxos.NewAcceptor()
	}

	// define rpc communication -
	// drop 80% of requests and responses
	// in total, 0.96% of requests don't go through
	dropRate := 0.80
	rpcList := make([]paxos.RPC, n)
	for i := 0; i < n; i++ {
		i := i
		rpcList[i] = func(req paxos.Request, resCh chan<- paxos.Response) {
			go func() {
				if rand.Float64() < dropRate {
					resCh <- nil
					return
				}
				res := acceptorList[i].Handle(req)
				if rand.Float64() < dropRate {
					resCh <- nil
					return
				}
				resCh <- res
			}()
		}
	}

	listenerList := make([][]string, n)
	for i := 0; i < n; i++ {
		i := i
		acceptorList[i].Listen(0, func(logId paxos.LogId, value paxos.Value) {
			listenerList[i] = append(listenerList[i], fmt.Sprintf("%v", value))
		})
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
				paxos.Update(acceptorList[i], rpcList)
				logId := acceptorList[i].Next()
				ok := paxos.Write(acceptorList[i], paxos.NodeId(i), logId, v, rpcList)
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
		paxos.Update(acceptorList[i], rpcList)
	}
	// check the committed values
	// it should print the same 3 lines
	for i := 0; i < n; i++ {
		fmt.Println(strings.Join(listenerList[i], ""))
	}

	// new subscriber from 13
	for i := 0; i < n; i++ {
		acceptorList[i].Listen(13, func(logId paxos.LogId, value paxos.Value) {
			fmt.Printf("%v", value)
		})
		fmt.Println()
	}

	return
}
