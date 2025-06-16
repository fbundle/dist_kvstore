package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
)

func main() {
	n := 3

	// make 3 servers
	acceptorList := make([]paxos.Acceptor, n)
	for i := 0; i < n; i++ {
		i := i
		opts := badger.DefaultOptions(fmt.Sprintf("data/acceptor%d", i))
		db, err := badger.Open(opts)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		store := kvstore.NewBargerStore[paxos.LogId, paxos.Promise](db)

		// store := kvstore.NewMemStore[paxos.LogId, paxos.Promise]()
		acceptorList[i] = paxos.NewAcceptor(store)
	}

	// TODO - make this tcp or http
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
			fmt.Printf("acceptor %d log_id %d value %v\n", i, logId, value)
			listenerList[i] = append(listenerList[i], fmt.Sprintf("%v", value))
		})
	}

	// send updates at the same time
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				i, j := i, j
				v := fmt.Sprintf("value%d", i+3*j)
				for {
					// 1. update the acceptor
					// 2. get a new logId
					// 3. try to write the value to logId
					// 4. if failed, go back to 1
					logId := paxos.Update(acceptorList[i], rpcList).Next()
					ok := paxos.Write(acceptorList[i], paxos.NodeId(i), logId, v, rpcList)
					if ok {
						break
					}

					// time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
				}
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
