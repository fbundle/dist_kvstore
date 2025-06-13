package main

import (
	"fmt"
	"math/rand/v2"
	"paxos/paxos"
	"strings"
	"sync"
	"time"
)

type server struct {
	s paxos.Server
	m []string
}

func main() {
	n := 3

	// make 3 servers
	sList := make([]server, n)
	for i := 0; i < n; i++ {
		i := i
		sList[i] = server{
			s: paxos.NewServer(func(j paxos.LogId, v paxos.Value) {
				fmt.Println(i, j, v)
				sList[i].m = append(sList[i].m, v.(string))
			}),
			m: make([]string, 0),
		}
	}

	// define rpc communication
	dropRate := 0.80 // drop 80% of requests and responses
	rpcList := make([]paxos.RPC, n)
	for i := 0; i < n; i++ {
		i := i
		rpcList[i] = func(req paxos.Request) paxos.Response {
			if rand.Float64() < dropRate {
				return nil
			}
			ch := make(chan paxos.Response, 1)
			go func() {
				ch <- sList[i].s.Handle(req)
			}()
			res := <-ch
			if rand.Float64() < dropRate {
				return nil
			}
			return res
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
			// 1. update the server
			// 2. get a new logId
			// 3. try to write the value to logId
			// 4. if failed, go back to 1
			for {
				paxos.Update(sList[i].s, rpcList)
				logId := sList[i].s.Next()
				ok := paxos.Write(sList[i].s, paxos.NodeId(i), logId, v, rpcList)
				if ok {
					break
				}

				time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	// check the committed values
	// it should print the same 3 lines
	dropRate = 0.0
	for i := 0; i < n; i++ {
		paxos.Update(sList[i].s, rpcList)
		fmt.Println(strings.Join(sList[i].m, "_"))
	}

	return
}
