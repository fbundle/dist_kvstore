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

	sList := make([]server, n)
	for i := 0; i < n; i++ {
		i := i
		sList[i] = server{
			s: paxos.NewServer(func(j paxos.LogId, v paxos.Value) {
				sList[i].m = append(sList[i].m, v.(string))
			}),
			m: make([]string, 0),
		}
	}

	rpcList := make([]paxos.RPC, n)
	for i := 0; i < n; i++ {
		i := i
		rpcList[i] = func(req paxos.Request) paxos.Response {
			ch := make(chan paxos.Response, 1)
			go func() {
				ch <- sList[i].s.Handle(req)
			}()
			return <-ch
		}
	}

	wg := sync.WaitGroup{}
	for j := 0; j < 20; j++ {
		wg.Add(1)
		j := j
		go func() {
			defer wg.Done()
			i := j % n
			v := fmt.Sprintf("value%d", j)
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

	for i := 0; i < n; i++ {
		fmt.Println(strings.Join(sList[i].m, "_"))
	}

	return
}
