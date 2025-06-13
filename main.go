package main

import (
	"fmt"
	"math/rand"
	"paxos/paxos"
)

func main() {
	n := 3
	serverList := make([]paxos.Server, n)
	for i := 0; i < n; i++ {
		serverList[i] = paxos.NewServer(func(v paxos.Value) {
			fmt.Printf("server %d applies %v\n", i, v)
		})
	}

	rpc := make([]paxos.RPC, n)
	for i := 0; i < n; i++ {
		i := i
		rpc[i] = func(req paxos.Request) paxos.Response {
			_, ok := req.(*paxos.CommitRequest)
			if !ok && rand.Float64() < 0.99 {
				// drop all except commit requests
				return nil
			}
			server := serverList[i]
			return server.Handle(req)
		}
	}
	for j := 0; j < 20; j++ {
		j := j
		s := fmt.Sprintf("hello_%dth_times", j)
		paxos.Append(0, paxos.LogId(j), s, rpc)
	}
}
