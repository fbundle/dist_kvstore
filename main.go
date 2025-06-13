package main

import (
	"fmt"
	"math/rand"
	"paxos/paxos"
	"time"
)

func main() {
	n := 3
	serverList := make([]paxos.Server, n)
	for i := 0; i < n; i++ {
		i := i
		server := paxos.NewServer(func(j paxos.LogId, v paxos.Value) {
			fmt.Printf("server %d applies (%d, %v)\n", i, j, v)
		})
		serverList[i] = server
	}

	rpc := make([]paxos.RPC, n)
	for i := 0; i < n; i++ {
		i := i
		rpc[i] = func(req paxos.Request) paxos.Response {
			if rand.Float64() < 0.99 {
				return nil
			}
			server := serverList[i]
			return server.Handle(req)
		}
	}

	for j := 0; j < 20; j++ {
		j := j
		s := fmt.Sprintf("hello_%dth_times", j)
		nid := j % 3
		paxos.Write(paxos.NodeId(nid), serverList[nid], s, rpc)
	}
	time.Sleep(5 * time.Second)
	return
}
