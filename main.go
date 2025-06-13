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
	paxos.Append(0, "hello", []paxos.RPC{
		func(req paxos.Request) paxos.Response {
			if rand.Float64() < 0.8 {
				return nil
			}
			server := serverList[0]
			return server.Handle(req)
		},
		func(req paxos.Request) paxos.Response {
			if rand.Float64() < 0.8 {
				return nil
			}
			server := serverList[1]
			return server.Handle(req)
		},
		func(req paxos.Request) paxos.Response {
			if rand.Float64() < 0.8 {
				return nil
			}
			server := serverList[2]
			return server.Handle(req)
		},
	})
}
