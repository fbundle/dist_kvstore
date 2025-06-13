package main

import (
	"fmt"
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
			server := serverList[0]
			return server.Handle(req)
		},
		func(req paxos.Request) paxos.Response {
			server := serverList[1]
			return server.Handle(req)
		},
		func(req paxos.Request) paxos.Response {
			server := serverList[2]
			return server.Handle(req)
		},
	})
}
