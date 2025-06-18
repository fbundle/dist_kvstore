package main

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"

	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
	"github.com/khanh101/paxos/rpc"
)

func testLocal() {
	n := 3

	// make 3 servers
	acceptorList := make([]paxos.Acceptor[string], n)
	for i := 0; i < n; i++ {
		i := i
		acceptorList[i] = paxos.NewAcceptor[string](kvstore.NewMemStore[paxos.LogId, paxos.Promise[string]]())
	}

	// TODO - make this tcp or http
	// define rpc communication -
	// drop 80% of requests and responses
	// in total, 0.96% of requests don't go through
	// disable waiting in paxos.Write to test this
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
				res := acceptorList[i].HandleRPC(req)
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
		acceptorList[i].Subscribe(0, func(logId paxos.LogId, value string) {
			fmt.Printf("acceptor %d log_id %d value %v\n", i, logId, value)
			listenerList[i] = append(listenerList[i], fmt.Sprintf("%v", value))
		})
	}

	// send updates at the same time
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				v := fmt.Sprintf("value%d", i+3*j)
				for {
					// 1. update the acceptor
					// 2. get a new logId
					// 3. try to write the value to logId
					// 4. if failed, go back to 1
					logId := acceptorList[i].Next()
					val, ok := paxos.Write(acceptorList[i], paxos.ProposerId(i), logId, v, rpcList)
					if val == v && ok {
						break
					}
					paxos.Update(acceptorList[i], rpcList)
				}
			}
		}(i)
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

	return
}

func testRPC() {
	type AddReq struct {
		Values []int
	}

	type AddRes struct {
		Sum int
	}

	type SubReq struct {
		A int
		B int
	}

	type SubRes struct {
		Diff int
	}

	d := rpc.NewDispatcher()

	d.Register("add", func(req *AddReq) (res *AddRes) {
		sum := 0
		for _, v := range req.Values {
			sum += v
		}
		return &AddRes{
			Sum: sum,
		}
	}).Register("sub", func(req *SubReq) (res *SubRes) {
		return &SubRes{
			Diff: req.A - req.B,
		}
	})

	localTransport := d.Handle
	{
		res, err := rpc.RPC[AddReq, AddRes](
			localTransport,
			"add",
			&AddReq{Values: []int{1, 2, 3}},
		)
		if err != nil {
			panic(err)
		}
		fmt.Println(res)
	}
	{
		res, err := rpc.RPC[SubReq, SubRes](
			localTransport,
			"sub",
			&SubReq{A: 20, B: 16},
		)
		if err != nil {
			panic(err)
		}
		fmt.Println(res)
	}
}

func testRPCTCP() {
	type AddReq struct {
		Values []int
	}

	type AddRes struct {
		Sum int
	}

	type SubReq struct {
		A int
		B int
	}

	type SubRes struct {
		Diff int
	}

	addr := "localhost:14001"
	s, err := rpc.NewTCPServer(addr)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	s.Register("add", func(req *AddReq) (res *AddRes) {
		sum := 0
		for _, v := range req.Values {
			sum += v
		}
		return &AddRes{
			Sum: sum,
		}
	}).Register("sub", func(req *SubReq) (res *SubRes) {
		return &SubRes{
			Diff: req.A - req.B,
		}
	})

	go s.ListenAndServe()

	transport := rpc.TCPTransport(addr)
	{
		res, err := rpc.RPC[AddReq, AddRes](
			transport,
			"add",
			&AddReq{Values: []int{1, 2, 3}},
		)
		if err != nil {
			panic(err)
		}
		fmt.Println(res)
	}
	{
		res, err := rpc.RPC[SubReq, SubRes](
			transport,
			"sub",
			&SubReq{A: 20, B: 16},
		)
		if err != nil {
			panic(err)
		}
		fmt.Println(res)
	}
}

func main() {
	testLocal()
}
