package main

import (
	"encoding/json"
	"fmt"
	"github.com/khanh101/paxos/dist_kvstore"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

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
		store := kvstore.NewMemStore[paxos.LogId, paxos.Promise[string]]()
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
		acceptorList[i].Listen(0, func(logId paxos.LogId, value string) {
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
					logId := paxos.Update(acceptorList[i], rpcList).Next()
					ok := paxos.Write(acceptorList[i], paxos.NodeId(i), logId, v, rpcList)
					if ok {
						break
					}

					// time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
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

	// new subscriber from 13
	for i := 0; i < n; i++ {
		acceptorList[i].Listen(13, func(logId paxos.LogId, value string) {
			fmt.Printf("%v", value)
		})
		fmt.Println()
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

	d.Append("add", func(req *AddReq) (res *AddRes) {
		sum := 0
		for _, v := range req.Values {
			sum += v
		}
		return &AddRes{
			Sum: sum,
		}
	}).Append("sub", func(req *SubReq) (res *SubRes) {
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

	s.Append("add", func(req *AddReq) (res *AddRes) {
		sum := 0
		for _, v := range req.Values {
			sum += v
		}
		return &AddRes{
			Sum: sum,
		}
	}).Append("sub", func(req *SubReq) (res *SubRes) {
		return &SubRes{
			Diff: req.A - req.B,
		}
	})

	go s.Run()

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

func testDistKVStore() {
	badgerDBPathList := []string{
		"data/acceptor0",
		"data/acceptor1",
		"data/acceptor2",
	}
	peerAddrList := []string{
		"localhost:14000",
		"localhost:14001",
		"localhost:14002",
	}
	sList := make([]dist_kvstore.DistStore, 3)
	for i := 0; i < 3; i++ {
		s, err := dist_kvstore.NewDistStore(i, badgerDBPathList[i], peerAddrList)
		if err != nil {
			panic(err)
		}
		defer s.Close()
		go s.Run()
		sList[i] = s
	}
	time.Sleep(time.Second)
	sList[0].Set("key1", "value1")
}

type Config struct {
	Badger string `json:"badger"`
	RPC    string `json:"rpc"`
	Store  string `json:"store"`
}
type ConfigList []Config

func main() {
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var cl ConfigList
	err = json.Unmarshal(b, &cl)
	if err != nil {
		panic(err)
	}

	id, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}

	badgerDBPath := cl[id].Badger
	peerAddrList := make([]string, len(cl))
	for i, c := range cl {
		peerAddrList[i] = c.RPC
	}
	ds, err := dist_kvstore.NewDistStore(id, badgerDBPath, peerAddrList)
	if err != nil {
		panic(err)
	}
	defer ds.Close()
	go ds.Run()
	time.Sleep(time.Second)

	// http server
	hs := &http.Server{
		Addr:    cl[id].Store,
		Handler: dist_kvstore.HttpHandle(ds),
	}
	fmt.Println("http server listening on", cl[id].Store)
	err = hs.ListenAndServe()
	if err != nil {
		panic(err)
	}
	return
}
