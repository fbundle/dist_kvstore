package main

import (
	"fmt"

	"github.com/khanh101/paxos/pkg/rpc"
)

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
