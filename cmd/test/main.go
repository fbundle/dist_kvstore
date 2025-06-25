package main

import (
	"encoding/hex"
	"fmt"

	"github.com/khanh101/paxos/pkg/crypt"
	"github.com/khanh101/paxos/pkg/rpc"
)

func testRPC() {
	type AddReq struct {
		Values []int
	}

	type SubReq struct {
		A int
		B int
	}

	type SubRes struct {
		Diff int
	}

	d := rpc.NewDispatcher()

	d.Register("add", func(req *AddReq) (res *int) {
		sum := 0
		for _, v := range req.Values {
			sum += v
		}
		return &sum
	}).Register("sub", func(req *SubReq) (res *SubRes) {
		return &SubRes{
			Diff: req.A - req.B,
		}
	})

	localTransport := d.Handle
	{
		res, err := rpc.MakeRPC[AddReq, int](
			localTransport,
			"add",
			&AddReq{Values: []int{1, 2, 3}},
		)
		if err != nil {
			panic(err)
		}
		fmt.Println(*res)
	}
	{
		res, err := rpc.MakeRPC[SubReq, SubRes](
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
		res, err := rpc.MakeRPC[AddReq, AddRes](
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
		res, err := rpc.MakeRPC[SubReq, SubRes](
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

func testAES() {
	key := crypt.NewCrypt("example key 1234") // 16 bytes for AES-128, 24 for AES-192, 32 for AES-256
	plaintext := []byte("Hello, AES encryption in Go!")

	// Encrypt
	ciphertext, err := key.Encrypt(plaintext)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Encrypted (hex): %s\n", hex.EncodeToString(ciphertext))

	// Decrypt
	decrypted, err := key.Decrypt(ciphertext)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Decrypted: %s\n", decrypted)
}

func main() {
	testRPC()
}
