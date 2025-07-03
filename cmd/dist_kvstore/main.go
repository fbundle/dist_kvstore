package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/fbundle/paxos/pkg/dist_store"
)

type HostConfig struct {
	Badger string `json:"badger"`
	RPC    string `json:"rpc"`
	Store  string `json:"store"`
}
type Config []HostConfig

func main() {
	b, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	var cl Config
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
	ds, err := dist_store.NewStore(id, badgerDBPath, peerAddrList)
	if err != nil {
		panic(err)
	}
	defer ds.Close()
	go ds.ListenAndServeRPC()
	time.Sleep(time.Second)

	// http server
	hs := &http.Server{
		Addr:    cl[id].Store,
		Handler: dist_store.HttpHandle(ds),
	}
	fmt.Println("http server listening on", cl[id].Store)
	err = hs.ListenAndServe()
	if err != nil {
		panic(err)
	}
	return
}
