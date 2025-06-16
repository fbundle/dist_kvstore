package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/khanh101/paxos/dist_kvstore"
)

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
