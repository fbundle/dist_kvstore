package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
)

func testLocal() {
	n := 3

	// make 3 servers
	acceptorList := make([]paxos.Acceptor, n)
	for i := 0; i < n; i++ {
		i := i
		opts := badger.DefaultOptions(fmt.Sprintf("data/acceptor%d", i))
		db, err := badger.Open(opts)
		if err != nil {
			panic(err)
		}
		defer db.Close()
		store := kvstore.NewBargerStore[paxos.LogId, paxos.Promise](db)

		// store := kvstore.NewMemStore[paxos.LogId, paxos.Promise]()
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
		acceptorList[i].Listen(0, func(logId paxos.LogId, value paxos.Value) {
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
		acceptorList[i].Listen(13, func(logId paxos.LogId, value paxos.Value) {
			fmt.Printf("%v", value)
		})
		fmt.Println()
	}

	return
}

type Op string

const (
	OpGet Op = "get"
	OpSet Op = "set"
	OpDel Op = "del"
)

type command struct {
	Op  Op     `json:"op"`
	Key string `json:"key"`
	Val string `json:"val"`
}

type dstore struct {
	id       paxos.NodeId
	rpcList  []paxos.RPC
	db       *badger.DB
	acceptor paxos.Acceptor
	store    kvstore.Store[string, string]
	httpMu   sync.Mutex
}

type paxosRequest struct {
	Type string `json:"type"`
	Body []byte `json:"body"`
}

func unmarshal[T any](b []byte) (T, error) {
	var v T
	err := json.Unmarshal(b, &v)
	return v, err
}

func newDStore(id int, badgerPath string, addrList []string) *dstore {
	opts := badger.DefaultOptions(badgerPath)
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	acceptor := paxos.NewAcceptor(
		kvstore.NewBargerStore[paxos.LogId, paxos.Promise](
			db,
		),
	)
	store := kvstore.NewMemStore[string, string]()

	acceptor.Listen(0, func(logId paxos.LogId, value paxos.Value) {
		cmd := value.(command)
		if cmd.Op == OpSet {
			store.Update(func(txn kvstore.Txn[string, string]) any {
				txn.Set(cmd.Key, cmd.Val)
				return nil
			})
		} else if cmd.Op == OpDel {
			store.Update(func(txn kvstore.Txn[string, string]) any {
				txn.Del(cmd.Key)
				return nil
			})
		}
	})

	rpcList := make([]paxos.RPC, len(addrList))
	for i, addr := range addrList {
		if i == id {
			rpcList[i] = func(r paxos.Request, c chan<- paxos.Response) {
				c <- acceptor.Handle(r)
			}
		} else {
			rpcList[i] = func(r paxos.Request, c chan<- paxos.Response) {
				b, err := json.Marshal(r)
				if err != nil {
					c <- nil
					return
				}
				req1, res1 := func() (*paxosRequest, paxos.Response) {
					switch r.(type) {
					case paxos.PrepareRequest:
						return &paxosRequest{
							Type: "prepare",
							Body: b,
						}, &paxos.PrepareResponse{}
					case paxos.AcceptRequest:
						return &paxosRequest{
							Type: "accept",
							Body: b,
						}, &paxos.AcceptResponse{}
					case paxos.CommitRequest:
						return &paxosRequest{
							Type: "commit",
							Body: b,
						}, &paxos.CommitResponse{}
					case paxos.GetRequest:
						return &paxosRequest{
							Type: "get",
							Body: b,
						}, &paxos.GetResponse{}
					default:
						panic("wrong type")
					}
				}()
				b, err = json.Marshal(req1)
				if err != nil {
					c <- nil
					return
				}
				res, err := http.Post(
					fmt.Sprintf("%s/paxos", addr),
					"application/json",
					bytes.NewBuffer(b),
				)
				if err != nil {
					c <- nil
					return
				}
				if res.StatusCode != http.StatusOK {
					c <- nil
					return
				}
				body, err := io.ReadAll(res.Body)
				if err != nil {
					c <- nil
					return
				}
				err = json.Unmarshal(body, res1)
				if err != nil {
					c <- nil
					return
				}
				c <- res1
				return
			}
		}

	}

	return &dstore{
		id:       paxos.NodeId(id),
		rpcList:  rpcList,
		db:       db,
		acceptor: acceptor,
		store:    store,
		httpMu:   sync.Mutex{},
	}
}

func (ds *dstore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ds.httpMu.Lock()
	defer ds.httpMu.Unlock()

	parts := strings.Split(r.URL.Path, "/")
	if len(parts) == 2 && parts[1] == "paxos" {
		// expect path to be `/paxos`
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		r := paxosRequest{}
		err = json.Unmarshal(body, &r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		req, err := func() (paxos.Request, error) {
			switch r.Type {
			case "prepare":
				return unmarshal[paxos.PrepareRequest](r.Body)
			case "accept":
				return unmarshal[paxos.AcceptRequest](r.Body)
			case "commit":
				return unmarshal[paxos.CommitRequest](r.Body)
			case "get":
				return unmarshal[paxos.GetRequest](r.Body)
			default:
				return nil, fmt.Errorf("parse error")
			}
		}()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		res := ds.acceptor.Handle(req)

		b, err := json.Marshal(res)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Write(b)
		return
	}

	cmd := command{
		Op:  "",
		Key: "",
		Val: "",
	}
	// set key
	{
		// expect path to be `/kvstore/<key>`
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) != 3 || parts[1] != "kvstore" {
			http.Error(w, "invalid path, expected `/kvstore/<key>`", http.StatusBadRequest)
			return
		}
		cmd.Key = parts[2]
	}

	// set op
	{
		switch r.Method {
		case http.MethodGet:
			cmd.Op = OpGet
		case http.MethodPost:
			cmd.Op = OpSet
		case http.MethodPut:
			cmd.Op = OpSet
		case http.MethodDelete:
			cmd.Op = OpDel
		default:
			http.Error(w, "method must be GET POST PUT DELETE", http.StatusBadRequest)
			return
		}
	}

	// set val
	{
		if cmd.Op == OpSet {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			cmd.Val = string(body)
		}
	}
	// send command to process
	// get
	if cmd.Op == OpGet {
		cmd.Val = ds.store.Update(func(txn kvstore.Txn[string, string]) any {
			val, ok := txn.Get(cmd.Key)
			if !ok {
				return ""
			}
			return val
		}).(string)

		w.Write([]byte(cmd.Val))
		return
	}
	// set, del
	go func(ds *dstore, cmd command) {
		for {
			logId := paxos.Update(ds.acceptor, ds.rpcList).Next()
			if ok := paxos.Write(ds.acceptor, ds.id, logId, cmd, ds.rpcList); ok {
				break
			}
		}
	}(ds, cmd)

	w.WriteHeader(http.StatusOK)
}

func main() {
	addrList := []string{
		"localhost:14000",
		"localhost:14001",
		"localhost:14002",
	}
	i, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err)
	}
	addr := addrList[i]
	node := newDStore(
		i,
		fmt.Sprintf("data/%s", addr),
		addrList,
	)
	fmt.Printf("listening on %s\n", addr)
	err = http.ListenAndServe(addr, node)
	if err != nil {
		panic(err)
	}
}
