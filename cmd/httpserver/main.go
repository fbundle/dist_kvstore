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
		fmt.Println(cmd)
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
					case *paxos.PrepareRequest:
						return &paxosRequest{
							Type: "prepare",
							Body: b,
						}, &paxos.PrepareResponse{}
					case *paxos.AcceptRequest:
						return &paxosRequest{
							Type: "accept",
							Body: b,
						}, &paxos.AcceptResponse{}
					case *paxos.CommitRequest:
						return &paxosRequest{
							Type: "commit",
							Body: b,
						}, &paxos.CommitResponse{}
					case *paxos.GetRequest:
						return &paxosRequest{
							Type: "get",
							Body: b,
						}, &paxos.GetResponse{}
					default:
						fmt.Println(r, string(b))
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
