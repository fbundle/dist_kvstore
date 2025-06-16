package dist_kvstore

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
	"github.com/khanh101/paxos/rpc"
	"sync"
)

type DistStore interface {
}

func makeHandlerFunc[Req any, Res any](acceptor paxos.Acceptor) func(*Req) *Res {
	return func(req *Req) *Res {
		res := acceptor.Handle(req)
		if res == nil {
			return nil
		}
		return res.(*Res)
	}
}

type command struct {
	Key string `json:"key"`
	Val string `json:"val"`
}
type distStore struct {
	mu           sync.RWMutex
	id           paxos.NodeId
	peerAddrList []string
	db           *badger.DB
	store        kvstore.Store[string, string]
	acceptor     paxos.Acceptor
	server       rpc.TCPServer
	rpcList      []paxos.RPC
}

func NewDistStore(id int, badgerPath string, peerAddrList []string) (DistStore, error) {
	bindAddr := peerAddrList[id]
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, err
	}
	acceptor := paxos.NewAcceptor(kvstore.NewBargerStore[paxos.LogId, paxos.Promise](db))
	store := kvstore.NewMemStore[string, string]()
	acceptor.Listen(0, func(logId paxos.LogId, value paxos.Value) {
		cmd := value.(command)
		store.Update(func(txn kvstore.Txn[string, string]) any {
			if cmd.Val == "" {
				txn.Del(cmd.Key)
			} else {
				txn.Set(cmd.Key, cmd.Val)
			}
			return nil
		})
	})

	server, err := rpc.NewTCPServer(bindAddr)
	if err != nil {
		return nil, err
	}
	server = server.Append(
		"prepare", makeHandlerFunc[paxos.PrepareRequest, paxos.PrepareResponse](acceptor),
	).Append(
		"accept", makeHandlerFunc[paxos.AcceptRequest, paxos.AcceptResponse](acceptor),
	).Append(
		"commit", makeHandlerFunc[paxos.CommitRequest, paxos.CommitResponse](acceptor),
	).Append(
		"get", makeHandlerFunc[paxos.GetRequest, paxos.GetResponse](acceptor),
	)

	rpcList := make([]paxos.RPC, len(peerAddrList))
	for i := range peerAddrList {
		if i == id {
			rpcList[i] = func(req paxos.Request, resCh chan<- paxos.Response) {
				resCh <- acceptor.Handle(req)
			}
		} else {
			rpcList[i] = func(req paxos.Request, resCh chan<- paxos.Response) {
				transport := rpc.TCPTransport(peerAddrList[i])
				res, err := func() (paxos.Response, error) {
					switch req.(type) {
					case *paxos.PrepareRequest:
						return rpc.RPC[paxos.PrepareRequest, paxos.PrepareResponse](transport, "prepare", req.(*paxos.PrepareRequest))
					case *paxos.AcceptRequest:
						return rpc.RPC[paxos.AcceptRequest, paxos.AcceptResponse](transport, "accept", req.(*paxos.AcceptRequest))
					case *paxos.CommitRequest:
						return rpc.RPC[paxos.CommitRequest, paxos.CommitResponse](transport, "commit", req.(*paxos.CommitRequest))
					case *paxos.GetRequest:
						return rpc.RPC[paxos.GetRequest, paxos.GetResponse](transport, "get", req.(*paxos.GetRequest))
					default:
						return nil, nil
					}
				}()
				if err != nil {
					res = nil
				}
				resCh <- res
			}
		}
	}

	return &distStore{
		mu:           sync.RWMutex{},
		id:           paxos.NodeId(id),
		peerAddrList: peerAddrList,
		db:           db,
		store:        store,
		acceptor:     acceptor,
		server:       server,
		rpcList:      rpcList,
	}, nil
}

func (ds *distStore) Close() error {
	err1 := ds.db.Close()
	err2 := ds.server.Close()
	return combineErrors(err1, err2)
}

func (ds *distStore) Run() error {
	return ds.server.Run()
}

func (ds *distStore) Get(key string) (string, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	o := ds.store.Update(func(txn kvstore.Txn[string, string]) any {
		val, ok := txn.Get(key)
		return [2]any{val, ok}
	}).([2]any)
	val, ok := o[0].(string), o[1].(bool)
	return val, ok
}

func (ds *distStore) Set(key string, val string) {
	for {
		logId := paxos.Update(ds.acceptor, ds.rpcList).Next()
		ok := paxos.Write(ds.acceptor, ds.id, logId, command{Key: key, Val: val}, ds.rpcList)
		if ok {
			break
		}
	}
}

func (ds *distStore) Del(key string) {
	ds.Set(key, "")
}
