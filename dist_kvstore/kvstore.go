package dist_kvstore

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
	"github.com/khanh101/paxos/rpc"
	"sync"
	"time"
)

type Store interface {
	Close() error
	RunLoop() error
	Get(key string) (string, bool)
	Set(key string, val string)
	Keys() []string
}

func makeHandlerFunc[Req any, Res any](acceptor paxos.Acceptor[command]) func(*Req) *Res {
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
type store struct {
	id           paxos.NodeId
	peerAddrList []string
	db           *badger.DB
	memStore     kvstore.MemStore[string, string]
	acceptor     paxos.Acceptor[command]
	server       rpc.TCPServer
	rpcList      []paxos.RPC
	writeMu      sync.Mutex
	updateCtx    context.Context
	updateCancel context.CancelFunc
}

func NewDistStore(id int, badgerPath string, peerAddrList []string) (Store, error) {
	bindAddr := peerAddrList[id]
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, err
	}
	acceptor := paxos.NewAcceptor[command](kvstore.NewBargerStore[paxos.LogId, paxos.Promise[command]](db))
	memStore := kvstore.NewMemStore[string, string]()
	acceptor.Listen(0, func(logId paxos.LogId, cmd command) {
		memStore.Update(func(txn kvstore.Txn[string, string]) any {
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
		"accept", makeHandlerFunc[paxos.AcceptRequest[command], paxos.AcceptResponse](acceptor),
	).Append(
		"commit", makeHandlerFunc[paxos.CommitRequest[command], paxos.CommitResponse](acceptor),
	).Append(
		"get", makeHandlerFunc[paxos.GetRequest, paxos.GetResponse[command]](acceptor),
	)

	rpcList := make([]paxos.RPC, len(peerAddrList))
	for i := range peerAddrList {
		i := i
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
					case *paxos.AcceptRequest[command]:
						return rpc.RPC[paxos.AcceptRequest[command], paxos.AcceptResponse](transport, "accept", req.(*paxos.AcceptRequest[command]))
					case *paxos.CommitRequest[command]:
						return rpc.RPC[paxos.CommitRequest[command], paxos.CommitResponse](transport, "commit", req.(*paxos.CommitRequest[command]))
					case *paxos.GetRequest:
						return rpc.RPC[paxos.GetRequest, paxos.GetResponse[command]](transport, "get", req.(*paxos.GetRequest))
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

	updateCtx, updateCancel := context.WithCancel(context.Background())
	return &store{
		id:           paxos.NodeId(id),
		peerAddrList: peerAddrList,
		db:           db,
		memStore:     memStore,
		acceptor:     acceptor,
		server:       server,
		rpcList:      rpcList,
		writeMu:      sync.Mutex{},
		updateCtx:    updateCtx,
		updateCancel: updateCancel,
	}, nil
}

func (ds *store) Close() error {
	ds.updateCancel()
	err1 := ds.db.Close()
	err2 := ds.server.Close()
	return combineErrors(err1, err2)
}

func (ds *store) RunLoop() error {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ds.updateCtx.Done():
				return
			case <-ticker.C:
				paxos.Update(ds.acceptor, ds.rpcList)
			}
		}

	}()
	return ds.server.RunLoop()
}

func (ds *store) Set(key string, val string) {
	ds.writeMu.Lock()
	defer ds.writeMu.Unlock()
	wait := time.Millisecond
	// exponential backoff
	backoff := func() {
		time.Sleep(wait)
		wait *= 2
	}
	for {
		logId := paxos.Update(ds.acceptor, ds.rpcList).Next()
		ok := paxos.Write(ds.acceptor, ds.id, logId, command{Key: key, Val: val}, ds.rpcList)
		if ok {
			break
		}
		backoff()
	}
}

func (ds *store) Get(key string) (string, bool) {
	o := ds.memStore.Update(func(txn kvstore.Txn[string, string]) any {
		val, ok := txn.Get(key)
		return [2]any{val, ok}
	}).([2]any)
	val, ok := o[0].(string), o[1].(bool)
	return val, ok
}

func (ds *store) Keys() []string {
	return ds.memStore.Keys()
}
