package dist_kvstore

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
	"github.com/khanh101/paxos/rpc"
)

const (
	BACKOFF_MIN_TIME = 10 * time.Millisecond
	BACKOFF_MAX_TIME = 1000 * time.Millisecond
	UPDATE_INTERVAL  = 100 * time.Millisecond
)

type Store interface {
	Close() error
	ListenAndServeRPC() error
	Get(key string) Entry
	Set(Cmd)
	Keys() []string
}

func makeHandlerFunc[Req any, Res any](acceptor paxos.Acceptor[Cmd]) func(*Req) *Res {
	return func(req *Req) *Res {
		res := acceptor.Handle(req)
		if res == nil {
			return nil
		}
		return res.(*Res)
	}
}

type store struct {
	id           paxos.NodeId
	peerAddrList []string
	db           *badger.DB
	memStore     *stateMachine
	acceptor     paxos.Acceptor[Cmd]
	server       rpc.TCPServer
	rpcList      []paxos.RPC
	writeMu      sync.Mutex
	updateCtx    context.Context
	updateCancel context.CancelFunc
}

func getDefaultEntry(txn kvstore.Txn[string, Entry], key string) Entry {
	entry, ok := txn.Get(key)
	if !ok {
		return Entry{
			Key: key,
			Val: "",
			Ver: 0,
		}
	}
	return entry
}

func NewDistStore(id int, badgerPath string, peerAddrList []string) (Store, error) {
	bindAddr := peerAddrList[id]
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, err
	}
	acceptor := paxos.NewAcceptor(kvstore.NewBargerStore[paxos.LogId, paxos.Promise[Cmd]](db))
	memStore := newStateMachine()
	acceptor.Subscribe(0, memStore.Apply)

	server, err := rpc.NewTCPServer(bindAddr)
	if err != nil {
		return nil, err
	}
	server = server.
		Register("prepare", makeHandlerFunc[paxos.PrepareRequest, paxos.PrepareResponse](acceptor)).
		Register("accept", makeHandlerFunc[paxos.AcceptRequest[Cmd], paxos.AcceptResponse](acceptor)).
		Register("commit", makeHandlerFunc[paxos.CommitRequest[Cmd], paxos.CommitResponse](acceptor)).
		Register("poll", makeHandlerFunc[paxos.PollRequest, paxos.PollResponse[Cmd]](acceptor))

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
					case *paxos.AcceptRequest[Cmd]:
						return rpc.RPC[paxos.AcceptRequest[Cmd], paxos.AcceptResponse](transport, "accept", req.(*paxos.AcceptRequest[Cmd]))
					case *paxos.CommitRequest[Cmd]:
						return rpc.RPC[paxos.CommitRequest[Cmd], paxos.CommitResponse](transport, "commit", req.(*paxos.CommitRequest[Cmd]))
					case *paxos.PollRequest:
						return rpc.RPC[paxos.PollRequest, paxos.PollResponse[Cmd]](transport, "poll", req.(*paxos.PollRequest))
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

func (ds *store) ListenAndServeRPC() error {
	go func() {
		ticker := time.NewTicker(UPDATE_INTERVAL)
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
	return ds.server.ListenAndServe()
}

func (ds *store) Set(cmd Cmd) {
	ds.writeMu.Lock()
	defer ds.writeMu.Unlock()
	wait := BACKOFF_MIN_TIME
	backoff := func() {
		time.Sleep(time.Duration(rand.Intn(int(wait))))
		wait *= 2
		if wait > BACKOFF_MAX_TIME {
			wait = BACKOFF_MAX_TIME
		}
	}
	for {
		logId := ds.acceptor.Next()
		ok := paxos.Write(ds.acceptor, ds.id, logId, cmd, ds.rpcList)
		if ok {
			break
		}
		backoff()
	}
}

func (ds *store) Get(key string) Entry {
	return ds.memStore.Get(key)
}

func (ds *store) Keys() []string {
	return ds.memStore.Keys()
}
