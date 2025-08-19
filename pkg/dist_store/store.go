package dist_store

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"dist_kvstore/pkg/local_store"
	"dist_kvstore/pkg/paxos"
	"dist_kvstore/pkg/rpc"

	"github.com/dgraph-io/badger/v4"
)

const (
	BACKOFF_MIN_TIME = 10 * time.Millisecond
	BACKOFF_MAX_TIME = 1000 * time.Millisecond
	UPDATE_INTERVAL  = 100 * time.Millisecond
)

type DistStore interface {
	Close() error
	ListenAndServeRPC() error
	Get(key string) Entry
	Set(Cmd)
	Keys() []string
}

func makeHandlerFunc[Req any, Res any](acceptor paxos.Acceptor[Cmd]) func(*Req) *Res {
	return func(req *Req) *Res {
		res := acceptor.HandleRPC(req)
		if res == nil {
			return nil
		}
		return res.(*Res)
	}
}

type store struct {
	id           paxos.ProposerId
	peerAddrList []string
	db           *badger.DB
	memStore     *stateMachine
	acceptor     paxos.Acceptor[Cmd]
	dispatcher   rpc.Dispatcher
	server       rpc.TCPServer
	rpcList      []paxos.RPC
	writeMu      sync.Mutex
	updateCtx    context.Context
	updateCancel context.CancelFunc
}

func getDefaultEntry(txn local_store.Txn[string, Entry], key string) Entry {
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

func NewStore(id int, badgerPath string, peerAddrList []string) (DistStore, error) {
	bindAddr := peerAddrList[id]
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, err
	}
	ss := local_store.NewBadgerStringStore(db).Append("log")
	acceptor := paxos.NewAcceptor(local_store.MakeStoreFromStringStore[paxos.LogId, paxos.Promise[Cmd]](ss))
	memStore := newStateMachine()
	acceptor.Subscribe(0, memStore.Apply)

	dispatcher := rpc.NewDispatcher().
		Register("prepare", makeHandlerFunc[paxos.PrepareRequest, paxos.PrepareResponse[Cmd]](acceptor)).
		Register("accept", makeHandlerFunc[paxos.AcceptRequest[Cmd], paxos.AcceptResponse[Cmd]](acceptor)).
		Register("commit", makeHandlerFunc[paxos.CommitRequest[Cmd], paxos.CommitResponse](acceptor)).
		Register("poll", makeHandlerFunc[paxos.PollRequest, paxos.PollResponse[Cmd]](acceptor))

	server, err := rpc.NewTCPServer(bindAddr)
	if err != nil {
		return nil, err
	}

	rpcList := make([]paxos.RPC, len(peerAddrList))
	for i := range peerAddrList {
		i := i
		if i == id {
			rpcList[i] = func(req paxos.Request, resCh chan<- paxos.Response) {
				resCh <- acceptor.HandleRPC(req)
			}
		} else {
			rpcList[i] = func(req paxos.Request, resCh chan<- paxos.Response) {
				transport := rpc.TCPTransport(peerAddrList[i])
				res, err := func() (paxos.Response, error) {
					switch req := req.(type) {
					case *paxos.PrepareRequest:
						return rpc.RPC[paxos.PrepareRequest, paxos.PrepareResponse[Cmd]](transport, "prepare", req)
					case *paxos.AcceptRequest[Cmd]:
						return rpc.RPC[paxos.AcceptRequest[Cmd], paxos.AcceptResponse[Cmd]](transport, "accept", req)
					case *paxos.CommitRequest[Cmd]:
						return rpc.RPC[paxos.CommitRequest[Cmd], paxos.CommitResponse](transport, "commit", req)
					case *paxos.PollRequest:
						return rpc.RPC[paxos.PollRequest, paxos.PollResponse[Cmd]](transport, "poll", req)
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
		id:           paxos.ProposerId(id),
		peerAddrList: peerAddrList,
		db:           db,
		memStore:     memStore,
		acceptor:     acceptor,
		dispatcher:   dispatcher,
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
	return ds.server.ListenAndServe(ds.dispatcher)
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
		value, ok := paxos.Write(ds.acceptor, ds.id, logId, cmd, ds.rpcList)
		if ok && value.Equal(cmd) {
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
