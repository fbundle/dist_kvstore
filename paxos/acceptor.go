package paxos

import (
	"paxos/kvstore"
	"sync"
)

type Acceptor interface {
	Get(logId LogId) (val Value, ok bool)
	Next() LogId
	Handle(req Request) (res Response)
	Listen(from LogId, listener func(logId LogId, value Value)) (cancel func())
}

func NewAcceptor(log kvstore.Store[LogId, Promise]) Acceptor {
	return &acceptor{
		mu: sync.Mutex{},
		acceptor: &simpleAcceptor{
			log: log,
		},
		smallestUncommited: 0,
		listenerCount:      0,
		listenerMap:        make(map[uint64]func(logId LogId, value Value)),
	}
}

// acceptor - paxos acceptor must be persistent
type acceptor struct {
	mu                 sync.Mutex
	acceptor           *simpleAcceptor
	smallestUncommited LogId
	listenerCount      uint64
	listenerMap        map[uint64]func(logId LogId, value Value)
}

func (a *acceptor) Listen(from LogId, listener func(logId LogId, value Value)) (cancel func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.smallestUncommited < from {
		panic("subscribe from a future log_id")
	}
	for logId := from; logId < a.smallestUncommited; logId++ {
		listener(logId, a.acceptor.get(logId).Value)
	}
	count := a.listenerCount
	a.listenerCount++
	a.listenerMap[count] = listener
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(a.listenerMap, count)
	}
}

func (a *acceptor) Get(logId LogId) (Value, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	promise := a.acceptor.get(logId)
	return promise.Value, promise.Proposal == COMMITED
}

func (a *acceptor) Next() LogId {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.smallestUncommited
}
func (a *acceptor) Handle(r Request) Response {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch req := r.(type) {
	case *PrepareRequest:
		proposal, ok := a.acceptor.prepare(req.LogId, req.Proposal)
		return &PrepareResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *AcceptRequest:
		proposal, ok := a.acceptor.accept(req.LogId, req.Proposal, req.Value)
		return &AcceptResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *CommitRequest:
		a.acceptor.commit(req.LogId, req.Value)
		for {
			promise := a.acceptor.get(a.smallestUncommited)
			if promise.Proposal != COMMITED {
				break
			}
			for _, listener := range a.listenerMap {
				listener(a.smallestUncommited, promise.Value)
			}
			a.smallestUncommited++
		}
		return nil
	case *GetRequest:
		promise := a.acceptor.get(req.LogId)
		return &GetResponse{
			Promise: promise,
		}
	default:
		return nil
	}
}
