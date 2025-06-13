package paxos

import (
	"sync"
)

type Acceptor interface {
	Get(LogId) (Value, bool)
	Next() LogId
	Handle(Request) Response
	Subscribe(from LogId, apply func(logId LogId, value Value)) (cancel func())
}

func NewAcceptor() Acceptor {
	return &acceptor{
		mu:                 sync.Mutex{},
		acceptor:           &simpleAcceptor{},
		smallestUncommited: 0,
		subscriberCount:    0,
		subscriberMap:      make(map[uint64]func(logId LogId, value Value)),
	}
}

// acceptor - paxos acceptor must be persistent
type acceptor struct {
	mu                 sync.Mutex
	acceptor           *simpleAcceptor
	smallestUncommited LogId
	subscriberCount    uint64
	subscriberMap      map[uint64]func(logId LogId, value Value)
}

func (a *acceptor) Subscribe(from LogId, subscriber func(logId LogId, value Value)) (cancel func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.smallestUncommited < from {
		panic("subscribe from a future log_id")
	}
	for logId := from; logId < a.smallestUncommited; logId++ {
		subscriber(logId, a.acceptor.get(logId).Value)
	}
	count := a.subscriberCount
	a.subscriberCount++
	a.subscriberMap[count] = subscriber
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(a.subscriberMap, count)
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
func (a *acceptor) Handle(req Request) Response {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch req := req.(type) {
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
			for _, subscriber := range a.subscriberMap {
				subscriber(a.smallestUncommited, promise.Value)
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
