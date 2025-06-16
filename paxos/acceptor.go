package paxos

import (
	"github.com/khanh101/paxos/kvstore"
	"sync"
)

type Acceptor[T any] interface {
	UpdateLocalCommit() Acceptor[T]
	Get(logId LogId) (val T, ok bool)
	Next() LogId
	Handle(req Request) (res Response)
	Listen(from LogId, listener func(logId LogId, value T)) (cancel func())
}

func NewAcceptor[T any](log kvstore.Store[LogId, Promise[T]]) Acceptor[T] {
	return (&acceptor[T]{
		mu: sync.Mutex{},
		acceptor: &simpleAcceptor[T]{
			log: log,
		},
		smallestUnapplied: 0,
		listenerCount:     0,
		listenerMap:       make(map[uint64]func(logId LogId, value T)),
	}).updateLocalCommitWithoutLock()
}

// acceptor - paxos acceptor must be persistent
type acceptor[T any] struct {
	mu                sync.Mutex
	acceptor          *simpleAcceptor[T]
	smallestUnapplied LogId
	listenerCount     uint64
	listenerMap       map[uint64]func(logId LogId, value T)
}

func (a *acceptor[T]) UpdateLocalCommit() Acceptor[T] {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.updateLocalCommitWithoutLock()
}

func (a *acceptor[T]) updateLocalCommitWithoutLock() *acceptor[T] {
	for {
		promise := a.acceptor.get(a.smallestUnapplied)
		if promise.Proposal != COMMITED {
			break
		}
		for _, listener := range a.listenerMap {
			listener(a.smallestUnapplied, promise.Value)
		}
		a.smallestUnapplied++
	}
	return a
}

func (a *acceptor[T]) Listen(from LogId, listener func(logId LogId, value T)) (cancel func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.smallestUnapplied < from {
		panic("subscribe from a future log_id")
	}
	for logId := from; logId < a.smallestUnapplied; logId++ {
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

func (a *acceptor[T]) Get(logId LogId) (T, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	promise := a.acceptor.get(logId)
	return promise.Value, promise.Proposal == COMMITED
}

func (a *acceptor[T]) Next() LogId {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.smallestUnapplied
}
func (a *acceptor[T]) Handle(r Request) Response {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch req := r.(type) {
	case *PrepareRequest:
		proposal, ok := a.acceptor.prepare(req.LogId, req.Proposal)
		return &PrepareResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *AcceptRequest[T]:
		proposal, ok := a.acceptor.accept(req.LogId, req.Proposal, req.Value)
		return &AcceptResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *CommitRequest[T]:
		a.acceptor.commit(req.LogId, req.Value)
		a.updateLocalCommitWithoutLock()
		return nil
	case *GetRequest:
		promise := a.acceptor.get(req.LogId)
		return &GetResponse[T]{
			Promise: promise,
		}
	default:
		return nil
	}
}
