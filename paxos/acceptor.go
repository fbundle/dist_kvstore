package paxos

import (
	"sync"

	"github.com/khanh101/paxos/kvstore"
)

type StateMachine[T any] func(logId LogId, value T)

type Acceptor[T any] interface {
	Get(logId LogId) (val T, ok bool)
	Next() LogId
	Handle(req Request) (res Response)
	Subscribe(sm StateMachine[T]) (cancel func())
}

func NewAcceptor[T any](smallestUnapplied LogId, log kvstore.Store[LogId, Promise[T]]) Acceptor[T] {
	return (&acceptor[T]{
		mu:                sync.Mutex{},
		acceptor:          &simpleAcceptor[T]{log: log},
		snapshot:          smallestUnapplied - 1,
		smallestUnapplied: smallestUnapplied,
		subsciber:         nil,
	}).updateLocalCommitWithoutLock()
}

// acceptor - paxos acceptor must be persistent
type acceptor[T any] struct {
	mu                sync.Mutex
	acceptor          *simpleAcceptor[T]
	snapshot          LogId
	smallestUnapplied LogId
	subsciber         StateMachine[T]
}

func (a *acceptor[T]) updateLocalCommitWithoutLock() *acceptor[T] {
	for {
		promise := a.acceptor.get(a.smallestUnapplied)
		if promise.Proposal != COMMITTED {
			break
		}
		if a.subsciber != nil {
			a.subsciber(a.smallestUnapplied, promise.Value)
		}
		a.smallestUnapplied++
	}
	return a
}

func (a *acceptor[T]) Subscribe(sm StateMachine[T]) (cancel func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.subsciber != nil {
		panic("cannot subscribe twice")
	}
	a.subsciber = sm

	for logId := a.snapshot; logId < a.smallestUnapplied; logId++ {
		promise := a.acceptor.get(logId)
		a.subsciber(logId, promise.Value)
	}
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.subsciber = nil
	}
}

func (a *acceptor[T]) Get(logId LogId) (T, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	promise := a.acceptor.get(logId)
	return promise.Value, promise.Proposal == COMMITTED
}

func (a *acceptor[T]) Next() LogId {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.updateLocalCommitWithoutLock().smallestUnapplied
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
	case *PollRequest:
		promise := a.acceptor.get(req.LogId)
		return &PollResponse[T]{
			Promise: promise,
		}
	default:
		return nil
	}
}
