package paxos

import (
	"sync"

	"github.com/khanh101/paxos/kvstore"
)

type StateMachine[T any] func(logId LogId, value T)

type Acceptor[T any] interface {
	// GetValue - get value
	GetValue(logId LogId) (val T, ok bool)
	// Next - get smallestUnapplied - used to propose
	Next() LogId
	// HandleRPC - handle RPC requests
	HandleRPC(req Request) (res Response)
	// Subscribe - subscribe a state machine to log
	// smallestUnapplied is the index when state machine will start getting updates
	// it ignores all previous log entries
	Subscribe(smallestUnapplied LogId, sm StateMachine[T]) (cancel func())
}

func NewAcceptor[T any](log kvstore.Store[LogId, Promise[T]]) Acceptor[T] {
	return (&acceptor[T]{
		mu:                sync.Mutex{},
		acceptor:          &simpleAcceptor[T]{log: log},
		smallestUnapplied: 0,
		subsciber:         nil,
	}).applyCommitWithoutLock()
}

// acceptor - paxos acceptor must be persistent
type acceptor[T any] struct {
	mu                sync.Mutex
	acceptor          *simpleAcceptor[T]
	smallestUnapplied LogId
	subsciber         StateMachine[T]
}

func (a *acceptor[T]) applyCommitWithoutLock() *acceptor[T] {
	for {
		proposal, value := a.acceptor.get(a.smallestUnapplied)
		if proposal != COMMITTED {
			break
		}
		if a.subsciber != nil {
			a.subsciber(a.smallestUnapplied, *value)
		}
		a.smallestUnapplied++
	}
	return a
}

func (a *acceptor[T]) Subscribe(smallestUnapplied LogId, sm StateMachine[T]) (cancel func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.subsciber != nil {
		panic("cannot subscribe twice")
	}
	a.subsciber = sm
	a.smallestUnapplied = smallestUnapplied

	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		a.subsciber = nil
	}
}

func (a *acceptor[T]) GetValue(logId LogId) (T, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	proposal, value := a.acceptor.get(logId)
	if proposal == COMMITTED {
		return *value, true
	}
	return zero[T](), false
}

func (a *acceptor[T]) Next() LogId {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applyCommitWithoutLock().smallestUnapplied
}

func (a *acceptor[T]) HandleRPC(r Request) Response {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch req := r.(type) {
	case *PrepareRequest:
		promise, ok := a.acceptor.prepare(req.LogId, req.Proposal)
		return &PrepareResponse[T]{
			Promise: promise,
			Ok:      ok,
		}
	case *AcceptRequest[T]:
		promise, ok := a.acceptor.accept(req.LogId, req.Proposal, req.Value)
		return &AcceptResponse[T]{
			Promise: promise,
			Ok:      ok,
		}
	case *CommitRequest[T]:
		a.acceptor.commit(req.LogId, req.Value)
		a.applyCommitWithoutLock()
		return nil
	case *PollRequest:
		proposal, value := a.acceptor.get(req.LogId)
		return &PollResponse[T]{
			Proposal: proposal,
			Value:    value,
		}
	default:
		return nil
	}
}
