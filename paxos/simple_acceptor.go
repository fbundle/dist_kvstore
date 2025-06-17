package paxos

import (
	"github.com/khanh101/paxos/kvstore"
)

// ProposalNumber - roundId * 4294967296 + nodeId
type ProposalNumber uint64

type LogId uint64

const (
	INITIAL   ProposalNumber = 0
	COMMITTED ProposalNumber = 18446744073709551615
)

// Promise - promise to reject all PREPARE if proposal <= this and all ACCEPT if proposal < this
type Promise[T any] struct {
	Proposal ProposalNumber `json:"proposal"`
	Value    *T             `json:"value"`
}

func zero[T any]() T {
	var v T
	return v
}

type simpleAcceptor[T any] struct {
	log kvstore.Store[LogId, Promise[T]]
}

func getDefaultLogEntry[T any](txn kvstore.Txn[LogId, Promise[T]], logId LogId) (p Promise[T]) {
	v, ok := txn.Get(logId)
	if !ok {
		return Promise[T]{
			Proposal: INITIAL,
			Value:    nil,
		}
	}
	return v
}

func (a *simpleAcceptor[T]) get(logId LogId) (ProposalNumber, *T) {
	promise := a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		return getDefaultLogEntry(txn, logId)
	}).(Promise[T])
	return promise.Proposal, promise.Value
}

func (a *simpleAcceptor[T]) commit(logId LogId, v T) {
	a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		txn.Set(logId, Promise[T]{
			Proposal: COMMITTED,
			Value:    &v,
		})
		return nil
	})
}

func (a *simpleAcceptor[T]) prepare(logId LogId, proposal ProposalNumber) (Promise[T], bool) {
	r := a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		p := getDefaultLogEntry(txn, logId)
		if !(p.Proposal < proposal) {
			return [2]any{p, false}
		}
		txn.Set(logId, Promise[T]{
			Proposal: proposal,
			Value:    p.Value,
		})
		return [2]any{p, true}
	}).([2]any)
	promise, ok := r[0].(Promise[T]), r[1].(bool)
	return promise, ok
}

func (a *simpleAcceptor[T]) accept(logId LogId, proposal ProposalNumber, value T) (Promise[T], bool) {
	r := a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		p := getDefaultLogEntry(txn, logId)
		if !(p.Proposal <= proposal) {
			return [2]any{p, false}
		}
		txn.Set(logId, Promise[T]{
			Proposal: proposal,
			Value:    &value,
		})
		return [2]any{p, true}
	}).([2]any)
	promise, ok := r[0].(Promise[T]), r[1].(bool)
	return promise, ok
}
