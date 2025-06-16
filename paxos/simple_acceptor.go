package paxos

import (
	"github.com/khanh101/paxos/kvstore"
)

// ProposalNumber - roundId * 4294967296 + nodeId
type ProposalNumber uint64

const (
	INITIAL   ProposalNumber = 0
	COMMITTED ProposalNumber = 18446744073709551615
)

// Promise[T] - promise to reject all PREPARE if proposal <= this and all ACCEPT if proposal < this
type Promise[T any] struct {
	Proposal ProposalNumber `json:"proposal"`
	Value    T              `json:"value"`
}

func zero[T any]() T {
	var v T
	return v
}

type LogId uint64

type simpleAcceptor[T any] struct {
	log kvstore.Store[LogId, Promise[T]]
}

func getOrSetLogEntry[T any](txn kvstore.Txn[LogId, Promise[T]], logId LogId) (p Promise[T]) {
	if v, ok := txn.Get(logId); ok {
		return v
	}
	v := Promise[T]{
		Proposal: INITIAL,
		Value:    zero[T](),
	}
	txn.Set(logId, v)
	return v
}

func (a *simpleAcceptor[T]) get(logId LogId) (promise Promise[T]) {
	return a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		return getOrSetLogEntry(txn, logId)
	}).(Promise[T])
}

func (a *simpleAcceptor[T]) commit(logId LogId, v T) {
	a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		txn.Set(logId, Promise[T]{
			Proposal: COMMITTED,
			Value:    v,
		})
		return nil
	})
}

func (a *simpleAcceptor[T]) prepare(logId LogId, proposal ProposalNumber) (proposalOut ProposalNumber, ok bool) {
	r := a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		p := getOrSetLogEntry(txn, logId)
		if p.Proposal == COMMITTED { // reject if committed
			return [2]any{COMMITTED, false}
		}
		if proposal <= p.Proposal { // fulfill promise
			return [2]any{p.Proposal, false}
		}
		// make new promise
		txn.Set(logId, Promise[T]{
			Proposal: proposal,
			Value:    p.Value, // old value
		})
		return [2]any{proposal, true}
	}).([2]any)
	return r[0].(ProposalNumber), r[1].(bool)
}

func (a *simpleAcceptor[T]) accept(logId LogId, proposal ProposalNumber, value T) (proposalOut ProposalNumber, ok bool) {
	r := a.log.Update(func(txn kvstore.Txn[LogId, Promise[T]]) any {
		p := getOrSetLogEntry(txn, logId)
		if p.Proposal == COMMITTED { // reject if committed
			return [2]any{COMMITTED, false}
		}
		if proposal < p.Proposal { // fulfill promise
			return [2]any{p.Proposal, false}
		}
		// make new promise
		txn.Set(logId, Promise[T]{
			Proposal: proposal,
			Value:    value, // new value
		})
		return [2]any{proposal, true}
	}).([2]any)
	return r[0].(ProposalNumber), r[1].(bool)
}
