package paxos

import (
	"paxos/kvstore"
)

// ProposalNumber - roundId * 4294967296 + nodeId
type ProposalNumber uint64

const (
	ZERO     ProposalNumber = 0
	COMMITED ProposalNumber = 18446744073709551615
)

type Value interface{}

// Promise - promise to reject all PREPARE if proposal <= this and all ACCEPT if proposal < this
type Promise struct {
	Proposal ProposalNumber `json:"proposal"`
	Value    Value          `json:"value"`
}

type LogId uint64

type simpleAcceptor struct {
	log kvstore.Store[LogId, Promise]
}

func getOrSetLogEntry(txn kvstore.Txn[LogId, Promise], logId LogId) (p Promise) {
	if v, ok := txn.Get(logId); ok {
		return v
	}
	v := Promise{
		Proposal: ZERO,
		Value:    nil,
	}
	txn.Set(logId, v)
	return v
}

func (a *simpleAcceptor) get(logId LogId) (promise Promise) {
	return a.log.Update(func(txn kvstore.Txn[LogId, Promise]) any {
		return getOrSetLogEntry(txn, logId)
	}).(Promise)
}

func (a *simpleAcceptor) commit(logId LogId, v Value) {
	a.log.Update(func(txn kvstore.Txn[LogId, Promise]) any {
		txn.Set(logId, Promise{
			Proposal: COMMITED,
			Value:    v,
		})
		return nil
	})
}

func (a *simpleAcceptor) prepare(logId LogId, proposal ProposalNumber) (proposalOut ProposalNumber, ok bool) {
	r := a.log.Update(func(txn kvstore.Txn[LogId, Promise]) any {
		p := getOrSetLogEntry(txn, logId)
		if p.Proposal == COMMITED { // reject if committed
			return [2]any{COMMITED, false}
		}
		if proposal <= p.Proposal { // fulfill promise
			return [2]any{p.Proposal, false}
		}
		// make new promise
		txn.Set(logId, Promise{
			Proposal: proposal,
			Value:    p.Value, // old value
		})
		return [2]any{proposal, true}
	}).([2]any)
	return r[0].(ProposalNumber), r[1].(bool)
}

func (a *simpleAcceptor) accept(logId LogId, proposal ProposalNumber, value Value) (proposalOut ProposalNumber, ok bool) {
	r := a.log.Update(func(txn kvstore.Txn[LogId, Promise]) any {
		p := getOrSetLogEntry(txn, logId)
		if p.Proposal == COMMITED { // reject if committed
			return [2]any{COMMITED, false}
		}
		if proposal < p.Proposal { // fulfill promise
			return [2]any{p.Proposal, false}
		}
		// make new promise
		txn.Set(logId, Promise{
			Proposal: proposal,
			Value:    value, // new value
		})
		return [2]any{proposal, true}
	}).([2]any)
	return r[0].(ProposalNumber), r[1].(bool)
}
