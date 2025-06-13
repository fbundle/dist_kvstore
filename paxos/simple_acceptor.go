package paxos

import "paxos/kvstore"

type ProposalNumber uint64

const (
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
	log kvstore.Store[LogId, Promise] // should be persistent store
}

func (a *simpleAcceptor) get(logId LogId) Promise {
	var promise Promise
	a.log.Update(logId, func(p Promise) Promise {
		promise = p
		return p
	})
	return promise
}

func (a *simpleAcceptor) commit(logId LogId, v Value) {
	a.log.Update(logId, func(_ Promise) Promise {
		return Promise{
			Proposal: COMMITED,
			Value:    v,
		}
	})
}

func (a *simpleAcceptor) prepare(logId LogId, proposal ProposalNumber) (proposalOut ProposalNumber, ok bool) {
	a.log.Update(logId, func(p Promise) Promise {
		if p.Proposal == COMMITED {
			// reject if committed
			proposalOut, ok = COMMITED, false
			return p
		}
		if proposal <= p.Proposal {
			// fulfill promise
			proposalOut, ok = p.Proposal, false
			return p
		}
		// make promise
		p.Proposal = proposal
		proposalOut, ok = proposal, true
		return p
	})
	return
}

func (a *simpleAcceptor) accept(logId LogId, proposal ProposalNumber, value Value) (proposalOut ProposalNumber, ok bool) {
	a.log.Update(logId, func(p Promise) Promise {
		if p.Proposal == COMMITED {
			// reject if committed
			proposalOut, ok = COMMITED, false
			return p
		}
		if proposal < p.Proposal {
			// fulfill promise
			proposalOut, ok = p.Proposal, false
			return p
		}
		// make promise
		p.Proposal = proposal
		p.Value = value
		proposalOut, ok = proposal, true
		return p
	})
	return
}
