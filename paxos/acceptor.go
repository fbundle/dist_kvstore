package paxos

type ProposalNumber uint64

const (
	COMMITED ProposalNumber = 18446744073709551615
)

type Value interface{}

type Promise struct {
	// promise to reject all PREPARE if proposal <= this and all ACCEPT if proposal < this
	Proposal ProposalNumber `json:"proposal"`
	Value    Value          `json:"value"`
}

type LogId uint64

type acceptor struct {
	log []*Promise
}

func (a *acceptor) Get(logId LogId) *Promise {
	for len(a.log) <= int(logId) {
		a.log = append(a.log, &Promise{
			Proposal: 0,
		})
	}
	return a.log[logId]
}

func (a *acceptor) Commit(logId LogId, v Value) {
	promise := a.Get(logId)
	promise.Proposal = COMMITED
	promise.Value = v
}

func (a *acceptor) Prepare(logId LogId, proposal ProposalNumber) (ProposalNumber, bool) {
	promise := a.Get(logId)

	if promise.Proposal == COMMITED {
		// reject if committed
		return COMMITED, false
	}

	if proposal <= promise.Proposal {
		// fulfill promise
		return promise.Proposal, false
	}
	promise.Proposal = proposal
	return promise.Proposal, true
}

func (a *acceptor) Accept(logId LogId, proposal ProposalNumber, value Value) (ProposalNumber, bool) {
	promise := a.Get(logId)

	if promise.Proposal == COMMITED {
		// reject if committed
		return COMMITED, false
	}

	if proposal < promise.Proposal {
		// fulfill promise
		return promise.Proposal, false
	}
	promise.Proposal = proposal
	promise.Value = value
	return promise.Proposal, true
}
