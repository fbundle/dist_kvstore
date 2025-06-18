package simple_paxos

type Proposal uint64

type Value string

type Acceptor struct {
	Promise Proposal
	Value   *Value
}

func (a *Acceptor) Prepare(proposal Proposal) (Proposal, *Value, bool) {
	promise, acceptedValue := a.Promise, a.Value
	if !(promise < proposal) {
		return promise, acceptedValue, false
	}
	a.Promise = proposal
	return promise, acceptedValue, true
}

func (a *Acceptor) Accept(proposal Proposal, value Value) (Proposal, bool) {
	promise := a.Promise
	if !(promise <= proposal) {
		return promise, false
	}
	a.Promise = proposal
	a.Value = &value
	return promise, true
}

const (
	ProposalStep = 256
)

type ProposerId uint64
type Round uint64

func compose(round Round, id ProposerId) Proposal {
	return Proposal(uint64(round)*ProposalStep + uint64(id))
}

func decompose(proposal Proposal) (Round, ProposerId) {
	return Round(proposal / ProposalStep), ProposerId(proposal % ProposalStep)
}

func Propose(id ProposerId, acceptorList []*Acceptor, value Value) Value {
	quorum := len(acceptorList)/2 + 1
	round := Round(0)
	for {
		proposal := compose(round, id)
		// prepare phase
		maxPromise, maxValuePtr, ok := func() (Proposal, *Value, bool) {
			maxPromise := Proposal(0)
			maxValuePtr := (*Value)(nil)
			okCount := 0
			for _, a := range acceptorList {
				promise, valuePtr, ok := a.Prepare(proposal)
				if ok {
					okCount++
				}
				if maxPromise <= promise {
					maxPromise = promise
					maxValuePtr = valuePtr
				}
			}
			return maxPromise, maxValuePtr, okCount >= quorum
		}()
		if !ok {
			// backoff
			round, _ = decompose(maxPromise)
			round++
			continue
		}
		// accept phase
		if maxValuePtr == nil {
			maxValuePtr = &value
		}
		maxPromise, ok = func() (Proposal, bool) {
			maxPromise := Proposal(0)
			okCount := 0
			for _, a := range acceptorList {
				promise, ok := a.Accept(proposal, *maxValuePtr)
				if ok {
					okCount++
				}
				if maxPromise <= promise {
					maxPromise = promise
				}
			}
			return maxPromise, okCount >= quorum
		}()
		if !ok {
			// backoff
			round, _ = decompose(maxPromise)
			round++
			continue
		}
		// consensus has been reached at value (*maxValuePtr)
		return *maxValuePtr
	}
}
