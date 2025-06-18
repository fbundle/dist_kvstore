package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
)

const (
	PROPOSAL_STEP = 256
	DROP_CHANCE   = 0.9
)

type Proposal uint64

type Value string

type Acceptor struct {
	mu      sync.Mutex
	Promise Proposal
	Value   *Value
}

func (a *Acceptor) Prepare(proposal Proposal) (Proposal, *Value, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	promise, acceptedValue := a.Promise, a.Value
	if !(promise < proposal) {
		return promise, acceptedValue, false
	}
	a.Promise = proposal
	return promise, acceptedValue, true
}

func (a *Acceptor) Accept(proposal Proposal, value Value) (Proposal, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	promise := a.Promise
	if !(promise <= proposal) {
		return promise, false
	}
	a.Promise = proposal
	a.Value = &value
	return promise, true
}

type ProposerId uint64
type Round uint64

func compose(round Round, id ProposerId) Proposal {
	return Proposal(uint64(round)*PROPOSAL_STEP + uint64(id))
}

func decompose(proposal Proposal) (Round, ProposerId) {
	return Round(proposal / PROPOSAL_STEP), ProposerId(proposal % PROPOSAL_STEP)
}

func Propose(id ProposerId, acceptorList []*Acceptor, value Value) Value {
	quorum := len(acceptorList)/2 + 1
	round := Round(1)
	for {
		proposal := compose(round, id)
		// prepare phase
		maxPromise, maxValuePtr, ok := func() (Proposal, *Value, bool) {
			maxPromise := Proposal(0)
			maxValuePtr := (*Value)(nil)
			okCount := 0
			for _, a := range acceptorList {
				if rand.Float64() < DROP_CHANCE {
					continue
				}
				promise, valuePtr, ok := a.Prepare(proposal)
				if rand.Float64() < DROP_CHANCE {
					continue
				}
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

type Output struct {
	Id  ProposerId
	Val Value
}

func main() {
	n, m := 3, 5

	acceptorList := make([]*Acceptor, 0)
	for i := 0; i < n; i++ {
		acceptorList = append(acceptorList, &Acceptor{})
	}
	ch := make(chan Output, m)
	for j := 0; j < m; j++ {
		go func(j ProposerId) {
			value := Propose(j, acceptorList, Value(fmt.Sprintf("hello_%d", j)))
			ch <- Output{j, value}
		}(ProposerId(j))
	}
	for j := 0; j < m; j++ {
		o := <-ch
		fmt.Printf("proposer %d agrees at value %v\n", o.Id, o.Val)
	}
}
