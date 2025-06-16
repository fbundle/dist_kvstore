package paxos

import "time"

type NodeId uint64

const (
	PROPOSAL_STEP ProposalNumber = 4294967296
)

func decompose(proposal ProposalNumber) (uint64, NodeId) {
	return uint64(proposal / PROPOSAL_STEP), NodeId(proposal % PROPOSAL_STEP)
}
func compose(round uint64, nodeId NodeId) ProposalNumber {
	return PROPOSAL_STEP*ProposalNumber(round) + ProposalNumber(nodeId)
}

func quorum(n int) int {
	return n/2 + 1
}

type RPC func(Request, chan<- Response)

func broadcast[Req any, Res any](rpcList []RPC, req Req) []Res {
	ch := make(chan Response, len(rpcList))
	defer close(ch)
	for _, rpc := range rpcList {
		rpc(req, ch)
	}
	resList := make([]Res, 0, len(rpcList))
	for range rpcList {
		res := <-ch
		if res == nil {
			continue
		}
		resList = append(resList, res.(Res))
	}
	return resList
}

// Update - check if there is an update
func Update[T any](a Acceptor[T], rpcList []RPC) Acceptor[T] {
	wait := time.Millisecond
	// exponential backoff
	backoff := func() {
		time.Sleep(wait)
		wait *= 2
	}
	for {
		logId := a.UpdateLocalCommit().Next()
		commited := false
		var v T
		for _, res := range broadcast[*GetRequest, *GetResponse[T]](rpcList, &GetRequest{
			LogId: logId,
		}) {
			if res.Promise.Proposal == COMMITED {
				v = res.Promise.Value
				commited = true
				break
			}
		}
		if !commited {
			break
		}
		a.Handle(&CommitRequest[T]{
			LogId: logId,
			Value: v,
		})
		backoff()
	}
	return a
}

// Write - write new value
func Write[T any](a Acceptor[T], id NodeId, logId LogId, value T, rpcList []RPC) bool {
	n := len(rpcList)
	proposal := compose(0, id)
	wait := time.Millisecond
	// exponential backoff
	backoff := func() {
		time.Sleep(wait)
		wait *= 2
	}
	for {
		if _, committed := Update(a, rpcList).Get(logId); committed {
			return false
		}
		// prepare
		{
			resList := broadcast[*PrepareRequest, *PrepareResponse](rpcList, &PrepareRequest{
				LogId:    logId,
				Proposal: proposal,
			})
			okCount := 0
			for _, res := range resList {
				if res.Ok {
					okCount++
				}
			}
			if okCount < quorum(n) {
				// update proposal
				maxRound := uint64(0)
				for _, res := range resList {
					round, _ := decompose(res.Proposal)
					if maxRound < round {
						maxRound = round
					}
				}
				proposal = compose(maxRound+1, id)
				backoff()
				continue
			}
		}
		// accept
		{
			resList := broadcast[*AcceptRequest[T], *AcceptResponse](rpcList, &AcceptRequest[T]{
				LogId:    logId,
				Proposal: proposal,
				Value:    value,
			})
			okCount := 0
			for _, res := range resList {
				if res.Ok {
					okCount++
				}
			}
			if okCount < quorum(n) {
				// update proposal
				maxRound := uint64(0)
				for _, res := range resList {
					round, _ := decompose(res.Proposal)
					if maxRound < round {
						maxRound = round
					}
				}
				proposal = compose(maxRound+1, id)
				backoff()
				continue
			}
		}
		// commit
		{
			// local commit
			a.Handle(&CommitRequest[T]{
				LogId: logId,
				Value: value,
			})
			// broadcast commit
			broadcast[*CommitRequest[T], *CommitResponse](rpcList, &CommitRequest[T]{
				LogId: logId,
				Value: value,
			})
		}
		return true
	}
}
