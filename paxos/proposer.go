package paxos

import (
	"math/rand"
	"time"
)

type NodeId uint64

const (
	PROPOSAL_STEP    ProposalNumber = 4294967296
	BACKOFF_MIN_TIME time.Duration  = 10 * time.Millisecond
	BACKOFF_MAX_TIME time.Duration  = 1000 * time.Millisecond
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
	for {
		logId := a.Next()
		commited := false
		var v T
		for _, res := range broadcast[*PollRequest, *PollResponse[T]](rpcList, &PollRequest{
			LogId: logId,
		}) {
			if res.Promise.Proposal == COMMITTED {
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
	}
	return a
}

// Write - write new value
func Write[T any](a Acceptor[T], id NodeId, logId LogId, value T, rpcList []RPC) bool {
	n := len(rpcList)
	proposal := compose(0, id)
	wait := BACKOFF_MIN_TIME
	// exponential backoff
	backoff := func() {
		a = Update(a, rpcList)
		time.Sleep(time.Duration(rand.Intn(int(wait))))
		wait *= 2
		if wait > BACKOFF_MAX_TIME {
			wait = BACKOFF_MAX_TIME
		}
	}
	for {
		if _, committed := a.Get(logId); committed {
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
			// broadcast commit
			go broadcast[*CommitRequest[T], *CommitResponse](rpcList, &CommitRequest[T]{
				LogId: logId,
				Value: value,
			})
			// local commit
			a.Handle(&CommitRequest[T]{
				LogId: logId,
				Value: value,
			})
		}
		return true
	}
}

func LogCompact[T any](rpcList []RPC) {
	// TODO
	// 1. send request to get smallest logId
	// 2. emit log compaction request
	// 3. T is a sum type of Command CompressedCommand.
	//    compact log entries [0, 1, 2, 3, ...] -> [x, 2, 3, ...]
	//    where x stores CompressedCommand
	// 4. StateMachine applies CompressedCommand is recovering from snapshot
	// 5. initLogId is used to restore the StateMachine
}
