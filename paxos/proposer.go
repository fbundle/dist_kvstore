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
			if res.Proposal == COMMITTED {
				v = *res.Value
				commited = true
				break
			}
		}
		if !commited {
			break
		}
		a.HandleRPC(&CommitRequest[T]{
			LogId: logId,
			Value: v,
		})
	}
	return a
}

// Write - write new value
func Write[T comparable](a Acceptor[T], id NodeId, logId LogId, x T, rpcList []RPC) bool {
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
		if _, committed := a.GetValue(logId); committed {
			return false
		}
		// prepare
		var maxValue T
		var maxProposal ProposalNumber
		var okCount int
		{
			resList := broadcast[*PrepareRequest, *PrepareResponse[T]](rpcList, &PrepareRequest{
				LogId:    logId,
				Proposal: proposal,
			})

			maxValue, maxProposal, okCount = func() (T, ProposalNumber, int) {
				maxValue := zero[*T]()
				maxProposal := ProposalNumber(0)
				okCount := 0
				for _, res := range resList {
					if res.Proposal == proposal {
						okCount++
					}
					if maxProposal < res.Proposal {
						maxProposal = res.Proposal
						maxValue = res.Value
					}
				}
				if maxValue == nil {
					maxValue = &x
				}
				return *maxValue, maxProposal, okCount
			}()
		}

		if okCount < quorum(n) {
			maxRound, _ := decompose(maxProposal)
			proposal = compose(maxRound+1, id)
			backoff()
			continue
		}
		// accept
		{
			resList := broadcast[*AcceptRequest[T], *AcceptResponse](rpcList, &AcceptRequest[T]{
				LogId:    logId,
				Proposal: proposal,
				Value:    maxValue,
			})
			maxProposal, okCount = func() (ProposalNumber, int) {
				maxProposal := ProposalNumber(0)
				okCount := 0
				for _, res := range resList {
					if res.Proposal == proposal {
						okCount++
					}
					if maxProposal < res.Proposal {
						maxProposal = res.Proposal
					}
				}
				return maxProposal, okCount
			}()
		}
		if okCount < quorum(n) {
			maxRound, _ := decompose(maxProposal)
			proposal = compose(maxRound+1, id)
			backoff()
			continue
		}
		// commit
		{
			// broadcast commit
			go broadcast[*CommitRequest[T], *CommitResponse](rpcList, &CommitRequest[T]{
				LogId: logId,
				Value: maxValue,
			})
			// local commit
			a.HandleRPC(&CommitRequest[T]{
				LogId: logId,
				Value: maxValue,
			})
		}
		return maxValue == x
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

	/*

	 */
	// compressLog[T any](ts ...T) T // type signature for log compression where T is type of value
}
