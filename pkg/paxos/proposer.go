package paxos

import (
	"math/rand"
	"time"
)

type ProposerId uint64

const (
	PROPOSAL_STEP    = 4294967296
	BACKOFF_MIN_TIME = 10 * time.Millisecond
	BACKOFF_MAX_TIME = 1000 * time.Millisecond
)

type Round uint64

func compose(round Round, id ProposerId) Proposal {
	return Proposal(uint64(round)*PROPOSAL_STEP + uint64(id))
}

func decompose(proposal Proposal) (Round, ProposerId) {
	return Round(proposal / PROPOSAL_STEP), ProposerId(proposal % PROPOSAL_STEP)
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
func Write[T any](a Acceptor[T], id ProposerId, logId LogId, value T, rpcList []RPC) (T, bool) {
	quorum := len(rpcList)/2 + 1
	round := Round(1)

	wait := BACKOFF_MIN_TIME
	// exponential backoff
	backoff := func() {
		round++
		a = Update(a, rpcList)
		time.Sleep(time.Duration(rand.Intn(int(wait))))
		wait *= 2
		if wait > BACKOFF_MAX_TIME {
			wait = BACKOFF_MAX_TIME
		}
	}
	for {
		if _, committed := a.GetValue(logId); committed {
			return zero[T](), false
		}
		// prepare
		proposal := compose(round, id)
		maxValuePtr, ok := func() (*T, bool) {
			maxPromise := Proposal(0)
			maxValuePtr := (*T)(nil)
			okCount := 0
			resList := broadcast[*PrepareRequest, *PrepareResponse[T]](rpcList, &PrepareRequest{
				LogId:    logId,
				Proposal: proposal,
			})
			for _, res := range resList {
				if res.Ok {
					okCount++
					if res.Promise.Value != nil {
						if maxPromise <= res.Promise.Proposal {
							maxPromise = res.Promise.Proposal
							maxValuePtr = res.Promise.Value
						}
					}
				}
			}
			return maxValuePtr, okCount >= quorum
		}()

		if !ok {
			backoff()
			continue
		}
		// accept
		if maxValuePtr == nil {
			maxValuePtr = &value
		}

		ok = func() bool {
			resList := broadcast[*AcceptRequest[T], *AcceptResponse[T]](rpcList, &AcceptRequest[T]{
				LogId:    logId,
				Proposal: proposal,
				Value:    *maxValuePtr,
			})
			okCount := 0
			for _, res := range resList {
				if res.Ok {
					okCount++
				}
			}
			return okCount >= quorum
		}()
		if !ok {
			backoff()
			continue
		}
		// commit
		func() {
			// broadcast commit
			go broadcast[*CommitRequest[T], *CommitResponse](rpcList, &CommitRequest[T]{
				LogId: logId,
				Value: *maxValuePtr,
			})
			// local commit
			a.HandleRPC(&CommitRequest[T]{
				LogId: logId,
				Value: *maxValuePtr,
			})
		}()
		return *maxValuePtr, true
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
