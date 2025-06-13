package paxos

type NodeId uint32

const (
	PROPOSAL_STEP ProposalNumber = 4294967296
)

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
func Update(a Acceptor, rpcList []RPC) Acceptor {
	for {
		logId := a.UpdateLocalCommit().Next()
		commited := false
		var v Value = nil
		for _, res := range broadcast[*GetRequest, *GetResponse](rpcList, &GetRequest{
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
		a.Handle(&CommitRequest{
			LogId: logId,
			Value: v,
		})
	}
	return a
}

// Write - write new value
func Write(a Acceptor, id NodeId, logId LogId, value Value, rpcList []RPC) bool {
	n := len(rpcList)
	proposal := compose(0, id)
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
				continue
			}
		}
		// accept
		{
			resList := broadcast[*AcceptRequest, *AcceptResponse](rpcList, &AcceptRequest{
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
				continue
			}
		}
		// commit
		{
			// local commit
			a.Handle(&CommitRequest{
				LogId: logId,
				Value: value,
			})
			// broadcast commit
			broadcast[*CommitRequest, *CommitRequest](rpcList, &CommitRequest{
				LogId: logId,
				Value: value,
			})
		}
		return true
	}
}
