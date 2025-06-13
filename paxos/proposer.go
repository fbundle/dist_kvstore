package paxos

type NodeId uint32

const (
	PROPOSAL_STEP ProposalNumber = 4294967296
)

func quorum(n int) int {
	return n/2 + 1
}

// Update - check if there is an update
func Update(s Acceptor, rpcList []RPC) {
	for {
		logId := s.Next()
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
		s.Handle(&CommitRequest{
			LogId: logId,
			Value: v,
		})
	}
}

// Write - write new value
func Write(s Acceptor, id NodeId, logId LogId, value Value, rpcList []RPC) bool {
	resetProposal := func() ProposalNumber {
		return ProposalNumber(id)
	}
	n := len(rpcList)
	proposal := resetProposal()
	for {
		Update(s, rpcList)
		if _, committed := s.Get(logId); committed {
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
				proposal = proposal + PROPOSAL_STEP
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
				proposal = proposal + PROPOSAL_STEP
				continue
			}
		}
		// commit
		{
			// local commit
			s.Handle(&CommitRequest{
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
