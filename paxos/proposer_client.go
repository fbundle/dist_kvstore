package paxos

type NodeId uint32

const (
	PROPOSAL_STEP ProposalNumber = 4294967296
)

func quorum(n int) int {
	return n/2 + 1
}

func Touch(s Server, rpcList []RPC) {
	logId := s.GetNextApplyId()
	{
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
		if commited {
			s.Handle(&CommitRequest{
				LogId: logId,
				Value: v,
			})
		}
	}
}

func Write(s Server, id NodeId, value Value, rpcList []RPC) {
	n := len(rpcList)
	proposal := ProposalNumber(id)
	for {
		logId := s.GetNextApplyId()
		// get
		{
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
			if commited {
				s.Handle(&CommitRequest{
					LogId: logId,
					Value: v,
				})
				// reset proposal
				proposal = ProposalNumber(id)
				continue
			}
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
		// commitWithoutLock
		{
			// local commit
			s.Handle(&CommitRequest{
				LogId: logId,
				Value: value,
			})
		}
		break
	}
}
