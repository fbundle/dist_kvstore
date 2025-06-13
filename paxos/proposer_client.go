package paxos

type ProposerId uint32

const (
	PROPOSAL_STEP ProposalNumber = 4294967296
)

type RPC func(Request) Response

func quorum(n int) int {
	return n/2 + 1
}

func Append(id ProposerId, value Value, rpcList []RPC) {
	n := len(rpcList)
	logId := LogId(0)
	proposal := ProposalNumber(id)
	for {
		// get
		getResponseList := make([]*GetResponse, n)
		for i, rpc := range rpcList {
			res := rpc(&GetRequest{
				LogId: logId,
			}).(*GetResponse)
			getResponseList[i] = res
		}
		for _, res := range getResponseList {
			if res != nil && res.Promise.Proposal == COMMITED {
				// next logId
				logId = logId + 1
				proposal = ProposalNumber(id)
				continue
			}
		}
		// prepare
		prepareResponseList := make([]*PrepareResponse, n)
		for i, rpc := range rpcList {
			res := rpc(&PrepareRequest{
				LogId:    logId,
				Proposal: proposal,
			}).(*PrepareResponse)
			prepareResponseList[i] = res
		}
		okCount := 0
		for _, res := range prepareResponseList {
			if res != nil && res.Ok {
				okCount++
			}
		}
		if okCount < quorum(n) {
			// next proposal
			proposal = proposal + PROPOSAL_STEP
			continue
		}
		// accept
		acceptResponseList := make([]*AcceptResponse, n)
		for i, rpc := range rpcList {
			res := rpc(&AcceptRequest{
				LogId:    logId,
				Proposal: proposal,
				Value:    value,
			}).(*AcceptResponse)
			acceptResponseList[i] = res
		}
		okCount = 0
		for _, res := range acceptResponseList {
			if res != nil && res.Ok {
				okCount++
			}
		}
		if okCount < quorum(n) {
			// next proposal
			proposal = proposal + PROPOSAL_STEP
			continue
		}
		// commit
		for _, rpc := range rpcList {
			rpc(&CommitRequest{
				LogId: logId,
				Value: value,
			})
		}
	}
}
