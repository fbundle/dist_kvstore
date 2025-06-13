package paxos

type RPC func(Request) Response

func broadcast[Req any, Res any](rpcList []RPC, req Req) []Res {
	resList := make([]Res, 0, len(rpcList))
	for _, rpc := range rpcList {
		res := rpc(req)
		if res != nil {
			resList = append(resList, res.(Res))
		}
	}
	return resList
}
