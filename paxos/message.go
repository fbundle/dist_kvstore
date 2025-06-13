package paxos

type Request interface {
}

type Response interface {
}
type PrepareRequest struct {
	LogId    LogId          `json:"log_id"`
	Proposal ProposalNumber `json:"proposal"`
}

type PrepareResponse struct {
	Proposal ProposalNumber `json:"proposal"`
	Ok       bool           `json:"ok"`
}

type AcceptRequest struct {
	LogId    LogId          `json:"log_id"`
	Proposal ProposalNumber `json:"proposal"`
	Value    any            `json:"value"`
}

type AcceptResponse struct {
	Proposal ProposalNumber `json:"proposal"`
	Ok       bool           `json:"ok"`
}

type CommitRequest struct {
	LogId LogId `json:"log_id"`
	Value any   `json:"value"`
}

type CommitResponse struct {
}

type GetRequest struct {
	LogId LogId `json:"log_id"`
}

type GetResponse struct {
	Promise Promise `json:"promise"`
}
