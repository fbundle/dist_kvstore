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

type AcceptRequest[T any] struct {
	LogId    LogId          `json:"log_id"`
	Proposal ProposalNumber `json:"proposal"`
	Value    T              `json:"value"`
}

type AcceptResponse struct {
	Proposal ProposalNumber `json:"proposal"`
	Ok       bool           `json:"ok"`
}

type CommitRequest[T any] struct {
	LogId LogId `json:"log_id"`
	Value T     `json:"value"`
}

type CommitResponse struct {
}

type GetRequest struct {
	LogId LogId `json:"log_id"`
}

type GetResponse[T any] struct {
	Promise Promise[T] `json:"promise"`
}
