package paxos

type Request interface {
}

type Response interface {
}
type PrepareRequest struct {
	LogId    LogId          `json:"log_id"`
	Proposal ProposalNumber `json:"proposal"`
}

type PrepareResponse[T comparable] struct {
	Proposal ProposalNumber `json:"proposal"`
	Value    *T             `json:"value"`
}

type AcceptRequest[T comparable] struct {
	LogId    LogId          `json:"log_id"`
	Proposal ProposalNumber `json:"proposal"`
	Value    T              `json:"value"`
}

type AcceptResponse struct {
	Proposal ProposalNumber `json:"proposal"`
}

type CommitRequest[T any] struct {
	LogId LogId `json:"log_id"`
	Value T     `json:"value"`
}

type CommitResponse struct {
}

type PollRequest struct {
	LogId LogId `json:"log_id"`
}

type PollResponse[T any] struct {
	Proposal ProposalNumber `json:"proposal"`
	Value    *T             `json:"value"`
}
