package paxos

type Request interface {
}

type Response interface {
}
type PrepareRequest struct {
	LogId    LogId    `json:"log_id"`
	Proposal Proposal `json:"proposal"`
}

type PrepareResponse[T any] struct {
	Promise Promise[T] `json:"promise"`
	Ok      bool       `json:"ok"`
}

type AcceptRequest[T any] struct {
	LogId    LogId    `json:"log_id"`
	Proposal Proposal `json:"proposal"`
	Value    T        `json:"value"`
}

type AcceptResponse[T any] struct {
	Promise Promise[T] `json:"promise"`
	Ok      bool       `json:"ok"`
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
	Proposal Proposal `json:"proposal"`
	Value    *T       `json:"value"`
}
