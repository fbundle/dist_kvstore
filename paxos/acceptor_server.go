package paxos

type Server interface {
	Handle(Request) Response
}

func NewServer(apply func(Value)) Server {
	return &server{
		acceptor:    &acceptor{},
		nextApplyId: 0,
		apply:       apply,
	}
}

type server struct {
	acceptor    *acceptor
	nextApplyId LogId
	apply       func(value Value)
}

func (s *server) Handle(req Request) Response {
	switch req := req.(type) {
	case *PrepareRequest:
		proposal, ok := s.acceptor.Prepare(req.LogId, req.Proposal)
		return &PrepareResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *AcceptRequest:
		proposal, ok := s.acceptor.Accept(req.LogId, req.Proposal, req.Value)
		return &AcceptResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *CommitRequest:
		s.acceptor.Commit(req.LogId, req.Value)
		for {
			promise := s.acceptor.Get(s.nextApplyId)
			if promise.Proposal == COMMITED {
				s.apply(promise.Value)
				s.nextApplyId++
				continue
			}
			break
		}
		return nil
	case *GetRequest:
		promise := s.acceptor.Get(req.LogId)
		return &GetResponse{
			Promise: promise,
		}
	default:
		return nil
	}
}
