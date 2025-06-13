package paxos

import (
	"sync"
)

type Server interface {
	Get(LogId) (Value, bool)
	Next() LogId
	Handle(Request) Response
}

func NewServer(apply func(LogId, Value)) Server {
	return &server{
		acceptor:    &acceptor{},
		nextApplyId: 0,
		apply:       apply,
	}
}

// server - paxos server must be persistent
type server struct {
	mu          sync.Mutex
	acceptor    *acceptor
	nextApplyId LogId
	apply       func(logId LogId, value Value)
}

func (s *server) Get(logId LogId) (Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	promise := s.acceptor.Get(logId)
	return promise.Value, promise.Proposal == COMMITED
}

func (s *server) Next() LogId {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nextApplyId
}
func (s *server) Handle(req Request) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
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
				s.apply(s.nextApplyId, promise.Value)
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
