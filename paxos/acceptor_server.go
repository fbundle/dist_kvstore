package paxos

import (
	"sync"
)

type Server interface {
	Get(LogId) (Value, bool)
	Next() LogId
	Handle(Request) Response
	Subscribe(from LogId, apply func(logId LogId, value Value)) (cancel func())
}

func NewServer() Server {
	return &server{
		mu:                 sync.Mutex{},
		acceptor:           &acceptor{},
		smallestUncommited: 0,
		subscriberCount:    0,
		subscriberMap:      make(map[uint64]func(logId LogId, value Value)),
	}
}

// server - paxos server must be persistent
type server struct {
	mu                 sync.Mutex
	acceptor           *acceptor
	smallestUncommited LogId
	subscriberCount    uint64
	subscriberMap      map[uint64]func(logId LogId, value Value)
}

func (s *server) Subscribe(from LogId, subscriber func(logId LogId, value Value)) (cancel func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.smallestUncommited < from {
		panic("subscribe from a future log_id")
	}
	for logId := from; logId < s.smallestUncommited; logId++ {
		subscriber(logId, s.acceptor.get(logId).Value)
	}
	count := s.subscriberCount
	s.subscriberCount++
	s.subscriberMap[count] = subscriber
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.subscriberMap, count)
	}
}

func (s *server) Get(logId LogId) (Value, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	promise := s.acceptor.get(logId)
	return promise.Value, promise.Proposal == COMMITED
}

func (s *server) Next() LogId {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.smallestUncommited
}
func (s *server) Handle(req Request) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch req := req.(type) {
	case *PrepareRequest:
		proposal, ok := s.acceptor.prepare(req.LogId, req.Proposal)
		return &PrepareResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *AcceptRequest:
		proposal, ok := s.acceptor.accept(req.LogId, req.Proposal, req.Value)
		return &AcceptResponse{
			Proposal: proposal,
			Ok:       ok,
		}
	case *CommitRequest:
		s.acceptor.commit(req.LogId, req.Value)
		for {
			promise := s.acceptor.get(s.smallestUncommited)
			if promise.Proposal != COMMITED {
				break
			}
			for _, subscriber := range s.subscriberMap {
				subscriber(s.smallestUncommited, promise.Value)
			}
			s.smallestUncommited++
		}
		return nil
	case *GetRequest:
		promise := s.acceptor.get(req.LogId)
		return &GetResponse{
			Promise: promise,
		}
	default:
		return nil
	}
}
