package rpc

import (
	"fmt"
	"io"
	"net"
	"sync"
)

type TCPServer interface {
	Run() error
	Append(name string, h any) TCPServer
	Close() error
}

type tcpServer struct {
	mu         sync.Mutex
	dispatcher *Dispatcher
	listener   net.Listener
}

func NewTCPTCPServer(bindAddr string) (TCPServer, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return &tcpServer{
		listener: listener,
	}, nil
}

func (s *tcpServer) Close() error {
	return s.listener.Close()
}

func (s *tcpServer) Append(name string, h any) TCPServer {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatcher.Append(name, h)
	return s
}

func (s *tcpServer) handleConn(conn net.Conn) {
	defer conn.Close()
	b, err := io.ReadAll(conn)
	if err != nil {
		fmt.Println(err)
		return
	}
	{
		s.mu.Lock()
		defer s.mu.Unlock()
		b, err = s.dispatcher.Handle(b)
	}
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write(b)
}

func (s *tcpServer) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}
