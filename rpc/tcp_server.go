package rpc

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type TCPServer interface {
	Handle(input []byte) (output []byte, err error)
	Run() error
	Register(name string, h any) TCPServer
	Close() error
}

func TCPTransport(addr string) TransportFunc {
	return func(input []byte) (output []byte, err error) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		err = conn.SetDeadline(time.Now().Add(10 * time.Second))
		if err != nil {
			return
		}

		conn.Write(input)
		conn.Write([]byte("\n")) // '\n' notifies end of input
		output, err = io.ReadAll(conn)
		return output, err
	}
}

type tcpServer struct {
	mu         sync.Mutex
	dispatcher Dispatcher
	listener   net.Listener
}

func NewTCPServer(bindAddr string) (TCPServer, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return &tcpServer{
		mu:         sync.Mutex{},
		dispatcher: NewDispatcher(),
		listener:   listener,
	}, nil
}

func (s *tcpServer) Close() error {
	return s.listener.Close()
}

func (s *tcpServer) Handle(input []byte) (output []byte, err error) {
	return s.dispatcher.Handle(input)
}

func (s *tcpServer) Register(name string, h any) TCPServer {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatcher.Register(name, h)
	return s
}

func (s *tcpServer) handleConn(conn net.Conn) {
	defer conn.Close()
	err := conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return
	}

	msg, err := bufio.NewReader(conn).ReadString('\n') // read until '\n'
	if err != nil {
		return
	}
	b := []byte(msg)
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
