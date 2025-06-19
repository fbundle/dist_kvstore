package rpc

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/khanh101/paxos/pkg/crypt"
)

const (
	TCP_TIMEOUT = 10 * time.Second
	AES_KEY     = "AES_KEY"
)

type TCPServer interface {
	Handle(input []byte) (output []byte, err error)
	ListenAndServe() error
	Register(name string, h any) TCPServer
	Close() error
}

func getKey() crypt.Key {
	keyStr := os.Getenv(AES_KEY)
	if len(keyStr) == 0 {
		panic("no key found")
	}
	key := crypt.NewKey(keyStr)
	return key
}

func TCPTransport(addr string) TransportFunc {
	key := getKey()

	return func(b []byte) ([]byte, error) {

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		defer conn.Close()

		err = conn.SetDeadline(time.Now().Add(TCP_TIMEOUT))
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		err = key.EncryptToWriter(b, conn)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		b, err = key.DecryptFromReader(conn)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		return b, nil
	}
}

type tcpServer struct {
	mu         sync.Mutex
	dispatcher Dispatcher
	listener   net.Listener
	key        crypt.Key
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
		key:        getKey(),
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
	key := s.key
	defer conn.Close()
	err := conn.SetDeadline(time.Now().Add(TCP_TIMEOUT))
	if err != nil {
		fmt.Println(err)
		return
	}

	b, err := key.DecryptFromReader(conn)
	if err != nil {
		fmt.Println(err)
		return
	}

	{
		s.mu.Lock()
		defer s.mu.Unlock()
		b, err = s.dispatcher.Handle(b)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	err = key.EncryptToWriter(b, conn)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (s *tcpServer) ListenAndServe() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}
