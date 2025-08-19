package rpc

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/fbundle/go_util/pkg/crypt"
)

const (
	TCP_TIMEOUT = 10 * time.Second
	RPC_KEY_ENV = "DIST_KVSTORE_RPC_KEY"
)

type TCPServer interface {
	ListenAndServe(dispatcher Dispatcher) error
	Close() error
}

func getKey() IO {
	keyStr := os.Getenv(RPC_KEY_ENV)
	key := NewCryptIO(crypt.NewCrypt(keyStr))
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

		err = key.Write(b, conn)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		b, err = key.Read(conn)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		return b, nil
	}
}

type tcpServer struct {
	dispatcher Dispatcher
	listener   net.Listener
	key        IO
}

func NewTCPServer(bindAddr string) (TCPServer, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	return &tcpServer{
		dispatcher: nil,
		listener:   listener,
		key:        getKey(),
	}, nil
}

func (s *tcpServer) Close() error {
	return s.listener.Close()
}

func (s *tcpServer) handleConn(conn net.Conn) {
	key := s.key
	defer conn.Close()
	err := conn.SetDeadline(time.Now().Add(TCP_TIMEOUT))
	if err != nil {
		fmt.Println(err)
		return
	}

	b, err := key.Read(conn)
	if err != nil {
		fmt.Println(err)
		return
	}

	b, err = s.dispatcher.Handle(b)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = key.Write(b, conn)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func (s *tcpServer) ListenAndServe(dispatcher Dispatcher) error {
	s.dispatcher = dispatcher
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}
