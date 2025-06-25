package rpc

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/khanh101/paxos/pkg/crypt"
)

const (
	TCP_TIMEOUT = 10 * time.Second
	RPC_KEY_ENV = "DIST_KVSTORE_RPC_KEY"
)

type TCPServer interface {
	ListenAndServe(dispatcher Dispatcher) error
	Close() error
}

func getKey() crypt.CryptStream {
	keyStr := os.Getenv(RPC_KEY_ENV)
	key := crypt.NewStream(crypt.NewCrypt(keyStr))
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
	dispatcher Dispatcher
	listener   net.Listener
	key        crypt.CryptStream
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

	b, err := key.DecryptFromReader(conn)
	if err != nil {
		fmt.Println(err)
		return
	}

	b, err = s.dispatcher.Handle(b)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = key.EncryptToWriter(b, conn)
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
