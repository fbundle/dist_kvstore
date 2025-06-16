package dist_kvstore

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/khanh101/paxos/kvstore"
	"github.com/khanh101/paxos/paxos"
	"github.com/khanh101/paxos/rpc"
)

type Store interface {
}

type store struct {
	db       *badger.DB
	store    kvstore.Store[string, string]
	acceptor paxos.Acceptor
	server   rpc.TCPServer
}

func NewStore(id int, badgerPath string, peerAddrList []string) (Store, error) {
	bindAddr := peerAddrList[id]
	db, err := badger.Open(badger.DefaultOptions(badgerPath))
	if err != nil {
		return nil, err
	}
	acceptor := paxos.NewAcceptor(kvstore.NewBargerStore[paxos.LogId, paxos.Promise](db))
	store := kvstore.NewMemStore[string, string]()
}
