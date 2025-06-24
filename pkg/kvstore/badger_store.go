package kvstore

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"strings"
	"sync"
)

func NewBadgerStringStore(db *badger.DB) StringStore {
	return &badgerStringStore{
		mu:     &sync.Mutex{},
		db:     db,
		prefix: "",
	}
}

type badgerStringStore struct {
	mu     *sync.Mutex
	db     *badger.DB
	prefix string
}

func (b *badgerStringStore) Append(prefix string) StringStore {
	if strings.Contains(prefix, "/") {
		panic("prefix must not contain '/'")
	}
	return &badgerStringStore{
		mu:     b.mu,
		db:     b.db,
		prefix: fmt.Sprintf("%s/%s", b.prefix, prefix),
	}
}

func (b *badgerStringStore) Update(update func(txn Txn[string, string]) any) any {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out any
	_ = b.db.Update(func(txn *badger.Txn) error {
		out = update(&badgerStringTxn{txn: txn})
		return nil
	})
	return out
}

type badgerStringTxn struct {
	txn *badger.Txn
}

func (b *badgerStringTxn) Get(k string) (v string, ok bool) {
	i, err := b.txn.Get([]byte(k))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", false
	}
	if err != nil {
		panic(err)
	}
	err = i.Value(func(val []byte) error {
		v = string(val)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return v, true
}

func (b *badgerStringTxn) Set(k string, v string) {
	err := b.txn.Set([]byte(k), []byte(v))
	if err != nil {
		panic(err)
	}
}

func (b *badgerStringTxn) Del(k string) {
	err := b.txn.Delete([]byte(k))
	if err != nil {
		panic(err)
	}
}
