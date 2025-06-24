package local_store

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

func (ss *badgerStringStore) Append(prefix string) StringStore {
	if strings.Contains(prefix, "/") {
		panic("prefix must not contain '/'")
	}
	return &badgerStringStore{
		mu:     ss.mu,
		db:     ss.db,
		prefix: fmt.Sprintf("%s/%s", ss.prefix, prefix),
	}
}

func (ss *badgerStringStore) Update(update func(txn Txn[string, string]) any) any {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var out any
	_ = ss.db.Update(func(txn *badger.Txn) error {
		out = update(&badgerStringTxn{
			prefix: ss.prefix,
			txn:    txn,
		})
		return nil
	})
	return out
}

type badgerStringTxn struct {
	prefix string
	txn    *badger.Txn
}

func (t *badgerStringTxn) Get(k string) (v string, ok bool) {
	k = fmt.Sprintf("%s/%s", t.prefix, k)

	i, err := t.txn.Get([]byte(k))
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

func (t *badgerStringTxn) Set(k string, v string) {
	k = fmt.Sprintf("%s/%s", t.prefix, k)

	err := t.txn.Set([]byte(k), []byte(v))
	if err != nil {
		panic(err)
	}
}

func (t *badgerStringTxn) Del(k string) {
	k = fmt.Sprintf("%s/%s", t.prefix, k)

	err := t.txn.Delete([]byte(k))
	if err != nil {
		panic(err)
	}
}
