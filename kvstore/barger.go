package kvstore

import (
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"sync"
)

func NewBargerStore[K comparable, V any](db *badger.DB) Store[K, V] {

	return &badgerStore[K, V]{
		mu: sync.Mutex{},
		db: db,
	}
}

type badgerStore[K comparable, V any] struct {
	mu sync.Mutex
	db *badger.DB
}

type badgerTxn[K comparable, V any] struct {
	txn *badger.Txn
}

func (b *badgerTxn[K, V]) Get(k K) (v V, ok bool) {
	kb, err := json.Marshal(k)
	if err != nil {
		panic(err)
	}
	i, err := b.txn.Get(kb)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return v, false
	}
	if err != nil {
		panic(err)
	}
	err = i.Value(func(val []byte) error {
		return json.Unmarshal(val, &v)
	})
	if err != nil {
		panic(err)
	}
	return v, true
}

func (b *badgerTxn[K, V]) Set(k K, v V) {
	kb, err := json.Marshal(k)
	if err != nil {
		panic(err)
	}
	vb, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	err = b.txn.Set(kb, vb)
	if err != nil {
		panic(err)
	}
}

func (b *badgerTxn[K, V]) Del(k K) {
	kb, err := json.Marshal(k)
	if err != nil {
		panic(err)
	}
	err = b.txn.Delete(kb)
	if err != nil {
		panic(err)
	}
}

func (b *badgerStore[K, V]) Update(update func(txn Txn[K, V]) any) any {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out any
	_ = b.db.Update(func(txn *badger.Txn) error {
		out = update(&badgerTxn[K, V]{txn: txn})
		return nil
	})
	return out
}

func (b *badgerStore[K, V]) Close() {
	err := b.db.Close()
	if err != nil {
		panic(err)
	}
}
