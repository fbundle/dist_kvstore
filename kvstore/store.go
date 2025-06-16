package kvstore

import "sync"

// Store - supposed to be threadsafe for each key and persistent
type Store[K comparable, V any] interface {
	Update(update func(txn Txn[K, V]) any) any
}

type Txn[K comparable, V any] interface {
	Get(k K) (v V, ok bool)
	Set(k K, v V)
	Del(k K)
}

type MemStore[K comparable, V any] interface {
	Store[K, V]
	Keys() (keys []K)
}

func NewMemStore[K comparable, V any]() MemStore[K, V] {
	return &memStore[K, V]{
		mu:    sync.RWMutex{},
		store: make(map[K]V),
	}
}

type memStore[K comparable, V any] struct {
	mu    sync.RWMutex
	store map[K]V
}

func (m *memStore[K, V]) Keys() (keys []K) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys = make([]K, 0, len(m.store))
	for k := range m.store {
		keys = append(keys, k)
	}
	return keys
}

func (m *memStore[K, V]) Update(update func(txn Txn[K, V]) any) any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return update(m)
}

func (m *memStore[K, V]) Get(k K) (v V, ok bool) {
	v, ok = m.store[k]
	return v, ok
}

func (m *memStore[K, V]) Set(k K, v V) {
	m.store[k] = v
}

func (m *memStore[K, V]) Del(k K) {
	delete(m.store, k)
}
