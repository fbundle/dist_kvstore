package local_store

import "sync"

func NewMemStore[K comparable, V any]() MemStore[K, V] {
	return &memStore[K, V]{
		mu:    sync.Mutex{},
		store: make(map[K]V),
	}
}

type memStore[K comparable, V any] struct {
	mu    sync.Mutex
	store map[K]V
}

func (m *memStore[K, V]) Update(update func(txn Txn[K, V]) any) any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return update(m)
}

func (m *memStore[K, V]) Keys() (keys []K) {
	keys = make([]K, 0, len(m.store))
	for k := range m.store {
		keys = append(keys, k)
	}
	return keys
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
