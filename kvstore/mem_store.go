package kvstore

import "sync"

func NewMemStore[K comparable, V any]() Store[K, V] {
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

func (m *memStore[K, V]) Get(k K) (v V, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok = m.store[k]
	return v, ok
}

func (m *memStore[K, V]) Set(k K, v V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[k] = v
}

func (m *memStore[K, V]) Del(k K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, k)
}
