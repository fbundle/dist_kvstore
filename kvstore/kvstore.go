package kvstore

import "sync"

type Store[K comparable, V any] interface {
	Get(K) (V, bool)
	Set(K, V)
	Del(K)
	Update(k K, update func(V) V)
}

func zero[T any]() T {
	var v T
	return v
}

type memStore[K comparable, V any] struct {
	mu    sync.Mutex
	store map[K]V
}

func (m *memStore[K, V]) Get(k K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store[k]
	return v, ok
}

func (m *memStore[K, V]) Update(k K, update func(V) V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store[k]
	if !ok {
		v = zero[V]()
	}
	m.store[k] = update(v)
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

func NewMemStore[K comparable, V any]() Store[K, V] {
	return &memStore[K, V]{
		mu:    sync.Mutex{},
		store: make(map[K]V),
	}
}
