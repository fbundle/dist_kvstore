package kvstore

import "sync"

type Store[K comparable, V any] interface {
	Update(k K, update func(V) V)
}

func zero[T any]() T {
	var v T
	return v
}

// TODO - implement other persistent stores

type memStore[K comparable, V any] struct {
	mu    sync.Mutex
	store map[K]V
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

func NewMemStore[K comparable, V any]() Store[K, V] {
	return &memStore[K, V]{
		mu:    sync.Mutex{},
		store: make(map[K]V),
	}
}
