package subscriber

import (
	"sync"
)

type SubscriberPool[T any] interface {
	Subscribe(T)func()
	Iterate(func(T) bool)
}

type subscriberPool[T any] struct {
	mu sync.RWMutex
	count uint
	pool map[uint]T
}

func (p *subscriberPool[T]) Subscribe(t T) func() {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := p.count
	p.count++
	p.pool[count] = t
	return func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(p.pool, count)
	}
}

func (p *subscriberPool[T]) Iterate(f func(t T) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, t := range p.pool {
		ok := f(t)
		if !ok {
			break
		}
	}
}