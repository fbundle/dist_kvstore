package kvstore

type Txn[K comparable, V any] interface {
	Get(k K) (v V, ok bool)
	Set(k K, v V)
	Del(k K)
}

// Store - threadsafe stable store
type Store[K comparable, V any] interface {
	Update(update func(txn Txn[K, V]) any) any
}

type MemStore[K comparable, V any] interface {
	Store[K, V]
	Keys() []K
}
