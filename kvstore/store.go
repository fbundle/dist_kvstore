package kvstore

type Txn[K comparable, V any] interface {
	Get(k K) (v V, ok bool)
	Set(k K, v V)
	Del(k K)
}

// StableStore - threadsafe stable store
type StableStore[K comparable, V any] interface {
	Update(update func(txn Txn[K, V]) any) any
}

// Store - threadsafe memory store
type Store[K comparable, V any] interface {
	Txn[K, V]
	Keys() (keys []K)
}
