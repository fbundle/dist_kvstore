package kvstore

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
	Txn[K, V]
	Keys() (keys []K)
}
