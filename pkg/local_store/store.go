package local_store

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

type StringStore interface {
	Store[string, string]
	Append(prefix string) StringStore
}

func MakeStoreFromStringStore[K comparable, V any](ss StringStore) Store[K, V] {
	return &storeKV[K, V]{ss: ss}
}
