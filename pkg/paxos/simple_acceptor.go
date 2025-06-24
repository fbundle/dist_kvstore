package paxos

// Proposal - roundId * 4294967296 + nodeId
type Proposal uint64

type LogId uint64

const (
	INITIAL   Proposal = 0
	COMMITTED Proposal = 18446744073709551615
)

// Promise - promise to reject all PREPARE if proposal <= this and all ACCEPT if proposal < this
type Promise[T any] struct {
	Proposal Proposal `json:"proposal"`
	Value    *T       `json:"value"`
}

func zero[T any]() T {
	var v T
	return v
}

type Txn[T any] interface {
	Get(k LogId) (v Promise[T], ok bool)
	Set(k LogId, v Promise[T])
	Del(k LogId)
}

// Log - stable log
type Log[T any] interface {
	Update(update func(txn Txn[T]) any) any
}

type simpleAcceptor[T any] struct {
	log Log[T]
}

func getDefaultLogEntry[T any](txn Txn[T], logId LogId) (p Promise[T]) {
	v, ok := txn.Get(logId)
	if !ok {
		return Promise[T]{
			Proposal: INITIAL,
			Value:    nil,
		}
	}
	return v
}

func (a *simpleAcceptor[T]) get(logId LogId) (Proposal, *T) {
	promise := a.log.Update(func(txn Txn[T]) any {
		return getDefaultLogEntry(txn, logId)
	}).(Promise[T])
	return promise.Proposal, promise.Value
}

func (a *simpleAcceptor[T]) commit(logId LogId, v T) {
	a.log.Update(func(txn Txn[T]) any {
		txn.Set(logId, Promise[T]{
			Proposal: COMMITTED,
			Value:    &v,
		})
		return nil
	})
}

func (a *simpleAcceptor[T]) prepare(logId LogId, proposal Proposal) (Promise[T], bool) {
	r := a.log.Update(func(txn Txn[T]) any {
		p := getDefaultLogEntry(txn, logId)
		if !(p.Proposal < proposal) {
			return [2]any{p, false}
		}
		txn.Set(logId, Promise[T]{
			Proposal: proposal,
			Value:    p.Value,
		})
		return [2]any{p, true}
	}).([2]any)
	promise, ok := r[0].(Promise[T]), r[1].(bool)
	return promise, ok
}

func (a *simpleAcceptor[T]) accept(logId LogId, proposal Proposal, value T) (Promise[T], bool) {
	r := a.log.Update(func(txn Txn[T]) any {
		p := getDefaultLogEntry(txn, logId)
		if !(p.Proposal <= proposal) {
			return [2]any{p, false}
		}
		txn.Set(logId, Promise[T]{
			Proposal: proposal,
			Value:    &value,
		})
		return [2]any{p, true}
	}).([2]any)
	promise, ok := r[0].(Promise[T]), r[1].(bool)
	return promise, ok
}
