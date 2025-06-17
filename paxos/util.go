package paxos

type stateMachineFunc[T any] struct {
	apply func(logId LogId, value T)
}

func StateMachineFunc[T any](apply func(logId LogId, value T)) StateMachine[T] {
	return &stateMachineFunc[T]{apply: apply}
}

func (sm *stateMachineFunc[T]) Apply(logId LogId, value T) {
	sm.apply(logId, value)
}

func (sm *stateMachineFunc[T]) Snapshot() T {
	panic("state_machine_func doesn't implement snapshot")
}
