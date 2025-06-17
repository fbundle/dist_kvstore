package types

type Option[T any] struct {
	isFull bool
	val    T
}

func Some[T any](val T) Option[T] {
	return Option[T]{
		isFull: true,
		val:    val,
	}
}

func None[T any]() Option[T] {
	return Option[T]{
		isFull: false,
	}
}

func Bind[T any, T1 any](o Option[T], f func(val T) Option[T1]) Option[T1] {
	if o.isFull {
		return f(o.val)
	}
	return None[T1]()
}

type Sum[L any, R any] struct {
	val any
}

func ApplySum[T any, L any, R any](sum Sum[L, R], leftFunc func(L) T, rightFunc func(R) T) T {
	switch val := sum.val.(type) {
	case L:
		return leftFunc(val)
	case R:
		return rightFunc(val)
	default:
		panic("wrong type")
	}
}

func MakeSumLeft[L any, R any](left L) Sum[L, R] {
	return Sum[L, R]{val: left}
}

func MakeSumRight[L any, R any](right R) Sum[L, R] {
	return Sum[L, R]{val: right}
}

type Prod[L any, R any] struct {
	Left  L
	Right R
}

func MakeProd[L any, R any](left L, right R) Prod[L, R] {
	return Prod[L, R]{
		Left:  left,
		Right: right,
	}
}
