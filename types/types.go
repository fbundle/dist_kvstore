package types

type Sum[L any, R any] struct {
	Value any
}

func (s Sum[L, R]) Get() any {
	return s.Value
}

func MakeSumLeft[L any, R any](left L) Sum[L, R] {
	return Sum[L, R]{Value: left}
}

func MakeSumRight[L any, R any](right R) Sum[L, R] {
	return Sum[L, R]{Value: right}
}

type Prod[L any, R any] struct {
	Left  L
	Right R
}

func (p Prod[L, R]) Get() (L, R) {
	return p.Left, p.Right
}

func MakeProd[L any, R any](left L, right R) Prod[L, R] {
	return Prod[L, R]{
		Left:  left,
		Right: right,
	}
}
