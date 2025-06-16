package rpc

import (
	"encoding/json"
)

type TransportFunc func([]byte) ([]byte, error)

func zeroPtr[T any]() *T {
	var v T
	return &v
}

func RPC[Req any, Res any](transport TransportFunc, name string, req *Req) (res *Res, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	msg := Message{
		Name: name,
		Body: body,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	b, err = transport(b)
	if err != nil {
		return nil, err
	}

	res = zeroPtr[Res]()
	if err := json.Unmarshal(b, res); err != nil {
		return nil, err
	}
	return res, nil
}
