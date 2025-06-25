package rpc

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type Dispatcher interface {
	Register(cmd string, h any) Dispatcher
	Handle(input []byte) (output []byte, err error)
}

func NewDispatcher() Dispatcher {
	return dispatcher(make(map[string]handler))
}

type handler struct {
	handlerFunc reflect.Value
	argType     reflect.Type
}
type dispatcher map[string]handler

func (d dispatcher) Register(cmd string, h any) Dispatcher {
	handlerFunc := reflect.ValueOf(h)
	handlerFuncType := handlerFunc.Type()
	if handlerFuncType.Kind() != reflect.Func || handlerFuncType.NumIn() != 1 || handlerFuncType.NumOut() != 1 {
		panic("handler must be of form func(*SomeRequest) *SomeResponse")
	}
	argType := handlerFuncType.In(0)
	retType := handlerFuncType.Out(0)
	if argType.Kind() != reflect.Ptr || retType.Kind() != reflect.Ptr {
		panic("handler arguments and return type must be pointers")
	}
	d[cmd] = handler{
		handlerFunc: handlerFunc,
		argType:     argType,
	}
	return d
}

type message struct {
	Cmd  string `json:"cmd"`
	Body []byte `json:"body"`
}

func (d dispatcher) Handle(input []byte) (output []byte, err error) {
	msg := message{}
	if err = json.Unmarshal(input, &msg); err != nil {
		return nil, err
	}

	h, ok := d[msg.Cmd]
	if !ok {
		return nil, fmt.Errorf("command not found")
	}

	argPtr := reflect.New(h.argType.Elem()).Interface()
	if err = json.Unmarshal(msg.Body, argPtr); err != nil {
		return nil, err
	}

	out := h.handlerFunc.Call([]reflect.Value{reflect.ValueOf(argPtr)})[0].Interface()

	output, err = json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return output, nil
}

type TransportFunc func([]byte) ([]byte, error)

func zeroPtr[T any]() *T {
	var v T
	return &v
}

func RPC[Req any, Res any](transport TransportFunc, cmd string, req *Req) (res *Res, err error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	msg := message{
		Cmd:  cmd,
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
	if err = json.Unmarshal(b, res); err != nil {
		return nil, err
	}
	return res, nil
}
