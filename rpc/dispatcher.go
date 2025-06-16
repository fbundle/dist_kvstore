package rpc

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type Dispatcher interface {
	Register(name string, h any) Dispatcher
	Handle(input []byte) (output []byte, err error)
}

func NewDispatcher() Dispatcher {
	return &dispatcher{
		handlerMap: make(map[string]handler),
	}
}

type message struct {
	Cmd  string `json:"cmd"`
	Body []byte `json:"body"`
}

type handler struct {
	handlerFunc reflect.Value
	argType     reflect.Type
}

type dispatcher struct {
	handlerMap map[string]handler
}

func (d *dispatcher) Register(name string, h any) Dispatcher {
	handlerFunc := reflect.ValueOf(h)
	handlerFuncType := handlerFunc.Type()
	if handlerFuncType.Kind() != reflect.Func || handlerFuncType.NumIn() != 1 || handlerFuncType.NumOut() != 1 {
		panic("handler must be of form func(*SomeRequest) *SomeResponse")
	}
	argType := handlerFuncType.In(0)
	if argType.Kind() != reflect.Ptr || handlerFuncType.Out(0).Kind() != reflect.Ptr {
		panic("handler arguments and return type must be pointers")
	}
	d.handlerMap[name] = handler{
		handlerFunc: handlerFunc,
		argType:     argType,
	}
	return d
}

func (d *dispatcher) Handle(input []byte) (output []byte, err error) {
	msg := message{}
	if err := json.Unmarshal(input, &msg); err != nil {
		return nil, err
	}

	h, ok := d.handlerMap[msg.Cmd]
	if !ok {
		return nil, fmt.Errorf("command not found")
	}

	argPtr := reflect.New(h.argType.Elem()).Interface()
	if err := json.Unmarshal(msg.Body, argPtr); err != nil {
		return nil, err
	}

	out := h.handlerFunc.Call([]reflect.Value{reflect.ValueOf(argPtr)})[0].Interface()

	output, err = json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return output, nil
}
