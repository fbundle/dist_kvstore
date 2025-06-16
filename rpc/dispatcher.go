package rpc

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type handler struct {
	handlerFunc reflect.Value
	argType     reflect.Type
}

type Dispatcher struct {
	handlerMap map[string]handler
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		handlerMap: make(map[string]handler),
	}
}

type Message struct {
	Name string `json:"name"`
	Body []byte `json:"body"`
}

func (d *Dispatcher) Handle(input []byte) (output []byte, err error) {
	msg := Message{}
	if err := json.Unmarshal(input, &msg); err != nil {
		return nil, err
	}

	h, ok := d.handlerMap[msg.Name]
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

func (d *Dispatcher) Append(name string, h any) *Dispatcher {
	hv := reflect.ValueOf(h)
	ht := hv.Type()
	if ht.Kind() != reflect.Func || ht.NumIn() != 1 || ht.NumOut() != 1 {
		panic("handler must be of form func(*SomeRequest) *SomeResponse")
	}
	arg := ht.In(0)
	if arg.Kind() != reflect.Ptr || ht.Out(0).Kind() != reflect.Ptr {
		panic("handler arguments and return type must be pointers")
	}
	d.handlerMap[name] = handler{
		handlerFunc: hv,
		argType:     arg,
	}
	return d
}
