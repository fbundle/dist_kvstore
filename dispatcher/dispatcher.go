package dispatcher

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
