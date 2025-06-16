package dispatcher1

import (
	"encoding/json"
	"fmt"
	"reflect"
)

type Dispatcher struct {
	handlers map[string]handlerEntry
}

type handlerEntry struct {
	handlerFunc reflect.Value
	argType     reflect.Type
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{handlers: make(map[string]handlerEntry)}
}

func (d *Dispatcher) AddHandler(name string, handler any) {
	hv := reflect.ValueOf(handler)
	ht := hv.Type()
	if ht.Kind() != reflect.Func || ht.NumIn() != 1 || ht.NumOut() != 1 {
		panic("handler must be of form func(*SomeRequest) *SomeResponse")
	}
	arg := ht.In(0)
	if arg.Kind() != reflect.Ptr || ht.Out(0).Kind() != reflect.Ptr {
		panic("handler arguments and return type must be pointers")
	}
	d.handlers[name] = handlerEntry{
		handlerFunc: hv,
		argType:     arg,
	}
}

type Message struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

func (d *Dispatcher) Handle(data []byte) []byte {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return marshalError(err)
	}

	entry, ok := d.handlers[msg.Type]
	if !ok {
		return marshalError(fmt.Errorf("unknown message type: %s", msg.Type))
	}

	argPtr := reflect.New(entry.argType.Elem()).Interface()
	if err := json.Unmarshal(msg.Body, argPtr); err != nil {
		return marshalError(err)
	}

	res := entry.handlerFunc.Call([]reflect.Value{reflect.ValueOf(argPtr)})[0].Interface()

	output, err := json.Marshal(res)
	if err != nil {
		return marshalError(err)
	}
	return output
}

func marshalError(err error) []byte {
	msg := map[string]string{"error": err.Error()}
	b, _ := json.Marshal(msg)
	return b
}

type TransportFunc func(data []byte) ([]byte, error)

func RemoteCall[Req any, Res any](name string, req Req, transport TransportFunc) (Res, error) {
	var zero Res

	// Type name from struct
	t := reflect.TypeOf(req)
	if t.Kind() != reflect.Ptr {
		return zero, fmt.Errorf("req must be a pointer")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return zero, err
	}
	msg := Message{
		Type: name,
		Body: body,
	}
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return zero, err
	}

	// 🔌 Send over the transport
	resBytes, err := transport(msgBytes)
	if err != nil {
		return zero, err
	}

	// Decode response
	var res Res
	if err := json.Unmarshal(resBytes, &res); err != nil {
		// Try to parse error message
		var errMsg map[string]string
		if json.Unmarshal(resBytes, &errMsg) == nil {
			if msg, ok := errMsg["error"]; ok {
				return zero, fmt.Errorf(msg)
			}
		}
		return zero, err
	}
	return res, nil
}
