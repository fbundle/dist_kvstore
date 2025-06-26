package rpc

import "io"

type IO interface {
	Write(b []byte, writer io.Writer) (err error)
	Read(reader io.Reader) (b []byte, err error)
}


