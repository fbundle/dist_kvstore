package rpc

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/fbundle/go_util/pkg/crypt"
)

type IO interface {
	Write(b []byte, writer io.Writer) (err error)
	Read(reader io.Reader) (b []byte, err error)
}

func NewCryptIO(crypt crypt.Crypt) IO {
	return &cryptIO{
		crypt: crypt,
	}
}

func writeExact(writer io.Writer, b []byte) error {
	m, err := writer.Write(b)
	if err != nil {
		return err
	}
	if m != len(b) {
		return errors.New("cannot write buffer")
	}
	return nil
}

func readExact(reader io.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	m, err := reader.Read(b)
	if err != nil {
		return nil, err
	}
	if m != n {
		return nil, errors.New("cannot read buffer")
	}
	return b, nil
}

type cryptIO struct {
	crypt crypt.Crypt
}

func (c *cryptIO) Write(plaintext []byte, writer io.Writer) (err error) {
	ciphertext, err := c.crypt.Encrypt(plaintext)
	if err != nil {
		return err
	}
	n := uint64(len(ciphertext))
	b := make([]byte, 8, 8+len(ciphertext))
	binary.LittleEndian.PutUint64(b, n)
	b = append(b, ciphertext...)

	return writeExact(writer, b)
}

func (c *cryptIO) Read(reader io.Reader) (plaintext []byte, err error) {
	b, err := readExact(reader, 8)
	if err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint64(b))
	ciphertext, err := readExact(reader, n)
	if err != nil {
		return nil, err
	}
	plaintext, err = c.crypt.Decrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
