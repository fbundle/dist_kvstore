package crypt

import (
	"encoding/binary"
	"errors"
	"io"
)

type CryptStream interface {
	EncryptToWriter(plaintext []byte, writer io.Writer) (err error)
	DecryptFromReader(reader io.Reader) (plaintext []byte, err error)
}

func NewStream(c Crypt) CryptStream {
	return cryptStream{crypt: c}
}

type cryptStream struct {
	crypt Crypt
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

func (cs cryptStream) EncryptToWriter(plaintext []byte, writer io.Writer) (err error) {
	ciphertext, err := cs.crypt.Encrypt(plaintext)
	if err != nil {
		return err
	}
	n := uint64(len(ciphertext))
	b := make([]byte, 8, 8+len(ciphertext))
	binary.LittleEndian.PutUint64(b, n)
	b = append(b, ciphertext...)

	return writeExact(writer, b)
}

func (cs cryptStream) DecryptFromReader(reader io.Reader) (plaintext []byte, err error) {
	b, err := readExact(reader, 8)
	if err != nil {
		return nil, err
	}
	n := int(binary.LittleEndian.Uint64(b))
	ciphertext, err := readExact(reader, n)
	if err != nil {
		return nil, err
	}
	plaintext, err = cs.crypt.Decrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
