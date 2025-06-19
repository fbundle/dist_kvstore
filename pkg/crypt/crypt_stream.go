package crypt

import (
	"encoding/binary"
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

func (cs cryptStream) EncryptToWriter(plaintext []byte, writer io.Writer) (err error) {
	ciphertext, err := cs.crypt.Encrypt(plaintext)
	if err != nil {
		return err
	}
	n := uint64(len(ciphertext))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, n)

	_, err = writer.Write(append(b, ciphertext...))
	if err != nil {
		return err
	}
	return nil
}

func (cs cryptStream) DecryptFromReader(reader io.Reader) (plaintext []byte, err error) {
	b := make([]byte, 8)
	_, err = reader.Read(b)
	if err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint64(b)
	ciphertext := make([]byte, n)
	_, err = reader.Read(ciphertext)
	if err != nil {
		return nil, err
	}
	plaintext, err = cs.crypt.Decrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
