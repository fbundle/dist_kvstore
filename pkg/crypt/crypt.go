package crypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

func NewKey(s string) Key {
	hash := sha256.Sum256([]byte(s))
	return key(hash[:])
}

type Key interface {
	Encrypt(plaintext []byte) (ciphertext []byte, err error)
	Decrypt(ciphertext []byte) (plaintext []byte, err error)
	EncryptToWriter(plaintext []byte, writer io.Writer) (err error)
	DecryptFromReader(reader io.Reader) (plaintext []byte, err error)
}

type key []byte

func (k key) EncryptToWriter(plaintext []byte, writer io.Writer) (err error) {
	ciphertext, err := k.Encrypt(plaintext)
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

func (k key) DecryptFromReader(reader io.Reader) (plaintext []byte, err error) {
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
	plaintext, err = k.Decrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}

func (k key) Encrypt(plaintext []byte) (ciphertext []byte, err error) {
	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}

	// GCM mode provides authenticated encryption
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Seal appends the encrypted data to the nonce
	ciphertext = aesGCM.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

func (k key) Decrypt(ciphertext []byte) (plaintext []byte, err error) {
	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := aesGCM.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err = aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
