package crypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

type Crypt interface {
	Encrypt(plaintext []byte) (ciphertext []byte, err error)
	Decrypt(ciphertext []byte) (plaintext []byte, err error)
}

func NewCrypt(s string) Crypt {
	if len(s) == 0 {
		fmt.Println("WARNING: no key is used")
		return key(nil)
	}
	hash := sha256.Sum256([]byte(s))
	return key(hash[:])
}

type key []byte

func (k key) Encrypt(plaintext []byte) (ciphertext []byte, err error) {
	if len(k) == 0 {
		return plaintext, nil
	}

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
	if len(k) == 0 {
		return ciphertext, nil
	}

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
