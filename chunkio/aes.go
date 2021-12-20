// Copyright 2021 The tapedb authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkio

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
)

type AESCrypter struct {
	gcm          cipher.AEAD
	key          []byte
	initialNonce []byte
	nonce        [sha256.Size]byte
}

func NewAESCrypter(key, initialNonce []byte) (*AESCrypter, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	crypter := &AESCrypter{gcm: gcm, key: key, initialNonce: initialNonce}
	crypter.SetInitialNonce(initialNonce)
	return crypter, nil
}

func (c *AESCrypter) Key() []byte {
	if c == nil {
		return nil
	}
	return c.key
}

func (c *AESCrypter) SetInitialNonce(value []byte) {
	for index := range c.nonce {
		c.nonce[index] = 0x00
	}
	copy(c.nonce[sha256.Size-c.gcm.NonceSize():], value)
}

func (c *AESCrypter) InitialNonce() []byte {
	return c.initialNonce
}

func (c *AESCrypter) NonceSize() int {
	return c.gcm.NonceSize()
}

func (c *AESCrypter) Encrypt(plaintext []byte) ([]byte, error) {
	ciphertext := c.gcm.Seal(nil, c.currenctNonce(), plaintext, nil)
	c.advanceNonce()
	return ciphertext, nil
}

func (c *AESCrypter) Decrypt(ciphertext []byte) ([]byte, error) {
	plaintext, err := c.gcm.Open(nil, c.currenctNonce(), ciphertext, nil)
	if err != nil {
		return nil, err
	}
	c.advanceNonce()
	return plaintext, nil
}

func (c *AESCrypter) currenctNonce() []byte {
	return c.nonce[sha256.Size-c.gcm.NonceSize():]
}

func (c *AESCrypter) advanceNonce() {
	c.nonce = sha256.Sum256(c.nonce[:])
}

type AESReader struct {
	r Reader
	c *AESCrypter
}

func NewAESReader(r Reader, c *AESCrypter) *AESReader {
	return &AESReader{r: r, c: c}
}

func (r *AESReader) Read() ([]byte, error) {
	chunk, err := r.r.Read()
	if err != nil {
		return nil, err
	}
	return r.c.Decrypt(chunk)
}

type AESWriter struct {
	w Writer
	c *AESCrypter
}

func NewAESWriter(w Writer, c *AESCrypter) *AESWriter {
	return &AESWriter{w: w, c: c}
}

func (w *AESWriter) Write(chunk []byte) error {
	ciphertext, err := w.c.Encrypt(chunk)
	if err != nil {
		return err
	}
	return w.w.Write(ciphertext)
}

type AESReadWriter struct {
	rw ReadWriter
	c  *AESCrypter
}

func NewAESReadWriter(rw ReadWriter, c *AESCrypter) *AESReadWriter {
	return &AESReadWriter{rw: rw, c: c}
}

func (rw *AESReadWriter) Read() ([]byte, error) {
	chunk, err := rw.rw.Read()
	if err != nil {
		return nil, err
	}
	return rw.c.Decrypt(chunk)
}

func (rw *AESReadWriter) Write(chunk []byte) error {
	ciphertext, err := rw.c.Encrypt(chunk)
	if err != nil {
		return err
	}
	return rw.rw.Write(ciphertext)
}
