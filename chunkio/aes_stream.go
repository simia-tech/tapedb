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
	"bytes"
	"errors"
	"io"
)

const StreamBufferSize = 8192

type AESStreamWriter struct {
	w io.Writer
	c *AESCrypter

	nonceWritten bool
	buffer       [StreamBufferSize]byte
	bufferSize   int
}

func NewAESStreamWriter(w io.Writer, c *AESCrypter) *AESStreamWriter {
	return &AESStreamWriter{w: w, c: c}
}

func (w *AESStreamWriter) Write(data []byte) (int, error) {
	if !w.nonceWritten {
		if _, err := io.Copy(w.w, bytes.NewReader(w.c.InitialNonce())); err != nil {
			return 0, err
		}
		w.nonceWritten = true
	}

	n := len(data)
	if l := StreamBufferSize - w.bufferSize; l < n {
		n = l
	}

	copy(w.buffer[w.bufferSize:], data[:n])
	w.bufferSize += n

	if w.bufferSize == StreamBufferSize {
		if err := w.Flush(); err != nil {
			return n, err
		}
	}

	return n, nil
}

func (w *AESStreamWriter) Flush() error {
	ciphertext, err := w.c.Encrypt(w.buffer[:w.bufferSize])
	if err != nil {
		return err
	}

	if _, err := io.Copy(w.w, bytes.NewReader(ciphertext)); err != nil {
		return err
	}

	w.bufferSize = 0

	return nil
}

type AESStreamReader struct {
	r io.Reader
	c *AESCrypter

	nonceRead  bool
	buffer     [StreamBufferSize]byte
	bufferSize int
	plaintext  []byte
}

func NewAESStreamReader(r io.Reader, c *AESCrypter) *AESStreamReader {
	return &AESStreamReader{r: r, c: c}
}

func (r *AESStreamReader) Read(data []byte) (int, error) {
	if !r.nonceRead {
		nonce := make([]byte, r.c.NonceSize())
		if _, err := io.ReadFull(r.r, nonce); err != nil {
			return 0, err
		}
		r.c.SetInitialNonce(nonce)
		r.nonceRead = true
	}

	l := len(r.plaintext)

	if l == 0 && r.bufferSize == 0 {
		m, err := io.ReadFull(r.r, r.buffer[:])
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, err
		}
		r.bufferSize = m
	}

	switch {
	case l == 0 && r.bufferSize == 0:
		return 0, io.EOF

	case l == 0:
		plaintext, err := r.c.Decrypt(r.buffer[:r.bufferSize])
		if err != nil {
			return 0, err
		}
		r.plaintext = plaintext
		r.bufferSize = 0

		l = len(r.plaintext)
		fallthrough

	case l > 0:
		n := len(data)
		if l < n {
			n = l
		}
		copy(data, r.plaintext[:n])
		r.plaintext = r.plaintext[n:]
		return n, nil

	default:
		return 0, io.EOF
	}
}

type AESStreamReadCloser struct {
	r *AESStreamReader
	c io.Closer
}

func NewAESStreamReadCloser(rc io.ReadCloser, c *AESCrypter) *AESStreamReadCloser {
	return &AESStreamReadCloser{r: NewAESStreamReader(rc, c), c: rc}
}

func (r *AESStreamReadCloser) Read(data []byte) (int, error) {
	return r.r.Read(data)
}

func (r *AESStreamReadCloser) Close() error {
	return r.c.Close()
}
