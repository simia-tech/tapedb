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

package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const BlockSize = 4096

type BlockWriter[W io.Writer] struct {
	w            W
	gcm          cipher.AEAD
	nonce        []byte
	nonceWritten bool
	buffer       bytes.Buffer
}

func NewBlockWriter[W io.Writer](w W, key []byte, nonceFn NonceFunc) (*BlockWriter[W], error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	return &BlockWriter[W]{
		w:            w,
		gcm:          gcm,
		nonce:        nonceFn(gcm.NonceSize()),
		nonceWritten: false,
	}, nil
}

func (w *BlockWriter[W]) Write(data []byte) (int, error) {
	if !w.nonceWritten {
		if _, err := w.w.Write(w.nonce[:]); err != nil {
			return 0, err
		}
		w.nonceWritten = true
	}

	if n, err := w.buffer.Write(data); err != nil {
		return n, err
	}

	if w.buffer.Len() >= BlockSize {
		plainText, rest := w.buffer.Bytes()[:BlockSize], w.buffer.Bytes()[BlockSize:]

		cipherText := w.gcm.Seal(nil, w.nonce, plainText, nil)

		size := [2]byte{}
		binary.LittleEndian.PutUint16(size[:], uint16(len(cipherText)))

		if _, err := w.w.Write(size[:]); err != nil {
			return 0, err
		}

		if _, err := w.w.Write(cipherText); err != nil {
			return 0, err
		}

		w.advanceNonce()

		w.buffer.Reset()
		if _, err := w.buffer.Write(rest); err != nil {
			return 0, err
		}
	}

	return len(data), nil
}

func (w *BlockWriter[W]) Close() error {
	if w.buffer.Len() > 0 {
		cipherText := w.gcm.Seal(nil, w.nonce, w.buffer.Bytes(), nil)

		size := [2]byte{}
		binary.LittleEndian.PutUint16(size[:], uint16(len(cipherText)))

		if _, err := w.w.Write(size[:]); err != nil {
			return err
		}

		if _, err := w.w.Write(cipherText); err != nil {
			return err
		}

		w.buffer.Reset()
	}
	return nil
}

func (w *BlockWriter[W]) advanceNonce() {
	n := sha256.Sum256(w.nonce)
	w.nonce = n[sha256.Size-w.gcm.NonceSize():]
}

type BlockReader[R io.Reader] struct {
	r         R
	gcm       cipher.AEAD
	nonce     []byte
	nonceRead bool
	buffer    io.Reader
}

func NewBlockReader[R io.Reader](r R, key []byte) (*BlockReader[R], error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	return &BlockReader[R]{
		r:         r,
		gcm:       gcm,
		nonceRead: false,
		buffer:    bytes.NewReader([]byte{}),
	}, nil
}

func (r *BlockReader[R]) Read(data []byte) (int, error) {
	if !r.nonceRead {
		n := make([]byte, r.gcm.NonceSize())
		if _, err := io.ReadFull(r.r, n); err != nil {
			return 0, fmt.Errorf("read nonce: %w", err)
		}
		r.nonce = n
		r.nonceRead = true
	}

	br, err := r.readBlock()
	if err == nil {
		r.buffer = io.MultiReader(r.buffer, br)
	} else {
		if !errors.Is(err, io.EOF) {
			return 0, err
		}
	}

	return r.buffer.Read(data)
}

func (r *BlockReader[R]) readBlock() (io.Reader, error) {
	size := [2]byte{}
	if _, err := r.r.Read(size[:]); err != nil {
		return nil, err
	}
	blockSize := binary.LittleEndian.Uint16(size[:])

	cipherText := make([]byte, blockSize)
	if _, err := io.ReadFull(r.r, cipherText); err != nil {
		return nil, err
	}

	plainText, err := r.gcm.Open(nil, r.nonce, cipherText, nil)
	if err != nil {
		return nil, err
	}
	r.advanceNonce()

	return bytes.NewReader(plainText), nil
}

func (r *BlockReader[W]) advanceNonce() {
	n := sha256.Sum256(r.nonce)
	r.nonce = n[sha256.Size-r.gcm.NonceSize():]
}
