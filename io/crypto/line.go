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
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"io"
)

type LineWriter[W io.Writer] struct {
	w       W
	gcm     cipher.AEAD
	nonceFn NonceFunc
	buffer  bytes.Buffer
}

func NewLineWriter[W io.Writer](w W, key []byte, nonceFn NonceFunc) (*LineWriter[W], error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	return &LineWriter[W]{
		w:       w,
		gcm:     gcm,
		nonceFn: nonceFn,
	}, nil
}

func (w *LineWriter[W]) Write(data []byte) (int, error) {
	if index := bytes.Index(data, []byte("\n")); index >= 0 {
		nonce := w.nonceFn(w.gcm.NonceSize())

		if _, err := io.Copy(&w.buffer, bytes.NewReader(data[:index])); err != nil {
			return 0, err
		}

		cipherText := w.gcm.Seal(nil, nonce, w.buffer.Bytes(), nil)

		wc := base64.NewEncoder(base64.RawStdEncoding, w.w)
		if _, err := io.Copy(wc, bytes.NewReader(nonce)); err != nil {
			return 0, err
		}
		if _, err := io.Copy(wc, bytes.NewReader(cipherText)); err != nil {
			return 0, err
		}
		if err := wc.Close(); err != nil {
			return 0, err
		}
		if _, err := fmt.Fprint(w.w, "\n"); err != nil {
			return 0, err
		}

		w.buffer.Reset()
		if _, err := io.Copy(&w.buffer, bytes.NewReader(data[index+1:])); err != nil {
			return 0, err
		}
	} else {
		if _, err := io.Copy(&w.buffer, bytes.NewReader(data)); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

func (w *LineWriter[W]) Close() error {
	if w.buffer.Len() == 0 {
		return nil
	}

	nonce := w.nonceFn(w.gcm.NonceSize())

	cipherText := w.gcm.Seal(nil, nonce, w.buffer.Bytes(), nil)

	wc := base64.NewEncoder(base64.RawStdEncoding, w.w)
	if _, err := io.Copy(wc, bytes.NewReader(nonce)); err != nil {
		return err
	}
	if _, err := io.Copy(wc, bytes.NewReader(cipherText)); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	if _, err := fmt.Fprint(w.w, "\n"); err != nil {
		return err
	}

	w.buffer.Reset()

	return nil
}

type LineReader[R io.Reader] struct {
	s      *bufio.Scanner
	gcm    cipher.AEAD
	buffer io.Reader
}

func NewLineReader[R io.Reader](r R, key []byte) (*LineReader[R], error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	return &LineReader[R]{
		s:      bufio.NewScanner(r),
		gcm:    gcm,
		buffer: bytes.NewReader([]byte{}),
	}, nil
}

func (r *LineReader[R]) Read(data []byte) (int, error) {
	if r.s.Scan() {
		line := make([]byte, base64.RawStdEncoding.DecodedLen(len(r.s.Bytes())))
		if _, err := base64.RawStdEncoding.Decode(line, r.s.Bytes()); err != nil {
			return 0, err
		}

		nonce, cipherText := line[:r.gcm.NonceSize()], line[r.gcm.NonceSize():]

		plainText, err := r.gcm.Open(nil, nonce, cipherText, nil)
		if err != nil {
			return 0, err
		}

		r.buffer = io.MultiReader(r.buffer, bytes.NewReader(append(plainText, '\n')))
	}

	return r.buffer.Read(data)
}
