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
	"fmt"
	"io"
	"strings"

	tapeio "github.com/simia-tech/tapedb/v2/io"
)

type LogWriter[W tapeio.LogWriter] struct {
	w       W
	gcm     cipher.AEAD
	nonceFn NonceFunc
}

func WrapLogWriter(w tapeio.LogWriter, key []byte, nonceFn NonceFunc) (tapeio.LogWriter, error) {
	if w == nil || len(key) == 0 {
		return w, nil
	}
	return NewLogWriter(w, key, nonceFn)
}

func NewLogWriter[W tapeio.LogWriter](w W, key []byte, nonceFn NonceFunc) (*LogWriter[W], error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	return &LogWriter[W]{
		w:       w,
		gcm:     gcm,
		nonceFn: nonceFn,
	}, nil
}

func (w *LogWriter[W]) WriteEntry(et tapeio.LogEntryType, plainText []byte) (int64, error) {
	nonce := w.nonceFn(w.gcm.NonceSize())

	cipherText := w.gcm.Seal(nil, nonce, plainText, nil)

	return w.w.WriteEntry(tapeio.LogEntryTypeAESGCMEncrypted, append(nonce, cipherText...))
}

type LogReader[R tapeio.LogReader] struct {
	r         R
	gcm       cipher.AEAD
	nonceSize int
}

func WrapLogReader(r tapeio.LogReader, key []byte) (tapeio.LogReader, error) {
	if r == nil || len(key) == 0 {
		return r, nil
	}
	return NewLogReader(r, key)
}

func NewLogReader[R tapeio.LogReader](r R, key []byte) (*LogReader[R], error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("new aes cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, fmt.Errorf("new gcm: %w", err)
	}

	return &LogReader[R]{
		r:         r,
		gcm:       gcm,
		nonceSize: gcm.NonceSize(),
	}, nil
}

func (r *LogReader[R]) ReadEntry() (tapeio.LogEntry, error) {
	entry, err := r.r.ReadEntry()
	if err != nil {
		return entry, err
	}

	return &logEntry[R]{r: r, entry: entry}, nil
}

type logEntry[R tapeio.LogReader] struct {
	r     *LogReader[R]
	entry tapeio.LogEntry
}

var _ tapeio.LogEntry = &logEntry[tapeio.LogReader]{}

func (e *logEntry[R]) Type() tapeio.LogEntryType {
	return tapeio.LogEntryTypeBinary
}

func (e *logEntry[R]) Reader() (io.Reader, error) {
	r, err := e.entry.Reader()
	if err != nil {
		return nil, fmt.Errorf("reader: %w", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read all: %w", err)
	}

	nonce, cipherText := data[:e.r.nonceSize], data[e.r.nonceSize:]

	plainText, err := e.r.gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		if strings.HasSuffix(err.Error(), "message authentication failed") {
			return nil, ErrInvalidKey
		}
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return bytes.NewReader(plainText), nil
}
