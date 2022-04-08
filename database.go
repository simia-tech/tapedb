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

package tapedb

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"sync"

	"github.com/simia-tech/tapedb/v2/chunkio"
)

var RandomNonce = func() []byte {
	nonce := [12]byte{}
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		panic(err)
	}
	return nonce[:]
}

type Database struct {
	header        Header
	state         State
	stateMutex    sync.RWMutex
	key           []byte
	changesChunkW chunkio.Writer
	changesCount  int
}

func (m *Model) CreateDatabase(w io.Writer, opts ...CreateOption) (*Database, error) {
	options := defaultCreateOptions
	for _, opt := range opts {
		opt(&options)
	}

	header := options.headerFunc()

	key, err := keyFor(options.keyFunc, header)
	if err != nil {
		return nil, err
	}

	crypter, err := chunkCrypter(key, RandomNonce())
	if err != nil {
		return nil, err
	}

	base, err := m.newBase()
	if err != nil {
		return nil, err
	}

	if err := writeHeaderAndBase(w, crypter, header, base); err != nil {
		return nil, err
	}

	db := &Database{
		header:       header,
		key:          key,
		changesCount: 0,
	}

	state, err := m.newState(base, db.stateMutex.RLocker())
	if err != nil {
		return nil, err
	}
	db.state = state

	db.changesChunkW = chunkWriter(w, crypter)

	return db, nil
}

func (m *Model) OpenDatabase(rw io.ReadWriter, opts ...OpenOption) (*Database, error) {
	options := defaultOpenOptions
	for _, opt := range opts {
		opt(&options)
	}

	header, base, crypter, r, err := m.readHeaderAndBase(rw, options.keyFunc)
	if err != nil {
		return nil, err
	}

	db := &Database{
		header:        header,
		key:           crypter.Key(),
		changesChunkW: chunkWriter(rw, crypter),
	}

	state, changesCount, err := m.readState(base, db.stateMutex.RLocker(), chunkReader(r, crypter))
	if err != nil {
		return nil, err
	}
	db.state = state
	db.changesCount = changesCount

	return db, nil
}

func (m *Model) SpliceDatabase(w io.Writer, r io.Reader, opts ...SpliceOption) ([]string, error) {
	options := defaultSpliceOptions
	for _, opt := range opts {
		opt(&options)
	}

	header, base, crypter, r, err := m.readHeaderAndBase(r, options.sourceKeyFunc)
	if err != nil {
		return nil, err
	}

	chunkR := chunkio.Reader(nil)
	if r != nil {
		chunkR = chunkReader(r, crypter)
	}

	targetKey, err := keyFor(options.targetKeyFunc, header)
	if err != nil {
		return nil, err
	}

	newCrypter, err := chunkCrypter(targetKey, RandomNonce())
	if err != nil {
		return nil, err
	}

	changeIndex := 0
	headerAndBaseWritten := false
	chunkW := chunkio.Writer(nil)
	payloadIDs := []string{}
	_, err = m.readChanges(chunkR, func(change Change) (bool, error) {
		switch {
		case changeIndex < options.consumeChanges:
			if err := base.Apply(change); err != nil {
				return false, err
			}
		case !headerAndBaseWritten:
			payloadIDs = appendPayloadIDs(payloadIDs, base)

			if err := writeHeaderAndBase(w, newCrypter, header, base); err != nil {
				return false, err
			}

			chunkW = chunkWriter(w, newCrypter)

			headerAndBaseWritten = true

			fallthrough
		default:
			payloadIDs = appendPayloadIDs(payloadIDs, change)

			if err := writeChange(chunkW, change); err != nil {
				return false, err
			}
		}
		changeIndex++
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if !headerAndBaseWritten {
		payloadIDs = appendPayloadIDs(payloadIDs, base)

		if err := writeHeaderAndBase(w, newCrypter, header, base); err != nil {
			return nil, err
		}
	}

	return payloadIDs, nil
}

func (m *Model) readHeaderAndBase(r io.Reader, keyFunc KeyFunc) (Header, Base, *chunkio.AESCrypter, io.Reader, error) {
	header, r, err := ReadHeader(r)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, nil, nil, fmt.Errorf("read header: %w", err)
	}

	key, err := keyFor(keyFunc, header)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	crypter, err := chunkCrypter(key, header.GetBytes(HeaderNonce, []byte{}))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	base, err := m.newBase()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	if r == nil {
		return header, base, crypter, nil, nil
	}

	if crypter == nil {
		jr := json.NewDecoder(r)
		if err := jr.Decode(base); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("read json base: %w", err)
		}
		r = io.MultiReader(jr.Buffered(), r)

	} else {
		br := bufio.NewReader(r)
		ciphertext, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, textproto.NewReader(br).DotReader()))
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("read base64 base: %w", err)
		}

		plaintext, err := crypter.Decrypt(ciphertext)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("decrypt base: %w", err)
		}

		if err := json.Unmarshal(plaintext, base); err != nil {
			return nil, nil, nil, nil, fmt.Errorf("decode json base: %w", err)
		}

		r = br
	}

	return header, base, crypter, r, nil
}

func (m *Model) readState(base Base, readLocker sync.Locker, r chunkio.Reader) (State, int, error) {
	state, err := m.newState(base, readLocker)
	if err != nil {
		return nil, 0, err
	}

	if r == nil {
		return state, 0, nil
	}

	changesCount, err := m.readChanges(r, func(change Change) (bool, error) {
		if err := state.Apply(change); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return state, changesCount, fmt.Errorf("read changes: %w", err)
	}

	return state, changesCount, nil
}

func (m *Model) readChanges(r chunkio.Reader, fn func(Change) (bool, error)) (int, error) {
	if r == nil {
		return 0, nil
	}

	count := 0
	chunk, err := []byte(nil), error(nil)
	for err == nil {
		chunk, err = r.Read()
		if err != nil {
			break
		}

		parts := bytes.SplitN(chunk, []byte(" "), 2)
		if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
			continue
		}

		change, err := m.newChange(string(parts[0]))
		if err != nil {
			return count, err
		}

		if err := json.Unmarshal(parts[1], change); err != nil {
			return count, fmt.Errorf("unmarshal change: %w", err)
		}

		ok, err := fn(change)
		if err != nil {
			return count, err
		}
		if !ok {
			break
		}

		count++
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return count, err
	}
	return count, nil
}

func (db *Database) Header() Header {
	return db.header
}

func (db *Database) State() State {
	return db.state
}

func (db *Database) ChangesCount() int {
	return db.changesCount
}

func (db *Database) Apply(change Change) error {
	db.stateMutex.Lock()
	defer db.stateMutex.Unlock()

	if err := db.state.Apply(change); err != nil {
		return err
	}

	if err := writeChange(db.changesChunkW, change); err != nil {
		return err
	}

	db.changesCount++

	return nil
}

func chunkCrypter(key, initialNonce []byte) (*chunkio.AESCrypter, error) {
	if key == nil {
		return nil, nil
	}
	return chunkio.NewAESCrypter(key, initialNonce)
}

func chunkReader(r io.Reader, c *chunkio.AESCrypter) chunkio.Reader {
	chunkR := chunkio.Reader(chunkio.NewLineReader(r))

	if c == nil {
		return chunkR
	}

	return chunkio.NewAESReader(chunkio.NewBase64Reader(chunkR), c)
}

func chunkWriter(w io.Writer, c *chunkio.AESCrypter) chunkio.Writer {
	chunkW := chunkio.Writer(chunkio.NewLineWriter(w))

	if c == nil {
		return chunkW
	}

	return chunkio.NewAESWriter(chunkio.NewBase64Writer(chunkW), c)
}

func writeHeaderAndBase(w io.Writer, c *chunkio.AESCrypter, header Header, base Base) error {
	if header == nil {
		header = Header{}
	}
	if c != nil {
		header.SetBytes(HeaderNonce, c.InitialNonce())
	}
	if _, err := header.WriteTo(w); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	if c == nil {
		if err := json.NewEncoder(w).Encode(base); err != nil {
			return fmt.Errorf("write json base: %w", err)
		}
	} else {
		plaintext, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("encode json base: %w", err)
		}

		ciphertext, err := c.Encrypt(plaintext)
		if err != nil {
			return fmt.Errorf("encrypt base: %w", err)
		}

		bw := bufio.NewWriter(w)
		dw := textproto.NewWriter(bw).DotWriter()
		wc := base64.NewEncoder(base64.StdEncoding, dw)
		if _, err := io.Copy(wc, bytes.NewReader(ciphertext)); err != nil {
			return err
		}
		if err := wc.Close(); err != nil {
			return err
		}
		if err := dw.Close(); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func writeChange(w chunkio.Writer, change Change) error {
	b := bytes.Buffer{}

	if _, err := io.WriteString(&b, change.TypeName()); err != nil {
		return err
	}
	if _, err := io.WriteString(&b, " "); err != nil {
		return err
	}
	if err := json.NewEncoder(&b).Encode(change); err != nil {
		return err
	}

	if err := w.Write(bytes.TrimSuffix(b.Bytes(), []byte("\n"))); err != nil {
		return err
	}

	return nil
}
