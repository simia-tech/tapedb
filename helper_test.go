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

package tapedb_test

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb"
)

var model = tapedb.NewModel(
	tapedb.PrototypeBaseFactory(&testBase{}),
	newTestState,
	tapedb.PrototypeChangeFactory(
		&testAddItemChange{},
		&testRemoveItemChange{},
		&testAttachPayloadChange{},
		&testDetachPayloadChange{},
	))

var testKey = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
}

var testNonce = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

func fixedNonce(nonce []byte) func() {
	f := tapedb.RandomNonce
	tapedb.RandomNonce = func() []byte { return nonce }
	return func() {
		tapedb.RandomNonce = f
	}
}

type testBase struct {
	Items    []string          `json:"items,omitempty"`
	Payloads map[string]string `json:"payloads,omitempty"`
}

func (b *testBase) Apply(change tapedb.Change) error {
	switch t := change.(type) {
	case *testAddItemChange:
		b.Items = append(b.Items, t.Name)
	case *testRemoveItemChange:
		items := make([]string, 0, len(b.Items))
		for _, item := range b.Items {
			if item != t.Name {
				items = append(items, item)
			}
		}
		b.Items = items
	case *testAttachPayloadChange:
		if b.Payloads == nil {
			b.Payloads = map[string]string{}
		}
		b.Payloads[t.PayloadID] = t.Name
	case *testDetachPayloadChange:
		delete(b.Payloads, t.PayloadID)
	default:
		return fmt.Errorf("cannot apply change type %T", t)
	}
	return nil
}

func (b *testBase) PayloadIDs() []string {
	ids := []string{}
	for id := range b.Payloads {
		ids = append(ids, id)
	}
	return ids
}

type testState struct {
	readLocker sync.Locker
	items      []string
	payloads   map[string]string
}

func newTestState(base tapedb.Base, readLocker sync.Locker) (tapedb.State, error) {
	return &testState{
		readLocker: readLocker,
		items:      base.(*testBase).Items,
		payloads:   map[string]string{},
	}, nil
}

func (s *testState) Apply(change tapedb.Change) error {
	switch t := change.(type) {
	case *testAddItemChange:
		s.items = append(s.items, t.Name)
	case *testRemoveItemChange:
		items := make([]string, 0, len(s.items))
		for _, item := range s.items {
			if item != t.Name {
				items = append(items, item)
			}
		}
		s.items = items
	case *testAttachPayloadChange:
		s.payloads[t.PayloadID] = t.Name
	case *testDetachPayloadChange:
		delete(s.payloads, t.PayloadID)
	default:
		return fmt.Errorf("cannot apply change type %T", t)
	}
	return nil
}

type testAddItemChange struct {
	Name string `json:"name"`
}

func (c *testAddItemChange) TypeName() string {
	return "addItem"
}

type testRemoveItemChange struct {
	Name string `json:"name"`
}

func (c *testRemoveItemChange) TypeName() string {
	return "removeItem"
}

type testAttachPayloadChange struct {
	Name      string `json:"name"`
	PayloadID string `json:"payloadID"`
}

func (c *testAttachPayloadChange) TypeName() string {
	return "attachPayload"
}

func (c *testAttachPayloadChange) PayloadIDs() []string {
	return []string{c.PayloadID}
}

type testDetachPayloadChange struct {
	PayloadID string `json:"payloadID"`
}

func (c *testDetachPayloadChange) TypeName() string {
	return "detachPayload"
}

func makeTempDir(tb testing.TB) (string, func()) {
	path, err := ioutil.TempDir("", "tapedb-")
	require.NoError(tb, err)
	return path, func() {
		require.NoError(tb, os.RemoveAll(path))
	}
}

func makeFile(tb testing.TB, path, content string) {
	require.NoError(tb, ioutil.WriteFile(path, []byte(content), 0600))
}

func readFile(tb testing.TB, path string) string {
	data, err := ioutil.ReadFile(path)
	require.NoError(tb, err)
	return string(data)
}

func readFileBase64(tb testing.TB, path string) string {
	data, err := ioutil.ReadFile(path)
	require.NoError(tb, err)
	return base64.RawStdEncoding.EncodeToString(data)
}

func existFile(tb testing.TB, path string) bool {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	} else if err == nil {
		return true
	}
	require.NoError(tb, err)
	return false
}

type buffer struct {
	b    [1024]byte
	pos  int
	size int
}

func makeBuffer(value string) *buffer {
	b := &buffer{
		pos:  0,
		size: len(value),
	}
	copy(b.b[:], value)
	return b
}

func (b *buffer) Read(data []byte) (int, error) {
	buf := b.b[b.pos:b.size]
	l := minLen(data, buf)
	if l == 0 {
		return 0, io.EOF
	}
	copy(data, buf[:l])
	b.pos += l
	return l, nil
}

func (b *buffer) Write(data []byte) (int, error) {
	buf := b.b[b.pos:]
	l := minLen(data, buf)
	copy(buf, data[:l])
	b.pos += l
	if b.pos > b.size {
		b.size = b.pos
	}
	return l, nil
}

func (b *buffer) String() string {
	return string(b.b[:b.size])
}

func minLen(b1, b2 []byte) int {
	if l1, l2 := len(b1), len(b2); l1 < l2 {
		return l1
	} else {
		return l2
	}
}
