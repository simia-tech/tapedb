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
	"io"
)

type Buffer struct {
	chunks [][]byte
	index  int
}

func NewBufferString(values ...string) *Buffer {
	chunks := make([][]byte, len(values))
	for index, value := range values {
		chunks[index] = []byte(value)
	}
	return NewBuffer(chunks...)
}

func NewBuffer(values ...[]byte) *Buffer {
	return &Buffer{chunks: values, index: 0}
}

func (b *Buffer) Read() ([]byte, error) {
	if b.index >= len(b.chunks) {
		return nil, io.EOF
	}
	chunk := b.chunks[b.index]
	b.index++
	return chunk, nil
}

func (b *Buffer) Write(chunk []byte) error {
	b.chunks = append(b.chunks, chunk)
	return nil
}

func (b Buffer) At(index int) []byte {
	return b.chunks[index]
}

func (b Buffer) StringAt(index int) string {
	return string(b.At(index))
}

func (b Buffer) Len() int {
	return len(b.chunks)
}

func (b *Buffer) String() string {
	return string(bytes.Join(b.chunks, []byte("\n"))) + "\n"
}
