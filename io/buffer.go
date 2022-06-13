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

package io

import (
	"errors"
	"io"
)

type Buffer struct {
	data      []byte
	readIndex int
}

var (
	_ io.Writer = &Buffer{}
	_ io.Reader = &Buffer{}
	_ io.Seeker = &Buffer{}
)

var (
	ErrOutOfRange = errors.New("out of range")
)

func NewBuffer(data []byte) *Buffer {
	return &Buffer{data: data}
}

func NewBufferString(data string) *Buffer {
	return NewBuffer([]byte(data))
}

func (b *Buffer) Write(data []byte) (int, error) {
	if b.data == nil {
		b.data = data
	} else {
		b.data = append(b.data, data...)
	}
	return len(data), nil
}

func (b *Buffer) Read(data []byte) (int, error) {
	if b.data == nil || b.readIndex >= len(b.data) {
		return 0, io.EOF
	}

	size, available := len(data), len(b.data)-b.readIndex

	if size > available {
		copy(data, b.data[b.readIndex:])
		b.readIndex += available
		return available, io.EOF
	}

	copy(data, b.data[b.readIndex:b.readIndex+size])
	b.readIndex += size

	return size, nil
}

func (b *Buffer) Seek(offset int64, whence int) (int64, error) {
	newReadIndex := b.readIndex

	switch whence {
	case io.SeekStart:
		newReadIndex = int(offset)
	case io.SeekCurrent:
		newReadIndex += int(offset)
	case io.SeekEnd:
		newReadIndex = len(b.data) + int(offset)
	}

	if newReadIndex < 0 {
		return 0, ErrOutOfRange
	}
	b.readIndex = newReadIndex

	return int64(newReadIndex), nil
}

func (b *Buffer) Bytes() []byte {
	return b.data
}

func (b *Buffer) String() string {
	return string(b.data)
}
