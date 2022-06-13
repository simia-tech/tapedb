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
	"encoding/hex"
)

type LogBuffer struct {
	buffer Buffer
	w      LogWriter
	r      LogReader
}

var (
	_ LogWriter = &LogBuffer{}
	_ LogReader = &LogBuffer{}
)

func NewLogBuffer(data []byte) *LogBuffer {
	return &LogBuffer{buffer: *NewBuffer(data)}
}

func NewLogBufferString(data string) *LogBuffer {
	return &LogBuffer{buffer: *NewBufferString(data)}
}

func (b *LogBuffer) WriteEntry(et LogEntryType, data []byte) (int64, error) {
	if b.w == nil {
		b.w = NewLogWriter(&b.buffer)
	}
	return b.w.WriteEntry(et, data)
}

func (b *LogBuffer) ReadEntry() (LogEntry, error) {
	if b.r == nil {
		b.r = NewLogReader(&b.buffer)
	}
	return b.r.ReadEntry()
}

func (b *LogBuffer) HexString() string {
	return hex.EncodeToString(b.buffer.Bytes())
}

func (b *LogBuffer) String() string {
	return b.buffer.String()
}
