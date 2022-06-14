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
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type LogEntryType uint32

const (
	LogEntryTypeBinary          LogEntryType = 0x00000000
	LogEntryTypeAESGCMEncrypted LogEntryType = 0x10000000
	LogEntryTypeMask            LogEntryType = 0xf0000000
)

type LogEntry interface {
	Type() LogEntryType
	Reader() (io.Reader, error)
}

type logEntry struct {
	entryType LogEntryType
	reader    io.Reader
}

var _ LogEntry = &logEntry{}

func (e *logEntry) Type() LogEntryType {
	return e.entryType
}

func (e *logEntry) Reader() (io.Reader, error) {
	return e.reader, nil
}

type LogReader interface {
	ReadEntry() (LogEntry, error)
}

var _ LogReader = &logReader[io.ReadSeeker]{}

type logReader[R io.ReadSeeker] struct {
	r               R
	lastSize        uint32
	lastCountReader *CountReader[io.Reader]
}

func NewLogReader[R io.ReadSeeker](r R) *logReader[R] {
	return &logReader[R]{r: r}
}

func (r *logReader[R]) ReadEntry() (LogEntry, error) {
	if r.lastCountReader != nil {
		left := int64(r.lastSize) - int64(r.lastCountReader.Count())
		if _, err := r.r.Seek(left, io.SeekCurrent); err != nil {
			return nil, err
		}
	}

	et, size, err := r.readEntryHeader()
	if err != nil {
		return nil, err
	}

	r.lastSize = size
	r.lastCountReader = NewCountReader(io.LimitReader(r.r, int64(size)))

	return &logEntry{
		entryType: et,
		reader:    r.lastCountReader,
	}, nil
}

func (r *logReader[R]) readEntryHeader() (LogEntryType, uint32, error) {
	buffer := [4]byte{}
	if _, err := io.ReadFull(r.r, buffer[:]); err != nil {
		return 0, 0, err
	}

	size := binary.BigEndian.Uint32(buffer[:])
	et := LogEntryType(size & uint32(LogEntryTypeMask))
	size &= uint32(^LogEntryTypeMask)

	return et, size, nil
}

type LogWriter interface {
	WriteEntry(LogEntryType, []byte) (int64, error)
}

type logWriter[W io.Writer] struct {
	w *bufio.Writer
}

var _ LogWriter = &logWriter[io.Writer]{}

func NewLogWriter[W io.Writer](w W) *logWriter[W] {
	return &logWriter[W]{w: bufio.NewWriter(w)}
}

func (w *logWriter[W]) WriteEntry(et LogEntryType, data []byte) (int64, error) {
	total, err := w.writeEntryHeader(et, uint32(len(data)))
	if err != nil {
		return total, err
	}

	n, err := io.Copy(w.w, bytes.NewReader(data))
	total += n
	if err != nil {
		return total, err
	}

	if err := w.w.Flush(); err != nil {
		return total, err
	}

	return total, nil
}

func (w *logWriter[W]) writeEntryHeader(et LogEntryType, size uint32) (int64, error) {
	size &= uint32(^LogEntryTypeMask)
	size |= uint32(et)

	buffer := [4]byte{}
	binary.BigEndian.PutUint32(buffer[:], size)

	n, err := w.w.Write(buffer[:])
	if err != nil {
		return int64(n), err
	}

	return int64(n), nil
}

func ReadLogLen(r LogReader) (int, error) {
	logIndex := 0
	err := ReadLogEntries(r, func(_ LogEntry) error {
		logIndex++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return logIndex, nil
}

func ReadLogEntries(r LogReader, fn func(LogEntry) error) error {
	if r == nil {
		return nil
	}

	for index := 0; true; index++ {
		entry, err := r.ReadEntry()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read entry %d: %w", index, err)
		}
		if err := fn(entry); err != nil {
			return fmt.Errorf("entry %d: %w", index, err)
		}
	}

	return nil
}
