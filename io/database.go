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
	"bytes"
	"fmt"
	"io"
	"sync"

	tapedb "github.com/simia-tech/tapedb/v2"
)

type Database[B tapedb.Base, S tapedb.State] struct {
	base       B
	state      S
	logW       LogWriter
	logLen     int
	stateMutex *sync.RWMutex
}

func NewDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	logW LogWriter,
) (*Database[B, S], error) {
	base := f.NewBase()

	stateMutex := &sync.RWMutex{}
	state := f.NewState(base, stateMutex.RLocker())

	return &Database[B, S]{
		base:       base,
		state:      state,
		logW:       logW,
		stateMutex: stateMutex,
	}, nil
}

func OpenDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	baseR io.Reader,
	logR LogReader,
	logW LogWriter,
) (*Database[B, S], error) {
	base := f.NewBase()

	if baseR != nil {
		if _, err := base.ReadFrom(baseR); err != nil {
			return nil, fmt.Errorf("read base: %w", err)
		}
	}

	stateMutex := &sync.RWMutex{}
	state := f.NewState(base, stateMutex.RLocker())

	logLen := 0
	err := readLogEntries(logR, func(entry LogEntry) error {
		r, err := entry.Reader()
		if err != nil {
			return fmt.Errorf("reader: %w", err)
		}

		change, err := readChange[B, S, F](f, r)
		if err != nil {
			return fmt.Errorf("read change: %w", err)
		}

		logLen++

		return state.Apply(change)
	})
	if err != nil {
		return nil, fmt.Errorf("read log entries: %w", err)
	}

	return &Database[B, S]{
		base:       base,
		state:      state,
		logW:       logW,
		logLen:     logLen,
		stateMutex: stateMutex,
	}, nil
}

func (db *Database[B, S]) Base() B {
	return db.base
}

func (db *Database[B, S]) State() S {
	return db.state
}

func (db *Database[B, S]) Apply(c tapedb.Change) error {
	db.stateMutex.Lock()
	defer db.stateMutex.Unlock()

	if err := db.state.Apply(c); err != nil {
		return err
	}

	if _, err := writeChange(db.logW, c); err != nil {
		return err
	}

	db.logLen++

	return nil
}

func (db *Database[B, S]) Close() error {
	return nil
}

func (db *Database[B, S]) LogLen() int {
	return db.logLen
}

func writeChange[W LogWriter](w W, c tapedb.Change) (int64, error) {
	typeName := c.TypeName()

	buffer := bytes.Buffer{}
	buffer.WriteByte(byte(len(typeName)))
	buffer.WriteString(typeName)

	if _, err := c.WriteTo(&buffer); err != nil {
		return 0, err
	}

	return w.WriteEntry(LogEntryTypeBinary, buffer.Bytes())
}

func readChange[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	r io.Reader,
) (tapedb.Change, error) {
	sizeBytes := [1]byte{}
	if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
		return nil, fmt.Errorf("read type name size: %w", err)
	}
	size := sizeBytes[0]

	typeNameBytes := make([]byte, size)
	if _, err := io.ReadFull(r, typeNameBytes); err != nil {
		return nil, fmt.Errorf("read type name of size %d: %w", size, err)
	}
	typeName := string(typeNameBytes)

	change, err := f.NewChange(typeName)
	if err != nil {
		return nil, err
	}

	if _, err := change.ReadFrom(r); err != nil {
		return nil, err
	}

	return change, nil
}

func SpliceDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	baseW io.Writer,
	logW LogWriter,
	baseR io.Reader,
	logR LogReader,
	rebaseChangeSelectFn func(tapedb.Change, int) (bool, error),
	baseOrChangeWrittenFn func(any) error,
) error {
	base := f.NewBase()
	if baseR != nil {
		if _, err := base.ReadFrom(baseR); err != nil {
			return fmt.Errorf("read base: %w", err)
		}
	}

	logIndex := 0
	rebase := true
	baseWritten := false

	err := readLogEntries(logR, func(entry LogEntry) error {
		r, err := entry.Reader()
		if err != nil {
			return err
		}

		change, err := readChange[B, S, F](f, r)
		if err != nil {
			return err
		}

		switch {
		case rebase:
			rebase, err = rebaseChangeSelectFn(change, logIndex)
			if err != nil {
				return err
			}

			if rebase {
				if err := base.Apply(change); err != nil {
					return fmt.Errorf("apply change to base: %w", err)
				}
				break
			}

			fallthrough
		case !baseWritten:
			if _, err := base.WriteTo(baseW); err != nil {
				return fmt.Errorf("write base: %w", err)
			}
			if err := baseOrChangeWrittenFn(base); err != nil {
				return err
			}
			baseWritten = true

			fallthrough
		default:
			if _, err := writeChange(logW, change); err != nil {
				return fmt.Errorf("write change: %w", err)
			}
			if err := baseOrChangeWrittenFn(change); err != nil {
				return err
			}
		}
		logIndex++

		return nil
	})
	if err != nil {
		return fmt.Errorf("read log entries: %w", err)
	}

	if !baseWritten {
		if _, err := base.WriteTo(baseW); err != nil {
			return fmt.Errorf("write base: %w", err)
		}
		if err := baseOrChangeWrittenFn(base); err != nil {
			return err
		}
	}

	return nil
}
