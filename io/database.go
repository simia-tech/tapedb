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
	"errors"
	"fmt"
	"io"

	"github.com/simia-tech/tapedb/v2"
)

var (
	ErrMalformedLog = errors.New("malformed log")
)

type Database[B tapedb.Base, S tapedb.State] struct {
	base   B
	state  S
	logW   io.Writer
	logLen int
}

func NewDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	logW io.Writer,
) (*Database[B, S], error) {
	base := f.NewBase()
	state := f.NewState(base)

	return &Database[B, S]{
		base:  base,
		state: state,
		logW:  logW,
	}, nil
}

func OpenDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	baseR, logR io.Reader,
	logW io.Writer,
) (*Database[B, S], error) {
	base := f.NewBase()

	if baseR != nil {
		if _, err := base.ReadFrom(baseR); err != nil {
			return nil, fmt.Errorf("read base: %w", err)
		}
	}

	state := f.NewState(base)

	logLen := 0
	if logR != nil {
		scanner := bufio.NewScanner(logR)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}

			change, err := readChange[B, S, F](f, line)
			if err != nil {
				return nil, err
			}

			if err := state.Apply(change); err != nil {
				return nil, err
			}

			logLen++
		}
	}

	return &Database[B, S]{
		base:   base,
		state:  state,
		logW:   logW,
		logLen: logLen,
	}, nil
}

func (db *Database[B, S]) Base() B {
	return db.base
}

func (db *Database[B, S]) State() S {
	return db.state
}

func (db *Database[B, S]) Apply(c tapedb.Change) error {
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

func writeChange[W io.Writer](w W, c tapedb.Change) (int64, error) {
	total := int64(0)

	n, err := fmt.Fprint(w, c.TypeName(), " ")
	if err != nil {
		return total, err
	}
	total += int64(n)

	n64, err := c.WriteTo(w)
	if err != nil {
		return total, err
	}
	total += n64

	return total, nil
}

func readChange[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	line []byte,
) (tapedb.Change, error) {
	parts := bytes.SplitN(line, []byte(" "), 2)
	if len(parts) != 2 {
		return nil, ErrMalformedLog
	}

	change, err := f.NewChange(string(parts[0]))
	if err != nil {
		return nil, err
	}

	if _, err := change.ReadFrom(bytes.NewReader(parts[1])); err != nil {
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
	baseW, logW io.Writer,
	baseR, logR io.Reader,
	rebaseLogEntries int,
	baseOrChangeWrittenFn func(any) error,
) error {
	base := f.NewBase()
	if baseR != nil {
		if _, err := base.ReadFrom(baseR); err != nil {
			return fmt.Errorf("read base: %w", err)
		}
	}

	logIndex := 0
	baseWritten := false

	if logR != nil {
		scanner := bufio.NewScanner(logR)
		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}

			change, err := readChange[B, S, F](f, line)
			if err != nil {
				return err
			}

			switch {
			case logIndex < rebaseLogEntries:
				if err := base.Apply(change); err != nil {
					return fmt.Errorf("apply change to base: %w", err)
				}
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
		}
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
