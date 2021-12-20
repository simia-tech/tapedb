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
	"errors"
	"io"
)

type Reader interface {
	Read() ([]byte, error)
}

type Writer interface {
	Write([]byte) error
}

type ReadWriter interface {
	Reader
	Writer
}

type ReadWriteCloser interface {
	Reader
	Writer
	io.Closer
}

func Copy(w Writer, r Reader) error {
	if w == nil || r == nil {
		return nil
	}

	chunk, err := []byte(nil), error(nil)
	for err == nil {
		chunk, err = r.Read()
		if err != nil {
			break
		}

		if err = w.Write(chunk); err != nil {
			break
		}
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
