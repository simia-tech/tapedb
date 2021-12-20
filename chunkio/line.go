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
	"bufio"
	"bytes"
	"io"
)

type LineReader struct {
	r *bufio.Reader
}

func NewLineReader(r io.Reader) *LineReader {
	if r == nil {
		return nil
	}
	return &LineReader{r: bufio.NewReader(r)}
}

func (r *LineReader) Read() ([]byte, error) {
	chunk, err := r.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(chunk, []byte{'\n'}), nil
}

type LineWriter struct {
	w io.Writer
}

func NewLineWriter(w io.Writer) *LineWriter {
	return &LineWriter{w: w}
}

func (w *LineWriter) Write(chunk []byte) error {
	if !bytes.HasSuffix(chunk, []byte{'\n'}) {
		chunk = append(chunk, '\n')
	}
	_, err := io.Copy(w.w, bytes.NewReader(chunk))
	return err
}

type LineReadWriter struct {
	r *LineReader
	w *LineWriter
}

func NewLineReadWriter(rw io.ReadWriter) *LineReadWriter {
	return &LineReadWriter{
		r: NewLineReader(rw),
		w: NewLineWriter(rw),
	}
}

func (rw *LineReadWriter) Read() ([]byte, error) {
	return rw.r.Read()
}

func (rw *LineReadWriter) Write(chunk []byte) error {
	return rw.w.Write(chunk)
}
