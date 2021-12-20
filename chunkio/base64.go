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

import "encoding/base64"

var encoding = base64.RawStdEncoding

type Base64Reader struct {
	r Reader
}

func NewBase64Reader(r Reader) *Base64Reader {
	return &Base64Reader{r: r}
}

func (r *Base64Reader) Read() ([]byte, error) {
	chunk, err := r.r.Read()
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, encoding.DecodedLen(len(chunk)))
	if _, err := encoding.Decode(buffer, chunk); err != nil {
		return nil, err
	}

	return buffer, nil
}

type Base64Writer struct {
	w Writer
}

func NewBase64Writer(w Writer) *Base64Writer {
	return &Base64Writer{w: w}
}

func (w *Base64Writer) Write(chunk []byte) error {
	buffer := make([]byte, encoding.EncodedLen(len(chunk)))
	encoding.Encode(buffer, chunk)
	return w.w.Write(buffer)
}

type Base64ReadWriter struct {
	r *Base64Reader
	w *Base64Writer
}

func NewBase64ReadWriter(rw ReadWriter) *Base64ReadWriter {
	return &Base64ReadWriter{
		r: NewBase64Reader(rw),
		w: NewBase64Writer(rw),
	}
}

func (rw *Base64ReadWriter) Read() ([]byte, error) {
	return rw.r.Read()
}

func (rw *Base64ReadWriter) Write(chunk []byte) error {
	return rw.w.Write(chunk)
}
