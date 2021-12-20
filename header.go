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

package tapedb

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net/textproto"
	"sort"
	"strconv"
)

type Header textproto.MIMEHeader

func ReadHeader(r io.Reader) (Header, io.Reader, error) {
	if r == nil {
		return Header{}, nil, nil
	}

	tr := textproto.NewReader(bufio.NewReader(r))
	mimeHeader, err := tr.ReadMIMEHeader()
	if err != nil {
		return Header{}, nil, err
	}

	return Header(mimeHeader), tr.R, nil
}

func (h Header) SetBytes(key string, value []byte) {
	h.Set(key, hex.EncodeToString(value))
}

func (h Header) SetUInt64(key string, value uint64) {
	h.Set(key, strconv.FormatUint(value, 10))
}

func (h Header) Set(key, value string) {
	textproto.MIMEHeader(h).Set(key, value)
}

func (h Header) GetBytes(key string, defaultValue []byte) []byte {
	if value := h.Get(key); value != "" {
		if v, err := hex.DecodeString(value); err == nil {
			return v
		}
	}
	return defaultValue
}

func (h Header) GetUInt64(key string, defaultValue uint64) uint64 {
	if value := h.Get(key); value != "" {
		if v, err := strconv.ParseUint(value, 10, 64); err == nil {
			return v
		}
	}
	return defaultValue
}

func (h Header) Get(key string) string {
	return textproto.MIMEHeader(h).Get(key)
}

func (h Header) Has(key string) bool {
	_, ok := h[textproto.CanonicalMIMEHeaderKey(key)]
	return ok
}

func (h Header) WriteTo(w io.Writer) (int64, error) {
	total := int64(0)

	keys := []string{}
	for key := range h {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		for _, value := range h[key] {
			n, err := fmt.Fprintf(w, "%s: %s\n", key, value)
			if err != nil {
				return total, err
			}
			total += int64(n)
		}
	}

	n, err := fmt.Fprintln(w)
	if err != nil {
		return total, err
	}
	total += int64(n)

	return total, nil
}
