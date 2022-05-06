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

package file

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"net/textproto"
	"os"
	"sort"
	"strconv"
)

type Meta textproto.MIMEHeader

func ReadMetaFile(path string) (Meta, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ReadMeta(f)
}

func ReadMeta(r io.Reader) (Meta, error) {
	if r == nil {
		return Meta{}, nil
	}

	tr := textproto.NewReader(bufio.NewReader(r))
	mimeHeader, err := tr.ReadMIMEHeader()
	if err != nil {
		return Meta{}, fmt.Errorf("read mime header: %w", err)
	}

	return Meta(mimeHeader), nil
}

func (m Meta) SetBytes(key string, value []byte) {
	m.Set(key, hex.EncodeToString(value))
}

func (m Meta) SetUInt64(key string, value uint64) {
	m.Set(key, strconv.FormatUint(value, 10))
}

func (m Meta) Set(key, value string) {
	textproto.MIMEHeader(m).Set(key, value)
}

func (m Meta) GetBytes(key string, defaultValue []byte) []byte {
	if value := m.Get(key); value != "" {
		if v, err := hex.DecodeString(value); err == nil {
			return v
		}
	}
	return defaultValue
}

func (m Meta) GetUInt64(key string, defaultValue uint64) uint64 {
	if value := m.Get(key); value != "" {
		if v, err := strconv.ParseUint(value, 10, 64); err == nil {
			return v
		}
	}
	return defaultValue
}

func (m Meta) Get(key string) string {
	return textproto.MIMEHeader(m).Get(key)
}

func (m Meta) Has(key string) bool {
	_, ok := m[textproto.CanonicalMIMEHeaderKey(key)]
	return ok
}

func (m Meta) WriteTo(w io.Writer) (int64, error) {
	total := int64(0)

	keys := []string{}
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		for _, value := range m[key] {
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
