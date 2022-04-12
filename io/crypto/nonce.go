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

package crypto

import (
	"crypto/rand"
	"io"
)

type NonceFunc func(int) []byte

func RandomNonceFn() NonceFunc {
	return func(size int) []byte {
		nonce := make([]byte, size)
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			panic(err)
		}
		return nonce
	}
}

func FixedNonceFn(nonce []byte) NonceFunc {
	return func(size int) []byte {
		n := make([]byte, size)
		if len(nonce) > size {
			copy(n[:], nonce[len(nonce)-size:])
		} else {
			copy(n[size-len(nonce):], nonce)
		}
		return n
	}
}
