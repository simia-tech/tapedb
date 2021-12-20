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

package chunkio_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/chunkio"
)

var (
	testKey   = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	testNonce = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
)

func TestAESReadWriter(t *testing.T) {
	c, err := chunkio.NewAESCrypter(testKey, testNonce)
	require.NoError(t, err)

	b := chunkio.NewBufferString(
		"PbP0J6k4PqOUMxSnS7QI6xe48lPG",
		"2Un2RsWLO3T3zmQcDThsAtSO1o09")
	d := chunkio.NewBase64ReadWriter(b)

	rw := chunkio.NewAESReadWriter(d, c)

	data, err := rw.Read()
	require.NoError(t, err)
	assert.Equal(t, "test0", string(data))

	data, err = rw.Read()
	require.NoError(t, err)
	assert.Equal(t, "test1", string(data))

	require.NoError(t, rw.Write([]byte("test0")))
	require.Equal(t, 3, b.Len())
	assert.Equal(t, "mr2Ep10AODynLr+POZ/Sibl8ITop", b.StringAt(2))

	require.NoError(t, rw.Write([]byte("test1")))
	require.Equal(t, 4, b.Len())
	assert.Equal(t, "TydqRVxLF0HY4S7+AKH2meT+hHa4", b.StringAt(3))
}
