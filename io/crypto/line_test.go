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

package crypto_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2/io/crypto"
)

func TestLineWriter(t *testing.T) {
	t.Run("TwoLines", func(t *testing.T) {
		cipherText := bytes.Buffer{}

		w, err := crypto.NewLineWriter(&cipherText, testKey, crypto.FixedNonceFn(testNonce))
		require.NoError(t, err)

		fmt.Fprintln(w, "test")
		fmt.Fprint(w, "test")

		require.NoError(t, w.Close())

		assert.Equal(t, "AAAAAAAAAAAAAAAAPbP0J5ZWAG53CTU0NbddELbZKVo\nAAAAAAAAAAAAAAAAPbP0J5ZWAG53CTU0NbddELbZKVo\n", cipherText.String())
	})
}

func TestLineReader(t *testing.T) {
	t.Run("TwoLines", func(t *testing.T) {
		cipherText := "AAAAAAAAAAAAAAAAPbP0J5ZWAG53CTU0NbddELbZKVo\nAAAAAAAAAAAAAAAAPbP0J5ZWAG53CTU0NbddELbZKVo\n"

		r, err := crypto.NewLineReader(strings.NewReader(cipherText), testKey)
		require.NoError(t, err)

		plainText, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, "test\ntest\n", string(plainText))
	})
}

func TestLineWriterAndReader(t *testing.T) {
	cipherText := bytes.Buffer{}

	w, err := crypto.NewLineWriter(&cipherText, testKey, crypto.RandomNonceFn())
	require.NoError(t, err)

	for index := 0; index < 50; index++ {
		fmt.Fprintf(w, "index = %d\n", index)
	}

	require.NoError(t, w.Close())

	r, err := crypto.NewLineReader(&cipherText, testKey)
	require.NoError(t, err)

	scanner := bufio.NewScanner(r)

	for index := 0; scanner.Scan(); index++ {
		assert.Equal(t,
			fmt.Sprintf("index = %d", index),
			scanner.Text())
	}
	require.NoError(t, scanner.Err())
}
