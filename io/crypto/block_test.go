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
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2/io/crypto"
)

func TestBlockWriter(t *testing.T) {
	t.Run("OneSmallBlock", func(t *testing.T) {
		cipherText := bytes.Buffer{}

		w, err := crypto.NewBlockWriter(&cipherText, testKey, crypto.FixedNonceFn(testNonce))
		require.NoError(t, err)

		fmt.Fprint(w, "test")

		require.NoError(t, w.Close())

		assert.Equal(t, "AAAAAAAAAAAAAAAAFAA9s/QnllYAbncJNTQ1t10QttkpWg==", base64.StdEncoding.EncodeToString(cipherText.Bytes()))
	})

	t.Run("TwoBlocks", func(t *testing.T) {
		cipherText := bytes.Buffer{}

		w, err := crypto.NewBlockWriter(&cipherText, testKey, crypto.FixedNonceFn(testNonce))
		require.NoError(t, err)

		fmt.Fprint(w, strings.Repeat("test", crypto.BlockSize/4))
		fmt.Fprint(w, strings.Repeat("test", 2))

		require.NoError(t, w.Close())

		encodedCipherText := base64.StdEncoding.EncodeToString(cipherText.Bytes())
		assert.True(t, strings.HasPrefix(encodedCipherText, "AAAAAAAA"), "%q should have prefix %q", encodedCipherText, "AAAAAAAA")
		assert.True(t, strings.HasSuffix(encodedCipherText, "tIiDI9Od"), "%q should have suffix %q", encodedCipherText, "tIiDI9Od")
	})
}

func TestBlockReader(t *testing.T) {
	t.Run("OneSmallBlock", func(t *testing.T) {
		cipherText, _ := base64.StdEncoding.DecodeString("AAAAAAAAAAAAAAAAFAA9s/QnllYAbncJNTQ1t10QttkpWg==")

		r, err := crypto.NewBlockReader(bytes.NewReader(cipherText), testKey)
		require.NoError(t, err)

		plainText, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, "test", string(plainText))
	})

	t.Run("TwoBlocks", func(t *testing.T) {
		cipherText, _ := base64.StdEncoding.DecodeString("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")

		r, err := crypto.NewBlockReader(bytes.NewReader(cipherText), testKey)
		require.NoError(t, err)

		plainText, err := ioutil.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t, strings.Repeat("test", (crypto.BlockSize/4)+2), string(plainText))
	})
}
