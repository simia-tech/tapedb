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

package file_test

import (
	"encoding/base64"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var testKey = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
}

var testNonce = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

func makeTempDir(tb testing.TB) (string, func()) {
	path, err := ioutil.TempDir("", "tapedb-")
	require.NoError(tb, err)
	return path, func() {
		require.NoError(tb, os.RemoveAll(path))
	}
}

func makeFile(tb testing.TB, path, content string) {
	require.NoError(tb, ioutil.WriteFile(path, []byte(content), 0600))
}

func readFile(tb testing.TB, path string) string {
	data, err := ioutil.ReadFile(path)
	require.NoError(tb, err)
	return string(data)
}

func readFileBase64(tb testing.TB, path string) string {
	data, err := ioutil.ReadFile(path)
	require.NoError(tb, err)
	return base64.RawStdEncoding.EncodeToString(data)
}
