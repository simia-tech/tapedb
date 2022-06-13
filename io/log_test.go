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

package io_test

import (
	"bytes"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tapeio "github.com/simia-tech/tapedb/v2/io"
)

func TestLogReader(t *testing.T) {
	t.Run("ReadBinary", func(t *testing.T) {
		buffer, err := hex.DecodeString("0000000474657374")
		require.NoError(t, err)
		r := tapeio.NewLogReader(bytes.NewReader(buffer))

		entry, err := r.ReadEntry()
		require.NoError(t, err)
		assert.Equal(t, tapeio.LogEntryTypeBinary, entry.Type())

		reader, err := entry.Reader()
		require.NoError(t, err)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "test", string(data))
	})
}

func TestLogWriter(t *testing.T) {
	t.Run("WriteBinary", func(t *testing.T) {
		buffer := bytes.Buffer{}
		w := tapeio.NewLogWriter(&buffer)

		n, err := w.WriteEntry(tapeio.LogEntryTypeBinary, []byte("test"))
		require.NoError(t, err)
		assert.Equal(t, 8, int(n))

		assert.Equal(t, "0000000474657374", hex.EncodeToString(buffer.Bytes()))
	})

	t.Run("WriteEncrypted", func(t *testing.T) {
		buffer := bytes.Buffer{}
		w := tapeio.NewLogWriter(&buffer)

		n, err := w.WriteEntry(tapeio.LogEntryTypeAESGCMEncrypted, []byte("test"))
		require.NoError(t, err)
		assert.Equal(t, 8, int(n))

		assert.Equal(t, "1000000474657374", hex.EncodeToString(buffer.Bytes()))
	})
}
