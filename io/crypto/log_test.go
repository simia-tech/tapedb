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
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tapeio "github.com/simia-tech/tapedb/v2/io"
	"github.com/simia-tech/tapedb/v2/io/crypto"
)

func TestLogWriter(t *testing.T) {
	logBuffer := tapeio.LogBuffer{}

	w, err := crypto.NewLogWriter(&logBuffer, testKey, crypto.FixedNonceFn(testNonce))
	require.NoError(t, err)

	n, err := w.WriteEntry(tapeio.LogEntryTypeBinary, []byte("test"))
	require.NoError(t, err)
	assert.Equal(t, 36, int(n))

	assert.Equal(t,
		"100000200000000000000000000000003db3f4279656006e7709353435b75d10b6d9295a",
		logBuffer.HexString())
}

func TestLogReader(t *testing.T) {
	encrypted, _ := hex.DecodeString("100000200000000000000000000000003db3f4279656006e7709353435b75d10b6d9295a")
	logR := tapeio.NewLogBuffer(encrypted)

	r, err := crypto.NewLogReader(logR, testKey)
	require.NoError(t, err)

	entry, err := r.ReadEntry()
	require.NoError(t, err)
	assert.Equal(t, tapeio.LogEntryTypeBinary, entry.Type())

	reader, err := entry.Reader()
	require.NoError(t, err)

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "test", string(data))
}
