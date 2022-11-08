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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2"
	"github.com/simia-tech/tapedb/v2/io"
	"github.com/simia-tech/tapedb/v2/test"
)

func TestIO(t *testing.T) {
	t.Run("NewDatabase", func(t *testing.T) {
		logBuffer := io.LogBuffer{}

		db, err := io.NewDatabase[*test.Base, *test.State](
			test.NewFactory(),
			&logBuffer)
		require.NoError(t, err)

		require.NoError(t, db.Apply(&test.ChangeCounterInc{Value: 1}))

		assert.Equal(t, "\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n", logBuffer.String())
	})

	t.Run("OpenDatabase", func(t *testing.T) {
		base := "{\"value\":20}\n"
		log := io.NewLogBufferString("\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n")
		logBuffer := io.LogBuffer{}

		db, err := io.OpenDatabase[*test.Base, *test.State](
			test.NewFactory(),
			strings.NewReader(base),
			log,
			&logBuffer)
		require.NoError(t, err)

		assert.Equal(t, 23, db.State().Counter)

		require.NoError(t, db.Apply(&test.ChangeCounterInc{Value: 3}))

		assert.Equal(t, "\x00\x00\x00\x18\x0bcounter-inc{\"value\":3}\n", logBuffer.String())
	})

	t.Run("SpliceDatabase", func(t *testing.T) {
		base := "{\"value\":20}\n"
		log := io.NewLogBufferString("\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n")
		newBase := bytes.Buffer{}
		newLog := io.LogBuffer{}

		err := io.SpliceDatabase[*test.Base, *test.State](
			test.NewFactory(),
			&newBase, &newLog,
			strings.NewReader(base), log,
			func(_ tapedb.Change, logIndex int) (bool, error) {
				return logIndex < 1, nil
			}, func(_ any) error {
				return nil
			})
		require.NoError(t, err)

		assert.Equal(t, "{\"value\":22}\n", newBase.String())
		assert.Equal(t, "\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n", newLog.String())
	})
}
