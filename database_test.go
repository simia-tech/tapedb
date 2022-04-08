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

package tapedb_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2"
)

func TestDatabaseCreate(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		buffer := makeBuffer("")

		db, err := model.CreateDatabase(buffer)
		require.NoError(t, err)

		state := db.State().(*testState)
		assert.Len(t, state.items, 0)

		assert.Equal(t, 0, db.ChangesCount())

		assert.Equal(t, `
{}
`,
			buffer.String())
	})

	t.Run("Encrypted", func(t *testing.T) {
		restore := fixedNonce(testNonce)
		defer restore()

		buffer := makeBuffer("")

		db, err := model.CreateDatabase(buffer, tapedb.WithCreateKey(testKey))
		require.NoError(t, err)

		state := db.State().(*testState)
		assert.Len(t, state.items, 0)

		assert.Equal(t, 0, db.ChangesCount())

		assert.Equal(t, "Nonce: 000000000000000000000000\n\n"+
			"MqulIDmrcA0bDIsXweQfC+gK\r\n"+
			".\r\n",
			buffer.String())
	})
}

func TestDatabaseOpen(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		t.Run("WithBase", func(t *testing.T) {
			buffer := makeBuffer(`
{"items":["one","two","three"]}
`)

			db, err := model.OpenDatabase(buffer)
			require.NoError(t, err)

			assert.Equal(t, 0, db.ChangesCount())

			state := db.State().(*testState)
			assert.Equal(t, []string{"one", "two", "three"}, state.items)
		})

		t.Run("WithBaseAndChanges", func(t *testing.T) {
			buffer := makeBuffer(`Name: Sample Base

{"items":["one","two","three"]}
addItem {"name":"four"}
removeItem {"name":"two"}
`)

			db, err := model.OpenDatabase(buffer)
			require.NoError(t, err)

			assert.Equal(t, 2, db.ChangesCount())

			state := db.State().(*testState)
			assert.Equal(t, []string{"one", "three", "four"}, state.items)
		})
	})

	t.Run("Encrypted", func(t *testing.T) {
		t.Run("WithBase", func(t *testing.T) {
			buffer := makeBuffer(`Nonce: 000000000000000000000000

MvTuJ/z21a7Z0lgHDuSSsZvZXEEWRuBMOC9TO77qgVwJjLeV3gxfwieM1FzM9PEs
.
`)

			db, err := model.OpenDatabase(buffer, tapedb.WithOpenKey(testKey))
			require.NoError(t, err)

			assert.Equal(t, 0, db.ChangesCount())

			state := db.State().(*testState)
			assert.Equal(t, []string{"one", "two", "three"}, state.items)
		})

		t.Run("WithBaseAndChanges", func(t *testing.T) {
			buffer := makeBuffer(`Nonce: 000000000000000000000000

MvTuJ/z21a7Z0lgHDuSSsZvZXEEWRuBMOC9TO77qgVwJjLeV3gxfwieM1FzM9PEs
.
zEjhe4DN27dlYVY11+5X5I7tpckSXBMij61x+ChmS+yQdVotqWqi
nL2avBvzvy1CvrSpn3EMcxjUhKHgqVaReVyr55/KCJYUXTYIREfCQgQ
`)

			db, err := model.OpenDatabase(buffer, tapedb.WithOpenKey(testKey))
			require.NoError(t, err)

			assert.Equal(t, 2, db.ChangesCount())

			state := db.State().(*testState)
			assert.Equal(t, []string{"one", "three", "four"}, state.items)
		})
	})
}

func TestDatabaseApply(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		buffer := makeBuffer("")

		db, err := model.CreateDatabase(buffer)
		require.NoError(t, err)

		require.NoError(t,
			db.Apply(&testAddItemChange{Name: "one"}))

		assert.Equal(t, 1, db.ChangesCount())

		assert.Equal(t, `
{}
addItem {"name":"one"}
`,
			buffer.String())
	})

	t.Run("Encrypted", func(t *testing.T) {
		restore := fixedNonce(testNonce)
		defer restore()

		buffer := makeBuffer("")

		db, err := model.CreateDatabase(buffer, tapedb.WithCreateKey(testKey))
		require.NoError(t, err)

		require.NoError(t,
			db.Apply(&testAddItemChange{Name: "one"}))

		assert.Equal(t, 1, db.ChangesCount())

		assert.Equal(t, "Nonce: 000000000000000000000000\n\n"+
			"MqulIDmrcA0bDIsXweQfC+gK\r\n"+
			".\r\n"+
			"zEjhe4DN27dlYVY11+5X5I7kpNlCA7YIWPPHeshT5ameEEHYieQ\n",
			buffer.String())
	})
}

func TestDatabaseSplice(t *testing.T) {
	t.Run("FromPlainToEncrypted", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			restore := fixedNonce(testNonce)
			defer restore()

			input := strings.NewReader(``)
			output := bytes.Buffer{}

			payloadIDs, err := model.SpliceDatabase(&output, input, tapedb.WithTargetKey(testKey))
			require.NoError(t, err)
			require.Empty(t, payloadIDs)

			assert.Equal(t, "Nonce: 000000000000000000000000\n\n"+
				"MqulIDmrcA0bDIsXweQfC+gK\r\n"+
				".\r\n",
				output.String())
		})

		t.Run("WithBase", func(t *testing.T) {
			restore := fixedNonce(testNonce)
			defer restore()

			input := strings.NewReader(`
{"items":["one","two","three","four"]}
`)
			output := bytes.Buffer{}

			payloadIDs, err := model.SpliceDatabase(&output, input, tapedb.WithTargetKey(testKey))
			require.NoError(t, err)
			require.Empty(t, payloadIDs)

			assert.Equal(t, "Nonce: 000000000000000000000000\n\n"+
				"MvTuJ/z21a7Z0lgHDuSSsZvZXEEWRuBMOC9TO76b3jBfFsT9V1Ffu4XUb/IBfbV8dfkt/r/v\r\n"+
				".\r\n",
				output.String())
		})

		t.Run("WithBaseAndChanges", func(t *testing.T) {
			restore := fixedNonce(testNonce)
			defer restore()

			input := strings.NewReader(`
{
	"items":["one","two","three"]
}
addItem {"name":"four"}
removeItem {"name":"two"}
`)
			output := bytes.Buffer{}

			payloadIDs, err := model.SpliceDatabase(&output, input, tapedb.WithTargetKey(testKey))
			require.NoError(t, err)
			require.Empty(t, payloadIDs)

			assert.Equal(t, "Nonce: 000000000000000000000000\n\n"+
				"MvTuJ/z21a7Z0lgHDuSSsZvZXEEWRuBMOC9TO77qgTiB1j2GFCCHfCwIiS5wx0w=\r\n"+
				".\r\n"+
				"zEjhe4DN27dlYVY11+5X5I7tpckSXBMij61x+ChmS+yQdVotqWqi\n"+
				"nL2avBvzvy1CvrSpn3EMcxjUhKHgqVaReVyr55/KCJYUXTYIREfCQgQ\n",
				output.String())
		})
	})
}
