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
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2"
)

func TestFileDatabaseCreate(t *testing.T) {
	t.Run("CreateMissing", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := model.CreateFileDatabase(path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 0, db.ChangesCount())

		state := db.State().(*testState)
		assert.Len(t, state.items, 0)
	})

	t.Run("ErrorOnExisting", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), "test")

		db, err := model.CreateFileDatabase(path)
		require.Nil(t, db)
		assert.ErrorIs(t, err, tapedb.ErrDatabaseExists)
	})
}

func TestFileDatabaseOpen(t *testing.T) {
	t.Run("NoFile", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := model.OpenFileDatabase(path)
		require.Nil(t, db)
		assert.ErrorIs(t, err, tapedb.ErrDatabaseMissing)
	})

	t.Run("WithBase", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{"items":["one","two","three"]}
`)

		db, err := model.OpenFileDatabase(path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 0, db.ChangesCount())

		state := db.State().(*testState)
		assert.Equal(t, []string{"one", "two", "three"}, state.items)
	})

	t.Run("WithBaseAndChanges", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{"items":["one","two","three"]}
addItem {"name":"four"}
removeItem {"name":"two"}
`)

		db, err := model.OpenFileDatabase(path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 2, db.ChangesCount())

		state := db.State().(*testState)
		assert.Equal(t, []string{"one", "three", "four"}, state.items)
	})
}

func TestFileDatabaseApply(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{}
addItem {"name":"one"}
`)

			db, err := model.OpenFileDatabase(path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(&testAddItemChange{Name: "two"}))

			assert.Equal(t, 2, db.ChangesCount())

			assert.Equal(t, `
{}
addItem {"name":"one"}
addItem {"name":"two"}
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
		})

		t.Run("WithPayload", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{"items":["one","two","three"]}
`)

			db, err := model.OpenFileDatabase(path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&testAttachPayloadChange{Name: "two", PayloadID: "123"},
					tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

			assert.Equal(t, `
{"items":["one","two","three"]}
attachPayload {"name":"two","payloadID":"123"}
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
			assert.Equal(t, "test content", readFile(t, filepath.Join(path, "payload-123")))
		})

		t.Run("WithExistingPayloadID", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			db, err := model.CreateFileDatabase(path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&testAttachPayloadChange{Name: "one", PayloadID: "123"},
					tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

			assert.ErrorIs(t,
				db.Apply(
					&testAttachPayloadChange{Name: "two", PayloadID: "123"},
					tapedb.NewPayload("123", bytes.NewReader([]byte("test content 2")))),
				tapedb.ErrPayloadIDAlreadyExists)

			assert.Equal(t, `
{}
attachPayload {"name":"one","payloadID":"123"}
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
			assert.Equal(t, "test content", readFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"123")))
		})
	})

	t.Run("Encrypted", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `Nonce: 000000000000000000000000

MquNmSFxxZN8O7KIBFqwI1PZpA==
.
zEjhe4DN27dlYVY11+5X5I7kpNlCA7YIWPPHeshT5ameEEHYieQ
`)

			db, err := model.OpenFileDatabase(path, tapedb.WithOpenKey(testKey))
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t, db.Apply(&testAddItemChange{Name: "one"}))

			assert.Equal(t, 2, db.ChangesCount())

			assert.Equal(t, `Nonce: 000000000000000000000000

MquNmSFxxZN8O7KIBFqwI1PZpA==
.
zEjhe4DN27dlYVY11+5X5I7kpNlCA7YIWPPHeshT5ameEEHYieQ
j7yTmhnzm3lc8fqz0HpPJF+Z0Oa2oz3GgNE555k4oxARXfrCRJg
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
		})

		t.Run("WithPayload", func(t *testing.T) {
			restore := fixedNonce(testNonce)
			defer restore()

			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `Nonce: 000000000000000000000000

MquNmSFxxZN8O7KIBFqwI1PZpA==
.
zEjhe4DN27dlYVY11+5X5I7kpNlCA7YIWPPHeshT5ameEEHYieQ
`)

			db, err := model.OpenFileDatabase(path, tapedb.WithOpenKey(testKey))
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&testAttachPayloadChange{Name: "two", PayloadID: "123"},
					tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

			assert.Equal(t, `Nonce: 000000000000000000000000

MquNmSFxxZN8O7KIBFqwI1PZpA==
.
zEjhe4DN27dlYVY11+5X5I7kpNlCA7YIWPPHeshT5ameEEHYieQ
j6yDsg7+pjhev/uz2T8WPBOX0+a25BvHcx8Z5imbUuxm2d2o3fc4s59H4szoSBzfjIA11TYr3bH7oYu5UII
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))

			assert.Equal(t,
				"AAAAAAAAAAAAAAAAPbP0J7n4yeKX7BQccJUnGtQ3zU4KeoCIo2hUCg",
				readFileBase64(t, filepath.Join(path, tapedb.FilePrefixPayload+"123")))
		})
	})
}

func TestFileDatabaseOpenPayload(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := model.CreateFileDatabase(path)
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(
				&testAttachPayloadChange{Name: "one", PayloadID: "123"},
				tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

		file, err := db.OpenPayload("123")
		require.NoError(t, err)

		content, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, "test content", string(content))

		require.NoError(t, file.Close())
	})

	t.Run("Encrypted", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := model.CreateFileDatabase(path, tapedb.WithCreateKey(testKey))
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(
				&testAttachPayloadChange{Name: "one", PayloadID: "123"},
				tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

		file, err := db.OpenPayload("123")
		require.NoError(t, err)

		content, err := io.ReadAll(file)
		require.NoError(t, err)
		assert.Equal(t, "test content", string(content))

		require.NoError(t, file.Close())
	})
}

func TestFileDatabaseSplice(t *testing.T) {
	t.Run("FromPlainToPlain", func(t *testing.T) {
		t.Run("NoFile", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			require.NoError(t,
				model.SpliceFileDatabase(path))

			assert.Equal(t, `
{}
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
		})

		t.Run("WithBaseAndChanges", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{"items":["one","two","three"]}
addItem {"name":"four"}
removeItem {"name":"two"}
`)

			require.NoError(t,
				model.SpliceFileDatabase(path))

			assert.Equal(t, `
{"items":["one","two","three"]}
addItem {"name":"four"}
removeItem {"name":"two"}
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
		})

		t.Run("WithPayloads", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{"items":["one","two"],"payloads":{"123":"one"}}
detachPayload {"name":"one","payloadID":"123"}
attachPayload {"name":"two","payloadID":"456"}
`)
			makeFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"123"), "test content")
			makeFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"456"), "test content")

			require.NoError(t,
				model.SpliceFileDatabase(path, tapedb.WithConsumeChanges(1)))

			assert.False(t, existFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"123")))
			assert.True(t, existFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"456")))
		})

		t.Run("WithChangeConsumed", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
{"items":["one","two","three"]}
addItem {"name":"four"}
removeItem {"name":"two"}
`)

			require.NoError(t,
				model.SpliceFileDatabase(path, tapedb.WithConsumeChanges(1)))

			assert.Equal(t, `
{"items":["one","two","three","four"]}
removeItem {"name":"two"}
`,
				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
		})
	})
}
