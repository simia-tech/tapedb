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
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2/io/crypto"
	"github.com/simia-tech/tapedb/v2/io/file"
	"github.com/simia-tech/tapedb/v2/test"
)

func TestCreateDatabase(t *testing.T) {
	t.Run("CreateMissing", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 0, db.LogLen())
		assert.Equal(t, 0, db.State().Counter)
	})

	t.Run("ErrorOnExisting", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, file.FileNameLog), "test")

		db, err := file.CreateDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
		require.Nil(t, db)
		assert.ErrorIs(t, err, file.ErrDatabaseExists)
	})

	t.Run("Encrypted", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path, file.WithCreateKey(testKey))
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(&test.ChangeCounterInc{Value: 21}))
	})
}

func TestOpenDatabase(t *testing.T) {
	t.Run("WithBase", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":3}`)

		db, err := file.OpenDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 0, db.LogLen())
		assert.Equal(t, 3, db.State().Counter)
	})

	t.Run("WithBaseAndLog", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":3}`)
		makeFile(t, filepath.Join(path, file.FileNameLog), `
counter-inc {"value":1}
counter-inc {"value":2}
`)

		db, err := file.OpenDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 2, db.LogLen())
		assert.Equal(t, 6, db.State().Counter)
	})
}

func TestFileDatabaseApply(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), `{}`)
			makeFile(t, filepath.Join(path, file.FileNameLog), `
counter-inc {"value":1}
`)

			db, err := file.OpenDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(&test.ChangeCounterInc{Value: 21}))

			assert.Equal(t, 2, db.LogLen())
			assert.Equal(t, `
counter-inc {"value":1}
counter-inc {"value":21}
`,
				readFile(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithPayload", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), `{}`)
			makeFile(t, filepath.Join(path, file.FileNameLog), `
counter-inc {"value":1}
`)

			db, err := file.OpenDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&test.ChangeAttachPayload{PayloadID: "123"},
					file.NewPayload("123", strings.NewReader("test content"))))

			assert.Equal(t, `
counter-inc {"value":1}
attach-payload {"payloadID":"123"}
`,
				readFile(t, filepath.Join(path, file.FileNameLog)))
			assert.Equal(t, "test content", readFile(t, filepath.Join(path, "payload-123")))
		})

		t.Run("WithExistingPayloadID", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			db, err := file.CreateDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&test.ChangeAttachPayload{PayloadID: "123"},
					file.NewPayload("123", strings.NewReader("test content"))))

			assert.ErrorIs(t,
				db.Apply(
					&test.ChangeAttachPayload{PayloadID: "123"},
					file.NewPayload("123", strings.NewReader("test content 2"))),
				file.ErrPayloadIDAlreadyExists)

			assert.Equal(t, `attach-payload {"payloadID":"123"}
`,
				readFile(t, filepath.Join(path, file.FileNameLog)))
			assert.Equal(t, "test content", readFile(t, filepath.Join(path, file.FilePrefixPayload+"123")))
		})
	})

	t.Run("Encrypted", func(t *testing.T) {
		file.NonceFn = crypto.FixedNonceFn(testNonce)

		t.Run("Simple", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameLog),
				"RvHVkTLxL6w2NuIve4yZWuDoi235HjF4lypGHH9GbQWcgp9fh0yCTqCkya8bwp0HQQyAPg\n")

			db, err := file.OpenDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path, file.WithOpenKey(testKey))
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t, db.Apply(&test.ChangeCounterInc{Value: 123}))

			assert.Equal(t, 2, db.LogLen())
			assert.Equal(t, `RvHVkTLxL6w2NuIve4yZWuDoi235HjF4lypGHH9GbQWcgp9fh0yCTqCkya8bwp0HQQyAPg
AAAAAAAAAAAAAAAAKrnyPe3+1KGK5xlIG6PG/NXYTgwOW/ALLba+QxD4jkcJYOo99rU7+DA
`,
				readFile(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithPayload", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameLog),
				"RvHVkTLxL6w2NuIve4yZWuDoi235HjF4lypGHH9GbQWcgp9fh0yCTqCkya8bwp0HQQyAPg\n")

			db, err := file.OpenDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path, file.WithOpenKey(testKey))
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&test.ChangeAttachPayload{PayloadID: "123"},
					file.NewPayload("123", bytes.NewReader([]byte("test content")))))

			assert.Equal(t, 2, db.LogLen())
			assert.Equal(t, `RvHVkTLxL6w2NuIve4yZWuDoi235HjF4lypGHH9GbQWcgp9fh0yCTqCkya8bwp0HQQyAPg
AAAAAAAAAAAAAAAAKKLzMvrzi/yC8BYHAeWQ5pvdSldYBaNcGRkUZL6GzmUSHoM0+S5nqVoaLW8WgkdwqwI
`,
				readFile(t, filepath.Join(path, file.FileNameLog)))

			assert.Equal(t,
				"AAAAAAAAAAAAAAAAHAA9s/QnufjJ4pfsFBxwlSca1DfNTgp6gIijaFQK",
				readFileBase64(t, filepath.Join(path, file.FilePrefixPayload+"123")))
		})
	})
}

// func TestFileDatabaseOpenPayload(t *testing.T) {
// 	t.Run("Plain", func(t *testing.T) {
// 		path, removeDir := makeTempDir(t)
// 		defer removeDir()

// 		db, err := model.CreateFileDatabase(path)
// 		require.NoError(t, err)
// 		defer db.Close()

// 		require.NoError(t,
// 			db.Apply(
// 				&testAttachPayloadChange{Name: "one", PayloadID: "123"},
// 				tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

// 		file, err := db.OpenPayload("123")
// 		require.NoError(t, err)

// 		content, err := io.ReadAll(file)
// 		require.NoError(t, err)
// 		assert.Equal(t, "test content", string(content))

// 		require.NoError(t, file.Close())
// 	})

// 	t.Run("Encrypted", func(t *testing.T) {
// 		path, removeDir := makeTempDir(t)
// 		defer removeDir()

// 		db, err := model.CreateFileDatabase(path, tapedb.WithCreateKey(testKey))
// 		require.NoError(t, err)
// 		defer db.Close()

// 		require.NoError(t,
// 			db.Apply(
// 				&testAttachPayloadChange{Name: "one", PayloadID: "123"},
// 				tapedb.NewPayload("123", bytes.NewReader([]byte("test content")))))

// 		file, err := db.OpenPayload("123")
// 		require.NoError(t, err)

// 		content, err := io.ReadAll(file)
// 		require.NoError(t, err)
// 		assert.Equal(t, "test content", string(content))

// 		require.NoError(t, file.Close())
// 	})
// }

// func TestFileDatabaseSplice(t *testing.T) {
// 	t.Run("FromPlainToPlain", func(t *testing.T) {
// 		t.Run("NoFile", func(t *testing.T) {
// 			path, removeDir := makeTempDir(t)
// 			defer removeDir()

// 			require.NoError(t,
// 				model.SpliceFileDatabase(path))

// 			assert.Equal(t, `
// {}
// `,
// 				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
// 		})

// 		t.Run("WithBaseAndChanges", func(t *testing.T) {
// 			path, removeDir := makeTempDir(t)
// 			defer removeDir()

// 			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
// {"items":["one","two","three"]}
// addItem {"name":"four"}
// removeItem {"name":"two"}
// `)

// 			require.NoError(t,
// 				model.SpliceFileDatabase(path))

// 			assert.Equal(t, `
// {"items":["one","two","three"]}
// addItem {"name":"four"}
// removeItem {"name":"two"}
// `,
// 				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
// 		})

// 		t.Run("WithPayloads", func(t *testing.T) {
// 			path, removeDir := makeTempDir(t)
// 			defer removeDir()

// 			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
// {"items":["one","two"],"payloads":{"123":"one"}}
// detachPayload {"name":"one","payloadID":"123"}
// attachPayload {"name":"two","payloadID":"456"}
// `)
// 			makeFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"123"), "test content")
// 			makeFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"456"), "test content")

// 			require.NoError(t,
// 				model.SpliceFileDatabase(path, tapedb.WithConsumeChanges(1)))

// 			assert.False(t, existFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"123")))
// 			assert.True(t, existFile(t, filepath.Join(path, tapedb.FilePrefixPayload+"456")))
// 		})

// 		t.Run("WithChangeConsumed", func(t *testing.T) {
// 			path, removeDir := makeTempDir(t)
// 			defer removeDir()

// 			makeFile(t, filepath.Join(path, tapedb.FileNameDatabase), `
// {"items":["one","two","three"]}
// addItem {"name":"four"}
// removeItem {"name":"two"}
// `)

// 			require.NoError(t,
// 				model.SpliceFileDatabase(path, tapedb.WithConsumeChanges(1)))

// 			assert.Equal(t, `
// {"items":["one","two","three","four"]}
// removeItem {"name":"two"}
// `,
// 				readFile(t, filepath.Join(path, tapedb.FileNameDatabase)))
// 		})
// 	})
// }