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
	"io"
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

		db, err := file.CreateDatabase(test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 0, db.LogLen())
		assert.Equal(t, 0, db.State().Counter)
	})

	t.Run("ErrorOnExisting", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, file.FileNameLog), "test")

		db, err := file.CreateDatabase(test.NewFactory(), path)
		require.Nil(t, db)
		assert.ErrorIs(t, err, file.ErrExisting)
	})

	t.Run("Encrypted", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase(test.NewFactory(), path, file.WithCreateKey(testKey))
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(&test.ChangeCounterInc{Value: 21}))
	})
}

func TestOpenDatabase(t *testing.T) {
	t.Run("NoFile", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.OpenDatabase(test.NewFactory(), path)
		require.Nil(t, db)
		assert.ErrorIs(t, err, file.ErrMissing)
	})

	t.Run("WithBase", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":3}`)

		db, err := file.OpenDatabase(test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 0, db.LogLen())
		assert.Equal(t, 3, db.State().Counter)
	})

	t.Run("WithBaseAndLog", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":3}`)
		makeFile(t, filepath.Join(path, file.FileNameLog),
			"\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n")

		db, err := file.OpenDatabase(test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 2, db.LogLen())
		assert.Equal(t, 6, db.State().Counter)
	})

	t.Run("WithEncryptedLog", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		makeFileBase64(t, filepath.Join(path, file.FileNameLog),
			"EAAANQAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDljzRVpqg0Xg5O3gChdsGaHUeOdn")

		_, err := file.OpenDatabase(test.NewFactory(), path, file.WithOpenKey(testInvalidKey))
		assert.ErrorIs(t, err, file.ErrInvalidKey)

		db, err := file.OpenDatabase(test.NewFactory(), path, file.WithOpenKey(testKey))
		require.NoError(t, err)
		defer db.Close()

		assert.Equal(t, 1, db.LogLen())
		assert.Equal(t, 21, db.State().Counter)
	})
}

func TestDatabaseApply(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		t.Run("Simple", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), "{}")
			makeFile(t, filepath.Join(path, file.FileNameLog), "\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n")

			db, err := file.OpenDatabase(test.NewFactory(), path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(&test.ChangeCounterInc{Value: 21}))

			assert.Equal(t, 2, db.LogLen())
			assert.Equal(t, "\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n\x00\x00\x00\x19\x0bcounter-inc{\"value\":21}\n",
				readFile(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithPayload", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), "{}")
			makeFile(t, filepath.Join(path, file.FileNameLog), "\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n")

			db, err := file.OpenDatabase(test.NewFactory(), path)
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&test.ChangeAttachPayload{PayloadID: "123"},
					file.NewPayload("123", strings.NewReader("test content"))))

			assert.Equal(t,
				"\x00\x00\x00\x18\x0bcounter-inc{\"value\":1}\n\x00\x00\x00#\x0eattach-payload{\"payloadID\":\"123\"}\n",
				readFile(t, filepath.Join(path, file.FileNameLog)))
			assert.Equal(t, "test content", readFile(t, filepath.Join(path, "payload-123")))
		})

		t.Run("WithExistingPayloadID", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			db, err := file.CreateDatabase(test.NewFactory(), path)
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

			assert.Equal(t,
				"\x00\x00\x00#\x0eattach-payload{\"payloadID\":\"123\"}\n",
				readFile(t, filepath.Join(path, file.FileNameLog)))
			assert.Equal(t,
				"test content",
				readFile(t, filepath.Join(path, file.FilePrefixPayload+"123")))
		})
	})

	t.Run("Encrypted", func(t *testing.T) {
		file.NonceFn = crypto.FixedNonceFn(testNonce)

		t.Run("Simple", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFileBase64(t, filepath.Join(path, file.FileNameLog),
				"EAAANQAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDljzRVpqg0Xg5O3gChdsGaHUeOdn")

			db, err := file.OpenDatabase(test.NewFactory(), path, file.WithOpenKey(testKey))
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t, db.Apply(&test.ChangeCounterInc{Value: 2}))

			assert.Equal(t, 2, db.LogLen())
			assert.Equal(t,
				"EAAANQAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDljzRVpqg0Xg5O3gChdsGaHUeOdnEAAANAAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDli/MpzG8dh/UYqsEnrWaFYZLyk",
				readFileBase64(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithPayload", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFileBase64(t, filepath.Join(path, file.FileNameLog),
				"EAAANQAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDljzRVpqg0Xg5O3gChdsGaHUeOdn")

			db, err := file.OpenDatabase(test.NewFactory(), path, file.WithOpenKey(testKey))
			require.NoError(t, err)
			defer db.Close()

			require.NoError(t,
				db.Apply(
					&test.ChangeAttachPayload{PayloadID: "123"},
					file.NewPayload("123", bytes.NewReader([]byte("test content")))))

			assert.Equal(t, 2, db.LogLen())
			assert.Equal(t,
				"EAAANQAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDljzRVpqg0Xg5O3gChdsGaHUeOdnEAAAPwAAAAAAAAAAAAAAAEe38yf4+M6hk+gDBA/g1Oab3UpXWAWjXBkZFGS+hs5lEh68QYx4FT0OqHeetgD1F83q6Q",
				readFileBase64(t, filepath.Join(path, file.FileNameLog)))

			assert.Equal(t,
				"AAAAAAAAAAAAAAAAHAA9s/QnufjJ4pfsFBxwlSca1DfNTgp6gIijaFQK",
				readFileBase64(t, filepath.Join(path, file.FilePrefixPayload+"123")))
		})
	})
}

func TestDatabaseOpenPayload(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase(test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(
				&test.ChangeAttachPayload{PayloadID: "123"},
				file.NewPayload("123", strings.NewReader("test content"))))

		f, err := db.OpenPayload("123")
		require.NoError(t, err)

		content, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, "test content", string(content))

		require.NoError(t, f.Close())
	})

	t.Run("Encrypted", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase(test.NewFactory(), path, file.WithCreateKey(testKey))
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(
				&test.ChangeAttachPayload{PayloadID: "123"},
				file.NewPayload("123", strings.NewReader("test content"))))

		f, err := db.OpenPayload("123")
		require.NoError(t, err)

		content, err := io.ReadAll(f)
		require.NoError(t, err)
		assert.Equal(t, "test content", string(content))

		require.NoError(t, f.Close())
	})
}

func TestDatabaseStatPayload(t *testing.T) {
	t.Run("Plain", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase(test.NewFactory(), path)
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(
				&test.ChangeAttachPayload{PayloadID: "123"},
				file.NewPayload("123", strings.NewReader("test content"))))

		stat, err := db.StatPayload("123")
		require.NoError(t, err)
		assert.Equal(t, "payload-123", stat.Name())
		assert.Equal(t, int64(12), stat.Size())
	})

	t.Run("Encrypted", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase(test.NewFactory(), path, file.WithCreateKey(testKey))
		require.NoError(t, err)
		defer db.Close()

		require.NoError(t,
			db.Apply(
				&test.ChangeAttachPayload{PayloadID: "123"},
				file.NewPayload("123", strings.NewReader("test content"))))

		stat, err := db.StatPayload("123")
		require.NoError(t, err)
		assert.Equal(t, "payload-123", stat.Name())
		assert.Equal(t, int64(42), stat.Size())
	})
}

func TestDatabaseSplice(t *testing.T) {
	t.Run("FromPlainToPlain", func(t *testing.T) {
		t.Run("NoFile", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			require.NoError(t,
				file.SpliceDatabase(test.NewFactory(), path))

			assert.Equal(t, "{\"value\":0}\n", readFile(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t, "", readFile(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithBaseAndLog", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":21}`)
			makeFile(t, filepath.Join(path, file.FileNameLog), "\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n")

			require.NoError(t,
				file.SpliceDatabase(test.NewFactory(), path))

			assert.Equal(t, "{\"value\":21}\n", readFile(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t, "\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n", readFile(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithPayloads", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":21}`)
			makeFile(t, filepath.Join(path, file.FileNameLog), "\x00\x00\x00#\x0eattach-payload{\"payloadID\":\"456\"}\n")
			makeFile(t, filepath.Join(path, file.FilePrefixPayload+"123"), "test content")
			makeFile(t, filepath.Join(path, file.FilePrefixPayload+"456"), "test content")

			require.NoError(t,
				file.SpliceDatabase(test.NewFactory(), path))

			assert.NoFileExists(t, filepath.Join(path, file.FilePrefixPayload+"123"))
			assert.FileExists(t, filepath.Join(path, file.FilePrefixPayload+"456"))
		})

		t.Run("WithRebaseLogEntries", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":21}`)
			makeFile(t, filepath.Join(path, file.FileNameLog),
				"\x00\x00\x00\x18\x0bcounter-inc{\"value\":7}\n\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n")

			require.NoError(t,
				file.SpliceDatabase(
					test.NewFactory(), path, file.WithRebaseChangeCount(1)))

			assert.Equal(t, "{\"value\":28}\n", readFile(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t,
				"\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n",
				readFile(t, filepath.Join(path, file.FileNameLog)))
		})
	})

	t.Run("FromPlainToEncrypted", func(t *testing.T) {
		file.NonceFn = crypto.FixedNonceFn(testNonce)

		t.Run("NoFile", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			require.NoError(t,
				file.SpliceDatabase(test.NewFactory(), path, file.WithTargetKey(testKey)))

			assert.Equal(t,
				"AAAAAAAAAAAAAAAAHAAy9PEy9e7Drtm5B2Ih+wBioy9nEqoVlbSJnZT3",
				readFileBase64(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t, "", readFile(t, filepath.Join(path, file.FileNameLog)))
		})

		t.Run("WithBaseAndLog", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFile(t, filepath.Join(path, file.FileNameBase), `{"value":21}`)
			makeFile(t, filepath.Join(path, file.FileNameLog), "\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n")

			require.NoError(t,
				file.SpliceDatabase(test.NewFactory(), path, file.WithTargetKey(testKey)))

			assert.Equal(t,
				"AAAAAAAAAAAAAAAAHQAy9PEy9e7Drtm7SxVq+PKr/ubvzKL1RyiHE+zmiQ",
				readFileBase64(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t,
				"EAAANAAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDli/MpzG8dh/UYqsEnrWaFYZLyk",
				readFileBase64(t, filepath.Join(path, file.FileNameLog)))
		})
	})

	t.Run("FromEncryptedToPlain", func(t *testing.T) {
		file.NonceFn = crypto.FixedNonceFn(testNonce)

		t.Run("WithBaseAndLog", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFileBase64(t, filepath.Join(path, file.FileNameBase),
				"AAAAAAAAAAAAAAAAHQAy9PEy9e7Drtm7SxVq+PKr/ubvzKL1RyiHE+zmiQ")
			makeFileBase64(t, filepath.Join(path, file.FileNameLog),
				"EAAANAAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDli/MpzG8dh/UYqsEnrWaFYZLyk")

			require.NoError(t,
				file.SpliceDatabase(test.NewFactory(), path, file.WithSourceKey(testKey)))

			assert.Equal(t, "{\"value\":21}\n", readFile(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t, "\x00\x00\x00\x18\x0bcounter-inc{\"value\":2}\n", readFile(t, filepath.Join(path, file.FileNameLog)))
		})
	})

	t.Run("FromEncryptedToEncrypted", func(t *testing.T) {
		file.NonceFn = crypto.FixedNonceFn(testNonce)

		t.Run("WithBaseAndLog", func(t *testing.T) {
			path, removeDir := makeTempDir(t)
			defer removeDir()

			makeFileBase64(t, filepath.Join(path, file.FileNameBase),
				"AAAAAAAAAAAAAAAAHQAy9PEy9e7Drtm7SxVq+PKr/ubvzKL1RyiHE+zmiQ")
			makeFileBase64(t, filepath.Join(path, file.FileNameLog),
				"EAAANAAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDli/MpzG8dh/UYqsEnrWaFYZLyk")

			require.NoError(t,
				file.SpliceDatabase(
					test.NewFactory(),
					path,
					file.WithSourceKey(testKey), file.WithTargetKey(testKey)))

			assert.Equal(t,
				"AAAAAAAAAAAAAAAAHQAy9PEy9e7Drtm7SxVq+PKr/ubvzKL1RyiHE+zmiQ",
				readFileBase64(t, filepath.Join(path, file.FileNameBase)))
			assert.Equal(t,
				"EAAANAAAAAAAAAAAAAAAAEK16Cb378P+zuAUCxujxvzV2E4MDli/MpzG8dh/UYqsEnrWaFYZLyk",
				readFileBase64(t, filepath.Join(path, file.FileNameLog)))
		})
	})
}
