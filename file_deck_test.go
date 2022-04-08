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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2"
)

func TestFileDeck(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		deck, err := model.NewFileDeck(2)
		require.NoError(t, err)
		defer deck.Close()

		require.NoError(t, deck.Create(path))
		assert.Equal(t, 1, deck.Len())

		assert.ErrorIs(t, deck.Create(path), tapedb.ErrDatabaseExists)
		assert.Equal(t, 1, deck.Len())

		require.NoError(t, deck.Create(path+"/a"))
		assert.Equal(t, 2, deck.Len())

		require.NoError(t, deck.Create(path+"/b"))
		assert.Equal(t, 2, deck.Len())
	})

	t.Run("Delete", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		deck, err := model.NewFileDeck(2)
		require.NoError(t, err)
		defer deck.Close()

		require.NoError(t, deck.Create(path))
		assert.Equal(t, 1, deck.Len())

		require.NoError(t, deck.Delete(path))
		assert.Equal(t, 0, deck.Len())
	})

	t.Run("ReadHeader", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		deck, err := model.NewFileDeck(2)
		require.NoError(t, err)
		defer deck.Close()

		require.NoError(t, deck.Create(path, tapedb.WithHeader(tapedb.Header{"Test": []string{"Value"}})))
		assert.Equal(t, 1, deck.Len())

		header, err := deck.ReadHeader(path)
		require.NoError(t, err)
		assert.Equal(t, tapedb.Header{"Test": []string{"Value"}}, header)
	})

	t.Run("WithOpen", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := model.CreateFileDatabase(path, tapedb.WithCreateKey(testKey))
		require.NoError(t, err)
		require.NoError(t, db.Close())

		deck, err := model.NewFileDeck(2)
		require.NoError(t, err)
		defer deck.Close()

		err = deck.WithOpen(path, []tapedb.OpenOption{tapedb.WithOpenKey(testKey)}, func(db *tapedb.FileDatabase) error {
			return nil
		})
		require.NoError(t, err)
	})
}
