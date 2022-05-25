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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2/io/file"
	"github.com/simia-tech/tapedb/v2/test"
)

func TestDeck(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		deck, err := file.NewDeck[*test.Base, *test.State, *test.Factory](2)
		require.NoError(t, err)
		defer deck.Close()

		testFactory := test.NewFactory()

		require.NoError(t, deck.Create(testFactory, path))
		assert.Equal(t, 1, deck.Len())

		assert.ErrorIs(t, deck.Create(testFactory, path), file.ErrExisting)
		assert.Equal(t, 1, deck.Len())

		require.NoError(t, deck.Create(testFactory, path+"/a"))
		assert.Equal(t, 2, deck.Len())

		require.NoError(t, deck.Create(testFactory, path+"/b"))
		assert.Equal(t, 2, deck.Len())
	})

	t.Run("Delete", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		deck, err := file.NewDeck[*test.Base, *test.State, *test.Factory](2)
		require.NoError(t, err)
		defer deck.Close()

		testFactory := test.NewFactory()

		require.NoError(t, deck.Create(testFactory, path))
		assert.Equal(t, 1, deck.Len())

		require.NoError(t, deck.Delete(path))
		assert.Equal(t, 0, deck.Len())
	})

	t.Run("ReadMeta", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		deck, err := file.NewDeck[*test.Base, *test.State, *test.Factory](2)
		require.NoError(t, err)
		defer deck.Close()

		testFactory := test.NewFactory()

		require.NoError(t, deck.Create(testFactory, path, file.WithMeta(file.Meta{"Test": []string{"Value"}})))
		assert.Equal(t, 1, deck.Len())

		meta, err := deck.ReadMeta(path)
		require.NoError(t, err)
		assert.Equal(t, file.Meta{"Test": []string{"Value"}}, meta)
	})

	t.Run("WithOpen", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path, file.WithCreateKey(testKey))
		require.NoError(t, err)
		require.NoError(t, db.Close())

		deck, err := file.NewDeck[*test.Base, *test.State, *test.Factory](2)
		require.NoError(t, err)
		defer deck.Close()

		testFactory := test.NewFactory()

		err = deck.WithOpen(testFactory, path, []file.OpenOption{file.WithOpenKey(testKey)}, func(db *file.Database[*test.Base, *test.State]) error {
			return nil
		})
		assert.NoError(t, err)

		err = deck.WithOpen(testFactory, path, []file.OpenOption{file.WithOpenKey(testKey)}, func(db *file.Database[*test.Base, *test.State]) error {
			return nil
		})
		assert.NoError(t, err)

		err = deck.WithOpen(testFactory, path, []file.OpenOption{file.WithOpenKey(testInvalidKey)}, func(db *file.Database[*test.Base, *test.State]) error {
			return nil
		})
		assert.ErrorIs(t, err, file.ErrInvalidKey)
	})

	t.Run("Splice", func(t *testing.T) {
		path, removeDir := makeTempDir(t)
		defer removeDir()

		db, err := file.CreateDatabase[*test.Base, *test.State, *test.Factory](test.NewFactory(), path, file.WithCreateKey(testKey))
		require.NoError(t, err)
		require.NoError(t, db.Close())

		deck, err := file.NewDeck[*test.Base, *test.State, *test.Factory](2)
		require.NoError(t, err)
		defer deck.Close()

		testFactory := test.NewFactory()

		require.NoError(t, deck.WithOpen(testFactory, path, []file.OpenOption{file.WithOpenKey(testKey)}, func(db *file.Database[*test.Base, *test.State]) error {
			return db.Apply(&test.ChangeCounterInc{Value: 21})
		}))

		require.NoError(t,
			deck.Splice(testFactory, path, file.WithSourceKey(testKey), file.WithRebaseChangeCount(1)))

		logLen := 0
		require.NoError(t, deck.WithOpen(testFactory, path, []file.OpenOption{}, func(db *file.Database[*test.Base, *test.State]) error {
			logLen = db.LogLen()
			return nil
		}))
		assert.Equal(t, 0, logLen)
	})
}
