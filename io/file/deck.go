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

package file

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"

	lru "github.com/hashicorp/golang-lru"

	"github.com/simia-tech/tapedb/v2"
)

type Deck[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
] struct {
	databases      *lru.Cache
	databasesMutex sync.RWMutex
}

func NewDeck[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](openDatabaseLimit int) (*Deck[B, S, F], error) {
	databases, err := lru.New(openDatabaseLimit)
	if err != nil {
		return nil, err
	}

	return &Deck[B, S, F]{
		databases: databases,
	}, nil
}

func (d *Deck[B, S, F]) Close() error {
	d.databasesMutex.Lock()
	defer d.databasesMutex.Unlock()

	for _, value, ok := d.databases.RemoveOldest(); ok; _, value, ok = d.databases.RemoveOldest() {
		entry := value.(*entry[B, S])

		entry.dbMutex.Lock()
		err := entry.db.Close()
		entry.dbMutex.Unlock()

		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Deck[B, S, F]) Len() int {
	d.databasesMutex.RLock()
	l := d.databases.Len()
	d.databasesMutex.RUnlock()
	return l
}

func (d *Deck[B, S, F]) Create(f F, path string, opts ...CreateOption) error {
	d.databasesMutex.Lock()
	defer d.databasesMutex.Unlock()

	db, err := CreateDatabase[B, S](f, path, opts...)
	if err != nil {
		return err
	}

	d.databases.Add(path, &entry[B, S]{db: db})

	return nil
}

func (d *Deck[B, S, F]) Delete(path string) error {
	d.databasesMutex.Lock()
	defer d.databasesMutex.Unlock()

	if value, ok := d.databases.Get(path); ok {
		entry := value.(*entry[B, S])

		entry.dbMutex.Lock()
		err := entry.db.Close()
		entry.dbMutex.Unlock()

		if err != nil {
			return err
		}
	}

	if err := os.RemoveAll(path); err != nil {
		return err
	}

	d.databases.Remove(path)

	return nil
}

func (d *Deck[B, S, F]) Meta(path string) (Meta, error) {
	d.databasesMutex.RLock()

	if value, ok := d.databases.Get(path); ok {
		meta := value.(*entry[B, S]).db.Meta()
		d.databasesMutex.RUnlock()
		return meta, nil
	}

	d.databasesMutex.RUnlock()

	return ReadMetaFile(filepath.Join(path, FileNameMeta))
}

func (d *Deck[B, S, F]) SetMeta(path string, meta Meta) error {
	d.databasesMutex.Lock()
	defer d.databasesMutex.Unlock()

	if value, ok := d.databases.Get(path); ok {
		if err := value.(*entry[B, S]).db.SetMeta(meta); err != nil {
			return err
		}
	}

	return WriteMetaFile(filepath.Join(path, FileNameMeta), meta)
}

func (d *Deck[B, S, F]) LogLen(path string) (int, error) {
	d.databasesMutex.RLock()

	if value, ok := d.databases.Get(path); ok {
		logLen := value.(*entry[B, S]).db.LogLen()
		d.databasesMutex.RUnlock()
		return logLen, nil
	}

	d.databasesMutex.RUnlock()

	return ReadLogLen(filepath.Join(path, FileNameLog))
}

func (d *Deck[B, S, F]) Open(f F, path string, opts []OpenOption) (*Database[B, S], func(), error) {
	d.databasesMutex.Lock()

	value, ok := d.databases.Get(path)
	if !ok {
		db, err := OpenDatabase[B, S](f, path, opts...)
		if err != nil {
			d.databasesMutex.Unlock()
			return nil, nil, err
		}
		value = &entry[B, S]{db: db}
		d.databases.Add(path, value)
	}
	entry := value.(*entry[B, S])

	key, err := deriveKey(opts, entry.db.Meta())
	if err != nil {
		d.databasesMutex.Unlock()
		return nil, nil, err
	}
	if !bytes.Equal(entry.db.Key(), key) {
		d.databasesMutex.Unlock()
		return nil, nil, ErrInvalidKey
	}
	entry.dbMutex.Lock()

	d.databasesMutex.Unlock()

	return entry.db, func() {
		entry.dbMutex.Unlock()
	}, nil
}

func (d *Deck[B, S, F]) WithOpen(f F, path string, opts []OpenOption, fn func(*Database[B, S]) error) error {
	db, unlockFn, err := d.Open(f, path, opts)
	if err != nil {
		return err
	}
	defer unlockFn()

	return fn(db)
}

func (d *Deck[B, S, F]) Splice(f F, path string, opts ...SpliceOption) error {
	d.databasesMutex.Lock()
	defer d.databasesMutex.Unlock()

	if value, ok := d.databases.Get(path); ok {
		e := value.(*entry[B, S])

		e.dbMutex.Lock()
		err := e.db.Close()
		e.dbMutex.Unlock()

		if err != nil {
			return err
		}

		d.databases.Remove(path)
	}

	if err := SpliceDatabase[B, S](f, path, opts...); err != nil {
		return err
	}

	return nil
}

type entry[B tapedb.Base, S tapedb.State] struct {
	db      *Database[B, S]
	dbMutex sync.Mutex
}

func deriveKey(opts []OpenOption, meta Meta) ([]byte, error) {
	options := defaultOpenOptions
	for _, opt := range opts {
		opt(&options)
	}

	if options.keyFunc != nil {
		return options.keyFunc(meta)
	}

	return nil, nil
}
