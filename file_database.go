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

package tapedb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/simia-tech/tapedb/v2/chunkio"
)

var (
	ErrDatabaseMissing = errors.New("database missing")
	ErrDatabaseExists  = errors.New("database exists")
)

type FileDatabase struct {
	path     string
	fileMode fs.FileMode
	db       *Database
	changesC io.Closer
}

func (m *Model) CreateFileDatabase(path string, opts ...CreateOption) (*FileDatabase, error) {
	options := defaultCreateOptions
	for _, opt := range opts {
		opt(&options)
	}

	if err := os.MkdirAll(path, options.directoryMode); err != nil {
		return nil, fmt.Errorf("make directory: %w", err)
	}

	databasePath := filepath.Join(path, FileNameDatabase)
	databaseF, err := os.OpenFile(databasePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_SYNC, options.fileMode)
	if err != nil {
		if os.IsExist(err) {
			return nil, fmt.Errorf("create %s: %w", databasePath, ErrDatabaseExists)
		}
		return nil, err
	}

	db, err := m.CreateDatabase(databaseF, opts...)
	if err != nil {
		return nil, err
	}

	return &FileDatabase{
		path:     path,
		fileMode: options.fileMode,
		db:       db,
		changesC: databaseF,
	}, nil
}

func (m *Model) OpenFileDatabase(path string, opts ...OpenOption) (*FileDatabase, error) {
	options := defaultOpenOptions
	for _, opt := range opts {
		opt(&options)
	}

	databasePath := filepath.Join(path, FileNameDatabase)
	databaseF, err := os.OpenFile(databasePath, os.O_RDWR|os.O_SYNC, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("open %s: %w", databasePath, ErrDatabaseMissing)
		}
		return nil, err
	}
	fileMode := fs.FileMode(0644)
	if stat, err := databaseF.Stat(); err == nil {
		fileMode = stat.Mode()
	}

	db, err := m.OpenDatabase(databaseF, opts...)
	if err != nil {
		return nil, err
	}

	return &FileDatabase{
		path:     path,
		fileMode: fileMode,
		db:       db,
		changesC: databaseF,
	}, nil
}

func (m *Model) ReadFileDatabaseHeader(path string) (Header, error) {
	databasePath := filepath.Join(path, FileNameDatabase)
	databaseF, err := os.OpenFile(databasePath, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("open %s: %w", databasePath, ErrDatabaseMissing)
		}
		return nil, err
	}

	header, _, err := ReadHeader(databaseF)
	if err != nil {
		return nil, err
	}

	if err := databaseF.Close(); err != nil {
		return nil, err
	}

	return header, nil
}

func (m *Model) SpliceFileDatabase(path string, opts ...SpliceOption) error {
	options := defaultSpliceOptions
	for _, opt := range opts {
		opt(&options)
	}

	fileMode := fs.FileMode(0644)
	databaseRC := io.ReadCloser(nil)
	databasePath := filepath.Join(path, FileNameDatabase)
	if f, err := os.OpenFile(databasePath, os.O_RDONLY, 0); err == nil {
		databaseRC = f
		if stat, err := f.Stat(); err == nil {
			fileMode = stat.Mode()
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	newDatabasePath := filepath.Join(path, FileNameNewDatabase)
	newDatabase, err := os.OpenFile(newDatabasePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_SYNC, fileMode)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("create %s: %w", newDatabasePath, ErrDatabaseExists)
		}
		return err
	}

	payloadIDs, err := m.SpliceDatabase(newDatabase, databaseRC, opts...)
	if err != nil {
		return err
	}

	if databaseRC != nil {
		if err := databaseRC.Close(); err != nil {
			return err
		}
	}
	if err := newDatabase.Close(); err != nil {
		return err
	}

	if err := m.deleteUnreferencedPayloads(path, payloadIDs); err != nil {
		return err
	}

	if err := os.Remove(databasePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(newDatabasePath, databasePath); err != nil {
		return err
	}

	return nil
}

func (m *Model) deleteUnreferencedPayloads(path string, ids []string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if name := entry.Name(); strings.HasPrefix(name, FilePrefixPayload) {
			id := strings.TrimPrefix(name, FilePrefixPayload)
			if !stringsContain(ids, id) {
				if err := os.Remove(filepath.Join(path, entry.Name())); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (db *FileDatabase) Close() error {
	if err := db.changesC.Close(); err != nil {
		return err
	}
	return nil
}

func (db *FileDatabase) Header() Header {
	return db.db.Header()
}

func (db *FileDatabase) State() State {
	return db.db.State()
}

func (db *FileDatabase) ChangesCount() int {
	return db.db.ChangesCount()
}

func (db *FileDatabase) Apply(change Change, payloads ...Payload) error {
	for _, payload := range payloads {
		f, err := os.OpenFile(db.payloadPath(payload.id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, db.fileMode)
		if err != nil {
			if os.IsExist(err) {
				return fmt.Errorf("create payload with id %s: %w", payload.id, ErrPayloadIDAlreadyExists)
			}
			return err
		}

		if db.db.key == nil {
			if _, err := io.Copy(f, payload.r); err != nil {
				return err
			}
		} else {
			c, err := chunkio.NewAESCrypter(db.db.key, RandomNonce())
			if err != nil {
				return err
			}

			w := chunkio.NewAESStreamWriter(f, c)

			if _, err := io.Copy(w, payload.r); err != nil {
				return err
			}

			if err := w.Flush(); err != nil {
				return err
			}
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	return db.db.Apply(change)
}

func (db *FileDatabase) OpenPayload(id string) (io.ReadCloser, error) {
	path := db.payloadPath(id)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPayloadMissing
		}
		return nil, err
	}

	if db.db.key == nil {
		return f, nil
	}

	c, err := chunkio.NewAESCrypter(db.db.key, []byte{})
	if err != nil {
		return nil, err
	}

	rc := chunkio.NewAESStreamReadCloser(f, c)

	return rc, nil
}

func (db *FileDatabase) payloadPath(id string) string {
	return filepath.Join(db.path, FilePrefixPayload+id)
}

func appendPayloadIDs(ids []string, container interface{}) []string {
	if c, ok := container.(PayloadContainer); ok {
		return append(ids, c.PayloadIDs()...)
	}
	return ids
}

func stringsContain(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
