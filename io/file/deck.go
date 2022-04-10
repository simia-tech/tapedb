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

// type FileDeck struct {
// 	model          *Model
// 	databases      *lru.Cache
// 	databasesMutex sync.RWMutex
// }

// func (m *Model) NewFileDeck(openDatabaseLimit int) (*FileDeck, error) {
// 	databases, err := lru.New(openDatabaseLimit)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &FileDeck{
// 		model:     m,
// 		databases: databases,
// 	}, nil
// }

// func (d *FileDeck) Close() error {
// 	d.databasesMutex.Lock()
// 	defer d.databasesMutex.Unlock()

// 	for _, value, ok := d.databases.RemoveOldest(); ok; _, value, ok = d.databases.RemoveOldest() {
// 		entry := value.(entry)
// 		entry.waitGroup.Wait()
// 		if err := entry.db.Close(); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func (d *FileDeck) Len() int {
// 	d.databasesMutex.RLock()
// 	l := d.databases.Len()
// 	d.databasesMutex.RUnlock()
// 	return l
// }

// func (d *FileDeck) Create(path string, opts ...CreateOption) error {
// 	d.databasesMutex.Lock()
// 	defer d.databasesMutex.Unlock()

// 	db, err := d.model.CreateFileDatabase(path, opts...)
// 	if err != nil {
// 		return err
// 	}

// 	d.databases.Add(path, entry{db: db})

// 	return nil
// }

// func (d *FileDeck) Delete(path string) error {
// 	d.databasesMutex.Lock()
// 	defer d.databasesMutex.Unlock()

// 	if value, ok := d.databases.Get(path); ok {
// 		entry := value.(entry)
// 		entry.waitGroup.Wait()
// 		if err := entry.db.Close(); err != nil {
// 			return err
// 		}
// 	}

// 	if err := os.RemoveAll(path); err != nil {
// 		return err
// 	}

// 	d.databases.Remove(path)

// 	return nil
// }

// func (d *FileDeck) ReadHeader(path string) (Header, error) {
// 	d.databasesMutex.RLock()

// 	if value, ok := d.databases.Get(path); ok {
// 		header := value.(entry).db.Header()
// 		d.databasesMutex.RUnlock()
// 		return header, nil
// 	}

// 	d.databasesMutex.RUnlock()

// 	return d.model.ReadFileDatabaseHeader(path)
// }

// func (d *FileDeck) WithOpen(path string, opts []OpenOption, fn func(*FileDatabase) error) error {
// 	d.databasesMutex.Lock()

// 	value, ok := d.databases.Get(path)
// 	if !ok {
// 		db, err := d.model.OpenFileDatabase(path, opts...)
// 		if err != nil {
// 			d.databasesMutex.Unlock()
// 			return err
// 		}
// 		value = entry{db: db}
// 		d.databases.Add(path, value)
// 	}
// 	entry := value.(entry)
// 	entry.waitGroup.Add(1)

// 	d.databasesMutex.Unlock()

// 	if err := fn(entry.db); err != nil {
// 		entry.waitGroup.Done()
// 		return err
// 	}
// 	entry.waitGroup.Done()

// 	return nil
// }

// type entry struct {
// 	db        *FileDatabase
// 	waitGroup sync.WaitGroup
// }
