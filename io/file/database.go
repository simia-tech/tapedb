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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/simia-tech/tapedb/v2"
	tapeio "github.com/simia-tech/tapedb/v2/io"
	"github.com/simia-tech/tapedb/v2/io/crypto"
)

const (
	MetaFieldNonce = "Nonce"
)

var (
	ErrMissing    = errors.New("missing")
	ErrExisting   = errors.New("existing")
	ErrInvalidKey = errors.New("invalid key")
)

var NonceFn crypto.NonceFunc = crypto.RandomNonceFn()

type Database[B tapedb.Base, S tapedb.State] struct {
	path       string
	fileMode   fs.FileMode
	meta       Meta
	key        []byte
	db         *tapeio.Database[B, S]
	logCloseFn func() error
}

func CreateDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	path string,
	opts ...CreateOption,
) (*Database[B, S], error) {
	options := defaultCreateOptions
	for _, opt := range opts {
		opt(&options)
	}

	if err := os.MkdirAll(path, options.directoryMode); err != nil {
		return nil, fmt.Errorf("make directory: %w", err)
	}

	meta := options.metaFunc()

	key := []byte(nil)
	err := error(nil)
	if options.keyFunc != nil {
		key, err = options.keyFunc(meta)
		if err != nil {
			return nil, fmt.Errorf("derive key: %w", err)
		}
	}

	if len(meta) > 0 {
		metaPath := filepath.Join(path, FileNameMeta)
		metaF, err := os.OpenFile(metaPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_SYNC, options.fileMode)
		if os.IsExist(err) {
			return nil, fmt.Errorf("create meta %s: %w", metaPath, ErrExisting)
		}
		if err != nil {
			return nil, err
		}

		if _, err := meta.WriteTo(metaF); err != nil {
			return nil, err
		}
	}

	logPath := filepath.Join(path, FileNameLog)
	logF, err := os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_SYNC, options.fileMode)
	if os.IsExist(err) {
		return nil, fmt.Errorf("create log %s: %w", logPath, ErrExisting)
	}
	if err != nil {
		return nil, err
	}

	db := (*tapeio.Database[B, S])(nil)
	logCloseFn := logF.Close
	if len(key) == 0 {
		db, err = tapeio.NewDatabase[B, S, F](f, logF)
	} else {
		logWC, err := crypto.NewLineWriter(logF, key, NonceFn)
		if err != nil {
			return nil, err
		}

		logCloseFn = func() error {
			if err := logWC.Close(); err != nil {
				return err
			}
			return logF.Close()
		}

		db, err = tapeio.NewDatabase[B, S, F](f, logWC)
	}
	if err != nil {
		return nil, err
	}

	return &Database[B, S]{
		path:       path,
		fileMode:   options.fileMode,
		meta:       meta,
		db:         db,
		logCloseFn: logCloseFn,
	}, nil
}

func OpenDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](
	f F,
	path string,
	opts ...OpenOption,
) (*Database[B, S], error) {
	options := defaultOpenOptions
	for _, opt := range opts {
		opt(&options)
	}

	meta := Meta{}
	metaPath := filepath.Join(path, FileNameMeta)
	metaF, err := os.OpenFile(metaPath, os.O_RDONLY, 0)
	if err == nil {
		m, err := ReadMeta(metaF)
		if err != nil {
			return nil, fmt.Errorf("read meta: %w", err)
		}
		meta = m
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open meta %s: %w", metaPath, err)
	}

	basePath := filepath.Join(path, FileNameBase)
	baseF, _, err := mayOpenReadOnlyFile(basePath)
	if err != nil {
		return nil, fmt.Errorf("open base %s: %w", basePath, err)
	}
	baseR := io.Reader(nil)
	if baseF != nil {
		baseR = baseF
	}

	logPath := filepath.Join(path, FileNameLog)
	logF, err := os.OpenFile(logPath, os.O_RDWR|os.O_SYNC, 0644)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("open log %s: %w", logPath, err)
	}
	if baseF == nil && logF == nil {
		return nil, ErrMissing
	}
	fileMode := fs.FileMode(0644)
	if stat, err := logF.Stat(); err == nil {
		fileMode = stat.Mode()
	}
	logCloseFn := logF.Close
	logR := io.Reader(nil)
	logWC := io.WriteCloser(nil)
	if logF != nil {
		logR = logF
		logWC = logF
	}

	key := []byte(nil)
	if options.keyFunc != nil {
		key, err = options.keyFunc(meta)
		if err != nil {
			return nil, fmt.Errorf("derive key: %w", err)
		}
	}

	if len(key) > 0 {
		if baseR != nil {
			br, err := crypto.NewBlockReader(baseR, key)
			if err != nil {
				return nil, fmt.Errorf("new block reader: %w", err)
			}
			baseR = br
		}

		if logR != nil {
			lr, err := crypto.NewLineReader(logR, key)
			if err != nil {
				return nil, fmt.Errorf("new line reader: %w", err)
			}
			logR = lr
		}

		lw, err := crypto.NewLineWriter(logWC, key, NonceFn)
		if err != nil {
			return nil, fmt.Errorf("new line writer: %w", err)
		}
		logWC = lw

		logCloseFn = func() error {
			if err := logWC.Close(); err != nil {
				return err
			}
			return logF.Close()
		}
	}

	db, err := tapeio.OpenDatabase[B, S, F](f, baseR, logR, logWC)
	if err != nil {
		if errors.Is(err, crypto.ErrInvalidKey) {
			return nil, ErrInvalidKey
		}
		return nil, err
	}

	return &Database[B, S]{
		path:       path,
		fileMode:   fileMode,
		meta:       meta,
		key:        key,
		db:         db,
		logCloseFn: logCloseFn,
	}, nil
}

func (db *Database[B, S]) Base() B {
	return db.db.Base()
}

func (db *Database[B, S]) State() S {
	return db.db.State()
}

func (db *Database[B, S]) Close() error {
	if err := db.logCloseFn(); err != nil {
		return err
	}
	return nil
}

func (db *Database[B, S]) Meta() Meta {
	return db.meta
}

func (db *Database[B, S]) Key() []byte {
	return db.key
}

func (db *Database[B, S]) LogLen() int {
	return db.db.LogLen()
}

func (db *Database[B, S]) Apply(change tapedb.Change, payloads ...Payload) error {
	for _, payload := range payloads {
		f, err := os.OpenFile(db.payloadPath(payload.id), os.O_CREATE|os.O_EXCL|os.O_WRONLY, db.fileMode)
		if err != nil {
			if os.IsExist(err) {
				return fmt.Errorf("create payload with id %s: %w", payload.id, ErrPayloadIDAlreadyExists)
			}
			return err
		}

		if len(db.key) == 0 {
			if _, err := io.Copy(f, payload.r); err != nil {
				return err
			}
		} else {
			wc, err := crypto.NewBlockWriter(f, db.key, NonceFn)
			if err != nil {
				return fmt.Errorf("new block writer: %w", err)
			}

			if _, err := io.Copy(wc, payload.r); err != nil {
				return err
			}

			if err := wc.Close(); err != nil {
				return err
			}
		}

		if err := f.Close(); err != nil {
			return err
		}
	}

	return db.db.Apply(change)
}

func (db *Database[B, S]) OpenPayload(id string) (io.ReadCloser, error) {
	path := db.payloadPath(id)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPayloadMissing
		}
		return nil, err
	}

	if len(db.key) == 0 {
		return f, nil
	}

	r, err := crypto.NewBlockReader(f, db.key)
	if err != nil {
		return nil, err
	}

	return tapeio.NewReadCloser(r, f.Close), nil
}

func (db *Database[B, S]) StatPayload(id string) (fs.FileInfo, error) {
	path := db.payloadPath(id)

	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPayloadMissing
		}
		return nil, err
	}

	return stat, nil
}

func (db *Database[B, S]) payloadPath(id string) string {
	return filepath.Join(db.path, FilePrefixPayload+id)
}

func SpliceDatabase[
	B tapedb.Base,
	S tapedb.State,
	F tapedb.Factory[B, S],
](f F, path string, opts ...SpliceOption) error {
	options := defaultSpliceOptions
	for _, opt := range opts {
		opt(&options)
	}

	meta := Meta{}
	// metaFileMode := fs.FileMode(0644)
	metaPath := filepath.Join(path, FileNameMeta)
	if f, err := os.OpenFile(metaPath, os.O_RDONLY, 0); err == nil {
		// if stat, err := f.Stat(); err == nil {
		// 	metaFileMode = stat.Mode()
		// }
		m, err := ReadMeta(f)
		if err != nil {
			return fmt.Errorf("read meta: %w", err)
		}
		meta = m
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	basePath := filepath.Join(path, FileNameBase)
	baseF, baseFileMode, err := mayOpenReadOnlyFile(basePath)
	if err != nil {
		return err
	}
	baseR := io.Reader(nil)
	if baseF != nil {
		baseR = baseF
	}

	logPath := filepath.Join(path, FileNameLog)
	logF, logFileMode, err := mayOpenReadOnlyFile(logPath)
	if err != nil {
		return err
	}
	logR := io.Reader(nil)
	if logF != nil {
		logR = logF
	}

	sourceKey := []byte(nil)
	if options.sourceKeyFunc != nil {
		key, err := options.sourceKeyFunc(meta)
		if err != nil {
			return fmt.Errorf("derive source key: %w", err)
		}
		sourceKey = key
	}

	if len(sourceKey) > 0 {
		br, err := crypto.NewBlockReader(baseR, sourceKey)
		if err != nil {
			return fmt.Errorf("new block reader: %w", err)
		}
		baseR = br

		lr, err := crypto.NewLineReader(logR, sourceKey)
		if err != nil {
			return fmt.Errorf("new line reader: %w", err)
		}
		logR = lr
	}

	newBasePath := filepath.Join(path, FileNameNewBase)
	newBaseF, err := createWriteOnlyFile(newBasePath, baseFileMode)
	if err != nil {
		return fmt.Errorf("create base %s: %w", newBasePath, ErrExisting)
	}
	newBaseWC := io.WriteCloser(newBaseF)

	newLogPath := filepath.Join(path, FileNameNewLog)
	newLogF, err := createWriteOnlyFile(newLogPath, logFileMode)
	if err != nil {
		return fmt.Errorf("create log %s: %w", newLogPath, ErrExisting)
	}
	newLogWC := io.WriteCloser(newLogF)

	targetKey := []byte(nil)
	if options.targetKeyFunc != nil {
		key, err := options.targetKeyFunc(meta)
		if err != nil {
			return fmt.Errorf("derive target key: %w", err)
		}
		targetKey = key
	}

	if len(targetKey) > 0 {
		bw, err := crypto.NewBlockWriter(newBaseWC, targetKey, NonceFn)
		if err != nil {
			return fmt.Errorf("new block writer: %w", err)
		}
		newBaseWC = bw

		lw, err := crypto.NewLineWriter(newLogWC, targetKey, NonceFn)
		if err != nil {
			return fmt.Errorf("new line writer: %w", err)
		}
		newLogWC = lw
	}

	payloadIDs := []string{}
	baseOrChangeWrittenFn := func(boc any) error {
		if c, ok := boc.(PayloadContainer); ok {
			payloadIDs = append(payloadIDs, c.PayloadIDs()...)
		}
		return nil
	}

	if err := tapeio.SpliceDatabase[B, S, F](f, newBaseWC, newLogWC, baseR, logR, options.rebaseLogEntries, baseOrChangeWrittenFn); err != nil {
		return err
	}

	if baseF != nil {
		if err := baseF.Close(); err != nil {
			return err
		}
	}
	if err := newBaseWC.Close(); err != nil {
		return err
	}
	newBaseF.Close() // ignore the error since the file might be already closed

	if logF != nil {
		if err := logF.Close(); err != nil {
			return err
		}
	}
	if err := newLogWC.Close(); err != nil {
		return err
	}
	newLogF.Close() // ignore the error since the file might be already closed

	if err := deleteUnreferencedPayloads(path, payloadIDs); err != nil {
		return err
	}

	if err := os.Remove(basePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(newBasePath, basePath); err != nil {
		return err
	}

	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(newLogPath, logPath); err != nil {
		return err
	}

	return nil
}

func deleteUnreferencedPayloads(path string, ids []string) error {
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

func stringsContain(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func mayOpenReadOnlyFile(path string) (*os.File, fs.FileMode, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return nil, 0644, nil
	}
	if err != nil {
		return nil, 0644, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, 0644, err
	}
	return f, stat.Mode(), nil
}

func createWriteOnlyFile(path string, mode fs.FileMode) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_SYNC, mode)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrExisting
		}
		return nil, err
	}
	return f, nil
}
