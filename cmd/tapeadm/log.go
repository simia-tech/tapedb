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

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"

	tapeio "github.com/simia-tech/tapedb/v2/io"
	"github.com/simia-tech/tapedb/v2/io/crypto"
	"github.com/simia-tech/tapedb/v2/io/file"
)

func logShow(path string, key []byte, follow bool) error {
	logPath := filepath.Join(path, file.FileNameLog)

	offset, err := logShowFile(logPath, key, 0)
	if err != nil {
		return err
	}

	if follow {
		for {
			offset, err = logWatchFile(logPath, key, offset)
			if errors.Is(err, errFileRemove) {
				fmt.Printf("log has been spliced\n")

				offset, err = logFileOffset(logPath)
				if err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

var errFileRemove = errors.New("file removed")

func logWatchFile(logPath string, key []byte, offset int64) (int64, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return 0, err
	}
	defer watcher.Close()

	errCh := make(chan error, 0)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					break
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					errCh <- errFileRemove
					break
				}
				if event.Op&fsnotify.Write != fsnotify.Write {
					continue
				}

				offset, err = logShowFile(logPath, key, offset)
				if err != nil {
					errCh <- err
					break
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					break
				}
				errCh <- fmt.Errorf("watcher: %w", err)
				break
			}
		}
	}()

	if err := watcher.Add(logPath); err != nil {
		return 0, err
	}

	if err := <-errCh; err != nil {
		return 0, err
	}

	return offset, nil
}

func logShowFile(logPath string, key []byte, offset int64) (int64, error) {
	logF, err := os.OpenFile(logPath, os.O_RDONLY, 0)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("open log %s: %w", logPath, err)
	}
	if logF == nil {
		return 0, file.ErrMissing
	}
	defer logF.Close()

	offset, err = logF.Seek(offset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	logR := tapeio.LogReader(nil)
	if logF != nil {
		logR = tapeio.NewLogReader(logF)
	}

	logR, err = crypto.WrapLogReader(logR, key)
	if err != nil {
		return 0, err
	}

	err = tapeio.ReadLogEntries(logR, func(entry tapeio.LogEntry) error {
		switch entry.Type() {
		case tapeio.LogEntryTypeBinary:
			typeName, data, err := readChange(entry)
			if err != nil {
				return err
			}

			fmt.Print(typeName)
			fmt.Print(" ")
			fmt.Print(string(bytes.TrimSuffix(data, []byte("\n"))))

		case tapeio.LogEntryTypeAESGCMEncrypted:
			fmt.Printf("encrypted (AES-GCM)")

		}
		fmt.Println()
		return nil
	})
	if err != nil {
		return 0, err
	}

	offset, err = logF.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func logFileOffset(logPath string) (int64, error) {
	logF, err := os.OpenFile(logPath, os.O_RDONLY, 0)
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("open log %s: %w", logPath, err)
	}
	if logF == nil {
		return 0, file.ErrMissing
	}
	defer logF.Close()

	return logF.Seek(0, io.SeekEnd)
}

func readChange(entry tapeio.LogEntry) (string, []byte, error) {
	r, err := entry.Reader()
	if err != nil {
		return "", nil, fmt.Errorf("reader: %w", err)
	}

	sizeBytes := [1]byte{}
	if _, err := io.ReadFull(r, sizeBytes[:]); err != nil {
		return "", nil, fmt.Errorf("read type name size: %w", err)
	}
	size := sizeBytes[0]

	typeNameBytes := make([]byte, size)
	if _, err := io.ReadFull(r, typeNameBytes); err != nil {
		return "", nil, fmt.Errorf("read type name of size %d: %w", size, err)
	}
	typeName := string(typeNameBytes)

	data, err := io.ReadAll(r)
	if err != nil {
		return "", nil, fmt.Errorf("read all data: %w", err)
	}

	return typeName, data, nil
}
