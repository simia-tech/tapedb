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
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh/terminal"

	"github.com/simia-tech/tapedb/v2/io/file"
)

func fetchKey(path string) ([]byte, error) {
	password, err := promptPassword()
	if err != nil {
		return nil, err
	}
	if password == "" {
		return nil, nil
	}

	keyFunc := file.DeriveKeyFrom(password, file.DefaultCryptSettings)
	if keyFunc == nil {
		return nil, nil
	}

	meta := file.Meta{}
	metaPath := filepath.Join(path, file.FileNameMeta)
	metaF, err := os.OpenFile(metaPath, os.O_RDONLY, 0)
	if err == nil {
		m, err := file.ReadMeta(metaF)
		if err != nil {
			return nil, fmt.Errorf("read meta: %w", err)
		}
		meta = m
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("open meta %s: %w", metaPath, err)
	}

	key, err := keyFunc(meta)
	if err != nil {
		return nil, err
	}

	return key, nil
}

func promptPassword() (string, error) {
	fmt.Printf("Password: ")
	password, err := terminal.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	return string(password), err
}
