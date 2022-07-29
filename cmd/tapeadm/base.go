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
	"io/ioutil"
	"os"

	"github.com/simia-tech/tapedb/v2/io/crypto"
	"github.com/simia-tech/tapedb/v2/io/file"
)

func baseShow(path string, key []byte) error {
	baseF, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("open base %s: %w", path, err)
	}
	if baseF == nil {
		return file.ErrMissing
	}
	defer baseF.Close()

	baseR, err := crypto.WrapBlockReader(baseF, key)
	if err != nil {
		return fmt.Errorf("new block reader: %w", err)
	}

	data, err := ioutil.ReadAll(baseR)
	if err != nil {
		return fmt.Errorf("read base: %w", err)
	}

	fmt.Println(string(data))

	return nil
}
