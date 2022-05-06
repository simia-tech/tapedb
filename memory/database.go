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

package memory

import (
	"github.com/simia-tech/tapedb/v2"
)

type Database[B tapedb.Base, S tapedb.State] struct {
	base  B
	state S
}

func NewDatabase[B tapedb.Base, S tapedb.State](f tapedb.Factory[B, S]) (tapedb.Database[B, S], error) {
	base := f.NewBase()
	state := f.NewState(base)
	return &Database[B, S]{
		base:  base,
		state: state,
	}, nil
}

func (db *Database[B, S]) Base() B {
	return db.base
}

func (db *Database[B, S]) State() S {
	return db.state
}

func (db *Database[B, S]) Apply(c tapedb.Change) error {
	return db.state.Apply(c)
}

func (db *Database[B, S]) Close() error {
	return nil
}
