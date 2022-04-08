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

package test

import (
	"fmt"

	"github.com/simia-tech/tapedb/v2"
)

type Factory struct{}

func NewFactory() tapedb.Factory[*Base, *State] {
	return &Factory{}
}

func (f *Factory) NewBase() *Base {
	return NewBase()
}

func (f *Factory) NewState(base *Base) *State {
	return NewState(base)
}

func (f *Factory) NewChange(typeName string) (tapedb.Change, error) {
	switch typeName {
	case "counter-inc":
		return &ChangeCounterInc{}, nil
	}
	return nil, fmt.Errorf("change type [%s]: %w", typeName, tapedb.ErrUnknownChangeType)
}
