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
	"fmt"
)

type Change interface {
	TypeName() string
}

type ChangeFactoryFunc func(string) (Change, error)

func PrototypeChangeFactory(prototypes ...Change) ChangeFactoryFunc {
	pm := map[string]Change{}
	for _, prototype := range prototypes {
		pm[prototype.TypeName()] = prototype
	}
	return func(typeName string) (Change, error) {
		prototype, ok := pm[typeName]
		if !ok {
			return nil, fmt.Errorf("new change of type %s: %w", typeName, ErrUnknownType)
		}
		return newInstance(prototype).(Change), nil
	}
}
