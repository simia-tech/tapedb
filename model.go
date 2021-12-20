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
	"reflect"
	"sync"
)

var (
	ErrUnknownType = errors.New("unknown type")
)

type Model struct {
	basefactory   BaseFactoryFunc
	stateFactory  StateFactoryFunc
	changeFactory ChangeFactoryFunc
}

func NewModel(bf BaseFactoryFunc, sf StateFactoryFunc, cf ChangeFactoryFunc) *Model {
	return &Model{
		basefactory:   bf,
		stateFactory:  sf,
		changeFactory: cf,
	}
}

func (m *Model) newBase() (Base, error) {
	return m.basefactory()
}

func (m *Model) newState(base Base, readLocker sync.Locker) (State, error) {
	return m.stateFactory(base, readLocker)
}

func (m *Model) newChange(typeName string) (Change, error) {
	return m.changeFactory(typeName)
}

func newInstance(prototype interface{}) interface{} {
	prototypeValue := reflect.ValueOf(prototype)
	if prototypeValue.Kind() == reflect.Ptr {
		prototypeValue = prototypeValue.Elem()
	}
	return reflect.New(prototypeValue.Type()).Interface()
}
