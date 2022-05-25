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
	"io/fs"

	"github.com/simia-tech/tapedb/v2"
)

type KeyFunc func(Meta) ([]byte, error)

func (kfn KeyFunc) deriveKey(meta Meta) ([]byte, error) {
	if kfn == nil {
		return nil, nil
	}
	return kfn(meta)
}

func StaticKeyFunc(value []byte) KeyFunc {
	return func(_ Meta) ([]byte, error) {
		return value, nil
	}
}

type createOptions struct {
	directoryMode fs.FileMode
	fileMode      fs.FileMode
	metaFunc      func() Meta
	keyFunc       KeyFunc
}

var defaultCreateOptions = createOptions{
	directoryMode: 0755,
	fileMode:      0644,
	metaFunc:      func() Meta { return Meta{} },
}

type CreateOption func(*createOptions)

func WithDirectoryMode(value fs.FileMode) CreateOption {
	return func(o *createOptions) {
		o.directoryMode = value
	}
}

func WithFileMode(value fs.FileMode) CreateOption {
	return func(o *createOptions) {
		o.fileMode = value
	}
}

func WithMeta(value Meta) CreateOption {
	return func(o *createOptions) {
		o.metaFunc = func() Meta { return value }
	}
}

func WithCreateKey(value []byte) CreateOption {
	return WithCreateKeyFunc(StaticKeyFunc(value))
}

func WithCreateKeyFunc(value KeyFunc) CreateOption {
	return func(o *createOptions) {
		o.keyFunc = value
	}
}

type openOptions struct {
	keyFunc KeyFunc
}

var defaultOpenOptions = openOptions{}

type OpenOption func(*openOptions)

func WithOpenKey(value []byte) OpenOption {
	return WithOpenKeyFunc(StaticKeyFunc(value))
}

func WithOpenKeyFunc(value KeyFunc) OpenOption {
	return func(o *openOptions) {
		o.keyFunc = value
	}
}

type spliceOptions struct {
	sourceKeyFunc          KeyFunc
	targetKeyFunc          KeyFunc
	rebaseChangeSelectFunc RebaseChangeSelectFunc
}

var defaultSpliceOptions = spliceOptions{
	rebaseChangeSelectFunc: StaticRebaseChangeSelectFunc(false),
}

type SpliceOption func(*spliceOptions)

func WithSourceKey(value []byte) SpliceOption {
	return WithSourceKeyFunc(StaticKeyFunc(value))
}

func WithSourceKeyFunc(value KeyFunc) SpliceOption {
	return func(o *spliceOptions) {
		o.sourceKeyFunc = value
	}
}

func WithTargetKey(value []byte) SpliceOption {
	return WithTargetKeyFunc(StaticKeyFunc(value))
}

func WithTargetKeyFunc(value KeyFunc) SpliceOption {
	return func(o *spliceOptions) {
		o.targetKeyFunc = value
	}
}

func WithRebaseChangeCount(value int) SpliceOption {
	return WithRebaseChangeSelectFunc(CountRebaseChangeSelectFunc(value))
}

func WithRebaseChangeSelectFunc(value RebaseChangeSelectFunc) SpliceOption {
	return func(o *spliceOptions) {
		o.rebaseChangeSelectFunc = value
	}
}

type RebaseChangeSelectFunc func(tapedb.Change, int) (bool, error)

func CountRebaseChangeSelectFunc(count int) RebaseChangeSelectFunc {
	return func(change tapedb.Change, logIndex int) (bool, error) {
		return logIndex < count, nil
	}
}

func StaticRebaseChangeSelectFunc(value bool) RebaseChangeSelectFunc {
	return func(_ tapedb.Change, _ int) (bool, error) {
		return value, nil
	}
}
