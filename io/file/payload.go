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
	"io"
)

var (
	ErrPayloadIDAlreadyExists = errors.New("payload id already exists")
	ErrPayloadMissing         = errors.New("payload missing")
)

type Payload struct {
	id string
	r  io.Reader
}

func NewPayload(id string, r io.Reader) Payload {
	return Payload{
		id: id,
		r:  r,
	}
}

func (p *Payload) ID() string {
	return p.id
}

type PayloadContainer interface {
	PayloadIDs() []string
}
