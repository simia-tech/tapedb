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

package io

import "io"

type CountReader[R io.Reader] struct {
	r     R
	count int
}

var _ io.Reader = &CountReader[io.Reader]{}

func NewCountReader[R io.Reader](r R) *CountReader[R] {
	return &CountReader[R]{r: r}
}

func (r *CountReader[R]) Read(data []byte) (int, error) {
	n, err := r.r.Read(data)
	r.count += n
	return n, err
}

func (r *CountReader[R]) Count() int {
	return r.count
}
