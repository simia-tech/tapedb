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

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/v2/memory"
	"github.com/simia-tech/tapedb/v2/test"
)

func TestMemory(t *testing.T) {
	t.Run("NewDatabase", func(t *testing.T) {
		db, err := memory.NewDatabase[*test.Base, *test.State](test.NewFactory())
		require.NoError(t, err)

		require.NoError(t, db.Apply(&test.ChangeCounterInc{Value: 1}))

		assert.Equal(t, 1, db.State().Counter)
	})
}
