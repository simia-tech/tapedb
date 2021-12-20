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

package chunkio_test

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/simia-tech/tapedb/chunkio"
)

func TestAESStreamWriter(t *testing.T) {
	t.Run("Simpel", func(t *testing.T) {
		c, err := chunkio.NewAESCrypter(testKey, testNonce)
		require.NoError(t, err)

		buffer := bytes.Buffer{}

		w := chunkio.NewAESStreamWriter(&buffer, c)

		_, err = fmt.Fprintf(w, "test 123 test")
		require.NoError(t, err)

		require.NoError(t, w.Flush())

		assert.Equal(t,
			"AAAAAAAAAAAAAAAAPbP0J7mqlL/D/R8bFKeAiICyNpUHqnzXD4HNvm0",
			base64.RawStdEncoding.EncodeToString(buffer.Bytes()))
	})

	t.Run("Large", func(t *testing.T) {
		c, err := chunkio.NewAESCrypter(testKey, testNonce)
		require.NoError(t, err)

		buffer := bytes.Buffer{}

		w := chunkio.NewAESStreamWriter(&buffer, c)

		for index := 0; index < 100; index++ {
			_, err = fmt.Fprintf(w, "test %d\n", index)
			require.NoError(t, err)
		}

		require.NoError(t, w.Flush())

		assert.Equal(t,
			"AAAAAAAAAAAAAAAAPbP0J7mrrPiG+g5IUYvE+MrZCxw+HqdLJH0FVOjSjyIQV7yrb1+vkLAYJbe1HfGJkfb8+hBStks0O1CpvyXab7EtiHJRo9uPsqYJ1pFIYgfKRg8o9yFMjM2sC+LUcWd81lDnXMpFqf2EonU85LM+9hI2RGsqOVc7BJgvytw+Ym0fPa4FtJY3SYOIfg2XnoBIwW+28d6fdv8fX7bBoVbltRTRxY8pbvt1AkLcbfe+gmO46Ls9jJRza+uCI1y5d7aqQWGM4tdGq8LHt8tl0CJKAcoGKbxjweOn8g/FskNu2dqmEc4CnXWAW0KFakKL8srSN+eNkAFmlXWH9XfrVIMtskYtOGVR+nfPrQTY6UEc2g0mJXZOeZ/UUqnJCIKnb0ti16rWfju290/Lni17xkPwgjaFN5lDQn/Pvv4ir3wS8DuADsg+Zkp8h7tfUhrqr8L7uUP21/7kN0rwvszYk01IPI30g5EzKRoNaKZ8PTLuyMAHxKh2I+vJu0/BZP4NKs17wE5cuVVl8gbwKOgpz33Rqv2Ek7JUscRIgOtjwUkUGd5pzp1TptZ0fbrnYntp0MV1ir7Mi8M6Mrl0YE90mR3iFApPLPhkc5JTJgjCLDdGiFw9yGgsZlDDTmeM34+wuDY3FyJEKOVO4RWRzzPcZzUG0CtHtyeQBxmlNj16Hd51dX9hmlFGa8dkS/N89JzSqvpPTRc07tXlY02jryCJzNa0GTcRCLhzEqLs807o/bG41g+W2YDEGazbXjBlmBqC7OKOAEuqhcie5btyhkStyPnVYlcQJg/Vp52HQKoi52MH9jd1phgYT42c789QkT93x36AAYyU8eXirhZ5I25glfBnMKT5PjgcAOPCbLsVpIuNI0m/CHBBRRQ6KGA7XS9FtDlyG8hKuujdDpVtr0SSMf6tH5xgSTvN8Psf21PpR+AryVPy4l6msiBoHyVfAwiQSsAoLVEs3IDOmLlr6yjFzxW5HcS01KX0SvsC2yFPzf0rpR5PxCK3gBu++4Ji4+LhPt48r6y7YwqsMhAH3NmjOzEwLYgO0nIKwx3kdop4k2bfHG8bNGK1u/c",
			base64.RawStdEncoding.EncodeToString(buffer.Bytes()))
	})
}

func TestAESStreamReader(t *testing.T) {
	t.Run("Simpel", func(t *testing.T) {
		c, err := chunkio.NewAESCrypter(testKey, testNonce)
		require.NoError(t, err)

		buffer, _ := base64.RawStdEncoding.DecodeString("AAAAAAAAAAAAAAAAPbP0J7mqlL/D/R8bFKeAiICyNpUHqnzXD4HNvm0")

		r := chunkio.NewAESStreamReader(bytes.NewReader(buffer), c)

		plaintext, err := io.ReadAll(r)
		require.NoError(t, err)

		assert.Equal(t,
			"test 123 test",
			string(plaintext))
	})

	t.Run("Large", func(t *testing.T) {
		c, err := chunkio.NewAESCrypter(testKey, testNonce)
		require.NoError(t, err)

		buffer, _ := base64.RawStdEncoding.DecodeString("AAAAAAAAAAAAAAAAPbP0J7mrrPiG+g5IUYvE+MrZCxw+HqdLJH0FVOjSjyIQV7yrb1+vkLAYJbe1HfGJkfb8+hBStks0O1CpvyXab7EtiHJRo9uPsqYJ1pFIYgfKRg8o9yFMjM2sC+LUcWd81lDnXMpFqf2EonU85LM+9hI2RGsqOVc7BJgvytw+Ym0fPa4FtJY3SYOIfg2XnoBIwW+28d6fdv8fX7bBoVbltRTRxY8pbvt1AkLcbfe+gmO46Ls9jJRza+uCI1y5d7aqQWGM4tdGq8LHt8tl0CJKAcoGKbxjweOn8g/FskNu2dqmEc4CnXWAW0KFakKL8srSN+eNkAFmlXWH9XfrVIMtskYtOGVR+nfPrQTY6UEc2g0mJXZOeZ/UUqnJCIKnb0ti16rWfju290/Lni17xkPwgjaFN5lDQn/Pvv4ir3wS8DuADsg+Zkp8h7tfUhrqr8L7uUP21/7kN0rwvszYk01IPI30g5EzKRoNaKZ8PTLuyMAHxKh2I+vJu0/BZP4NKs17wE5cuVVl8gbwKOgpz33Rqv2Ek7JUscRIgOtjwUkUGd5pzp1TptZ0fbrnYntp0MV1ir7Mi8M6Mrl0YE90mR3iFApPLPhkc5JTJgjCLDdGiFw9yGgsZlDDTmeM34+wuDY3FyJEKOVO4RWRzzPcZzUG0CtHtyeQBxmlNj16Hd51dX9hmlFGa8dkS/N89JzSqvpPTRc07tXlY02jryCJzNa0GTcRCLhzEqLs807o/bG41g+W2YDEGazbXjBlmBqC7OKOAEuqhcie5btyhkStyPnVYlcQJg/Vp52HQKoi52MH9jd1phgYT42c789QkT93x36AAYyU8eXirhZ5I25glfBnMKT5PjgcAOPCbLsVpIuNI0m/CHBBRRQ6KGA7XS9FtDlyG8hKuujdDpVtr0SSMf6tH5xgSTvN8Psf21PpR+AryVPy4l6msiBoHyVfAwiQSsAoLVEs3IDOmLlr6yjFzxW5HcS01KX0SvsC2yFPzf0rpR5PxCK3gBu++4Ji4+LhPt48r6y7YwqsMhAH3NmjOzEwLYgO0nIKwx3kdop4k2bfHG8bNGK1u/c")

		r := chunkio.NewAESStreamReader(bytes.NewReader(buffer), c)

		plaintext, err := io.ReadAll(r)
		require.NoError(t, err)

		b := bytes.Buffer{}
		for index := 0; index < 100; index++ {
			_, err = fmt.Fprintf(&b, "test %d\n", index)
			require.NoError(t, err)
		}
		assert.Equal(t, b.String(), string(plaintext))
	})
}
