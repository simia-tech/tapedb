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
	"crypto/sha256"

	"github.com/simia-tech/crypt"
)

const (
	MetaHeaderCryptSettings = "Crypt-Settings"

	DefaultCryptSettings = "$argon2id$v=19$m=65536,t=2,p=4$"
)

func DeriveKeyFrom(password, defaultCryptSettings string) KeyFunc {
	return func(meta Meta) ([]byte, error) {
		if password == "" {
			return nil, nil
		}

		cs := meta.Get(MetaHeaderCryptSettings)
		if cs == "" {
			cs = defaultCryptSettings
		}

		hashedPassword, err := crypt.Crypt(password, cs)
		if err != nil {
			return nil, err
		}

		meta.Set(MetaHeaderCryptSettings, crypt.Settings(hashedPassword))

		hash := sha256.Sum256([]byte(hashedPassword))

		return hash[:], nil
	}
}
