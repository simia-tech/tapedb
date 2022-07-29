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

package main

import (
	"log"

	"github.com/alecthomas/kong"
)

var cli struct {
	Path                  string `type:"existingdir" default:"." help:"Specifies the path of the database"`
	DeriveKeyFromPassword bool   `short:"p" default:"false" help:"Prompts for a password and derives the encryption key from it"`
	Log                   struct {
		Show struct {
			Follow bool `short:"f" help:"Follows the log and shows new entries immediately"`
		} `cmd:"" help:"Shows the log"`
	} `cmd:"" help:"Collection of log commands"`
	Base struct {
		Show struct{} `cmd:"" help:"Shows the base"`
	} `cmd:"" help:"Collection of base commands"`
}

func main() {
	ctx := kong.Parse(&cli)

	key := []byte(nil)
	if cli.DeriveKeyFromPassword {
		k, err := fetchKey(cli.Path)
		if err != nil {
			log.Fatal(err)
		}
		key = k
	}

	switch ctx.Command() {
	case "log show":
		if err := logShow(cli.Path, key, cli.Log.Show.Follow); err != nil {
			log.Fatal(err)
		}
	case "base show":
		if err := baseShow(cli.Path, key); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal(ctx.Command())
	}
}
