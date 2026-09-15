// Copyright (c) 2026 The BFE Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"

	"github.com/rainway-ai-gateway/log-reader/cmd/bfe-pblog-tool/common"
	"github.com/urfave/cli"

	_ "github.com/rainway-ai-gateway/log-reader/cmd/bfe-pblog-tool/cat"

	_ "github.com/rainway-ai-gateway/log-reader/cmd/bfe-pblog-tool/tail"
)

var (
	version string
	app     = cli.NewApp()
)

func init() {
	if version == "" {
		version = "debug"
	}
	app.Name = os.Args[0]
	app.Usage = "BFE protobuf access log reader"
	app.Description = "https://github.com/rainway-ai-gateway/log-reader"
	app.Version = version
	app.Commands = common.Cmds
	app.Action = func(ctx *cli.Context) error {
		fmt.Println("--help")
		return nil
	}
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Println("main :", err)
		os.Exit(-1)
	}
}