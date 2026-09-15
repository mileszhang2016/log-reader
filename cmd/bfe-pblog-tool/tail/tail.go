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

// tail.go - CLI tail command for protobuf access logs

package tail

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rainway-ai-gateway/log-reader/cmd/bfe-pblog-tool/common"
	"github.com/urfave/cli"
)

type tailCmd struct{}

func init() {
	common.RegistCmd(cli.Command{
		Name:   "tail",
		Usage:  "示例: pbtool tail -n 20 [-f --interval 200] /path/to/pb_access3.log",
		Action: new(tailCmd).Action,
		Flags: []cli.Flag{
			cli.IntFlag{Name: "n", Usage: "最后 N 条 (默认10)"},
			cli.BoolFlag{Name: "f", Usage: "持续跟随输出新增日志"},
			cli.IntFlag{Name: "interval", Usage: "跟随模式下的轮询间隔(毫秒), 默认500"},
		},
	})
}

func (t *tailCmd) Action(ctx *cli.Context) error {
	fp := ctx.Args().First()
	if fp == "" {
		return fmt.Errorf("需要提供日志文件路径, 如: pbtool tail -n 20 /var/log/pb_access3.log")
	}
	if _, err := os.Stat(fp); err != nil {
		return err
	}

	n := ctx.Int("n")
	follow := ctx.Bool("f")
	intervalMs := ctx.Int("interval")

	opts := TailOptions{N: n, Follow: follow}
	if follow && intervalMs > 0 {
		opts.Interval = time.Duration(intervalMs) * time.Millisecond
	}

	// Ctrl+C handling to stop follow mode gracefully
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	var canceled bool

	go func() {
		<-stopCh
		canceled = true
		// second Ctrl+C exits immediately
		go func() { <-stopCh; os.Exit(1) }()
	}()

	return PblogTail(fp, opts, func(line string) error {
		if canceled {
			return ErrTailStop
		}
		fmt.Println(line)
		return nil
	})
}
