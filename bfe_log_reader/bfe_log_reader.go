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

package bfe_log_reader

import (
	"fmt"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"
	"github.com/bfenetworks/bfe/bfe_util/signal_table"
	"github.com/bfenetworks/log-reader/reader_conf"
	"github.com/bfenetworks/log-reader/reader_module"
	"github.com/bfenetworks/log-reader/reader_util"
)

type BfeLogReader struct {
	Config     *reader_conf.ReaderConfig // server config
	logReaders []LogReader               // for read access log

	// web server for monitor and reload
	WebServer   *web_monitor.MonitorServer
	WebHandlers *web_monitor.WebHandlers
	SignalTable *signal_table.SignalTable // signal table

	// for monitor
	srvState     module_state2.State        // server state
	srvStateDiff module_state2.CounterSlice // diff for server state
}

/*
create LogBfeReader
param:

	config:  the Base config
	version: the version of LogReader

return:

	(*LogReader, error)
*/
func NewBfeLogReader(config *reader_conf.ReaderConfig, version string) (*BfeLogReader, error) {
	// create BfeReader
	br := new(BfeLogReader)

	// reader config
	br.Config = config

	// initialize counters, srvState
	br.srvState.Init()
	br.srvState.CountersInit(COUNTER_KEYS)
	br.srvState.SetKeyPrefix("bfe_reader")
	br.srvState.SetProgramName(config.Main.ProgramName)

	// initialize srvStateDiff
	br.srvStateDiff.Init(&br.srvState, config.Main.MonitorInterval)
	br.srvStateDiff.SetKeyPrefix("bfe_reader_diff")
	br.srvStateDiff.SetProgramName(config.Main.ProgramName)

	// to show bfe cluster name
	// Removed BFE_CLUSTER and IS_SMALL_CLUSTER metrics as corresponding fields were removed from config
	// Using empty string for cluster name as the field was removed
	// These metrics are no longer set as the corresponding config fields were removed

	// set SERVER_READY
	br.srvState.Set("SERVER_READY", "NO")

	// initialize web handlers
	br.WebHandlers = web_monitor.NewWebHandlers()
	if err := br.WebHandlersInit(); err != nil {
		return nil, fmt.Errorf("WebHandlersInit():%s", err.Error())
	}

	// initialize web server
	if config.Main.HttpAddr != "" {
		br.WebServer = web_monitor.NewMonitorServerWithAddr("log-reader", version, config.Main.HttpAddr, config.Main.HttpPort)
	} else {
		br.WebServer = web_monitor.NewMonitorServer("log-reader", version, config.Main.HttpPort)
	}
	br.WebServer.HandlersSet(br.WebHandlers)

	// new log reader
	br.newLogReader()

	return br, nil
}

// new logReader
func (br *BfeLogReader) newLogReader() error {
	// create LogReader for bfe access log pb
	pbConf := br.Config.PbAccessLogConf
	if pbConf.LogFile != "" {
		lr := NewPbLogReader(pbConf.LogFile, &br.srvState, "")
		lr.SetMaxSizePerBatch(pbConf.MaxSizePerBatch)
		br.logReaders = append(br.logReaders, lr)
	}

	return nil
}

// setup signal table
func (br *BfeLogReader) InitSignalTable() {
	/* create signal table */
	br.SignalTable = signal_table.NewSignalTable()

	/* register signal handlers */
	reader_util.RegisterSignalHandlers(br.SignalTable)

	/* start signal handler routine */
	br.SignalTable.StartSignalHandle()
}

// start BfeLogReader
func (br *BfeLogReader) Start(confRoot string) error {
	// start all work modules
	for mname, module := range reader_module.GetWorkModules() {
		// init module first
		err := module.Init(br.Config, br.WebHandlers, confRoot)
		if err != nil {
			log.Logger.Error("BfeLogReader failed to init module %s", mname)
			return err
		}

		go module.Start()
	}

	// start all logReader
	for _, lr := range br.logReaders {
		go lr.Start()
	}

	return nil
}

// set "SERVER_READY" to YES
func (br *BfeLogReader) SetReady() {
	br.srvState.Set("SERVER_READY", "YES")
}
