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

package mod_kafka

import (
	"fmt"
	"net/url"
	"path"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/bfenetworks/log-reader/reader_conf"
	"github.com/bfenetworks/log-reader/reader_module"
)

var COUNTER_KEYS = []string{
	"RECEIVED_LOGS",
	"RECEIVED_REQ",
	"SENT_TO_KAFKA",
	"CONVERT_FAILED",
	"SEND_KAFKA_FAILED",
	"DLQ_SENT",
	"DLQ_SENT_FAILED",
	"SENT_KAFKA_CHN_FULL",
}

// producer is a small interface to make ModuleKafka testable
type producer interface {
	Send([]byte)
	Start()
	Close() error
}

// ModuleKafka Kafka 模块
type ModuleKafka struct {
	name         string
	state        module_state2.State
	stateDiff    module_state2.CounterSlice
	conf         *ConfModKafka
	producer     producer
	outputFields *OutputFields
}

// NewModuleKafka 创建 ModuleKafka
func NewModuleKafka() *ModuleKafka {
	m := new(ModuleKafka)
	m.name = "mod_kafka"
	return m
}

// Name 返回模块名
func (m *ModuleKafka) Name() string {
	return m.name
}

func (m *ModuleKafka) getState(query url.Values) ([]byte, error) {
	s := m.state.GetAll()
	return s.FormatOutput(query)
}

func (m *ModuleKafka) getStateDiff(query url.Values) ([]byte, error) {
	s := m.stateDiff.Get()
	return s.FormatOutput(query)
}

func (m *ModuleKafka) monitorHandlers() map[string]interface{} {
	return map[string]interface{}{
		m.name:           m.getState,
		m.name + "_diff": m.getStateDiff,
	}
}

// Init 初始化模块
func (m *ModuleKafka) Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error {
	var err error

	m.state.Init()
	m.state.CountersInit(COUNTER_KEYS)
	m.state.SetKeyPrefix(m.name)
	m.state.SetProgramName(conf.Main.ProgramName)

	m.stateDiff.Init(&m.state, conf.Main.MonitorInterval)
	m.stateDiff.SetKeyPrefix(m.name + "_diff")
	m.stateDiff.SetProgramName(conf.Main.ProgramName)

	confPath := reader_module.ModConfPath(cr, m.name)
	m.conf, err = LoadConfig(confPath)
	if err != nil {
		log.Logger.Error("%s.Init(): LoadConfig(): %v", m.name, err)
		return fmt.Errorf("LoadConfig(): %v", err)
	}

	m.outputFields = DefaultOutputFields()
	if m.conf.Basic.DataPath != "" {
		dataPath := path.Join(path.Dir(confPath), m.conf.Basic.DataPath)
		log.Logger.Debug("%s.Init(): m.conf.Basic.DataPath:%s", m.name, dataPath)

		dataCfg, err := LoadKafkaDataConfig(dataPath)
		if err != nil {
			log.Logger.Warn("%s.Init(): LoadKafkaDataConfig(%s): %v, using default fields", m.name, dataPath, err)
			return fmt.Errorf("%s.Init(): LoadKafkaDataConfig() error: %s", m.name, err.Error())
		} else if dataCfg != nil {
			m.outputFields = dataCfg.ResolveFields()
			log.Logger.Info("%s.Init(): loaded field config from %s, mode=%s, fields=%d",
				m.name, dataPath, dataCfg.ConfFields.FieldMode, len(m.outputFields.Set))
		}
	} else {
		log.Logger.Info("%s.Init(): m.conf.Basic.DataPath is empty", m.name)
	}

	m.producer, err = NewKafkaProducer(&m.state, m.conf)
	if err != nil {
		log.Logger.Error("%s.Init(): NewKafkaProducer(): %v", m.name, err)
		return fmt.Errorf("NewKafkaProducer(): %v", err)
	}

	err = web_monitor.RegisterHandlers(whs, web_monitor.WebHandleMonitor, m.monitorHandlers())
	if err != nil {
		log.Logger.Error("%s.Init(): RegisterHandlers(): %v", m.name, err)
		return fmt.Errorf("RegisterHandlers(): %v", err)
	}

	log.Logger.Info("%s.Init(): success", m.name)
	return nil
}

// Start 启动模块
func (m *ModuleKafka) Start() {
	m.producer.Start()
	log.Logger.Info("%s.Start(): started", m.name)
}

// Update 处理 BfeLog 批次
func (m *ModuleKafka) Update(bfeLogs []*bfe_access_pb.BfeLog) {
	for _, bfeLog := range bfeLogs {
		m.state.Inc("RECEIVED_LOGS", 1)

		if bfeLog.GetLogType() != bfe_access_pb.BfeLogType_Request {
			continue
		}
		m.state.Inc("RECEIVED_REQ", 1)

		jsonBytes, err := ConvertBfeLogToJSON(bfeLog, m.outputFields)
		if err != nil {
			m.state.Inc("CONVERT_FAILED", 1)
			log.Logger.Error("%s.Update(): ConvertBfeLogToJSON failed: %v", m.name, err)
			continue
		}
		if openDebug {
			log.Logger.Debug("%s.Update(): ConvertBfeLogToJSON, json: %s", m.name, string(jsonBytes))
		}
		m.producer.Send(jsonBytes)
		m.state.Inc("SENT_TO_KAFKA", 1)
	}
}

// Close 清理模块资源
func (m *ModuleKafka) Close() error {
	if m.producer != nil {
		return m.producer.Close()
	}
	return nil
}
