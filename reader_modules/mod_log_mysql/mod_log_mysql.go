// Copyright(c) 2026 The Rainway AI Gateway (壬远AI网关) Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package mod_log_mysql

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/reader_conf"
	"github.com/rainway-ai-gateway/log-reader/reader_module"

	_ "github.com/go-sql-driver/mysql"
)

var COUNTER_KEYS = []string{
	"RECEIVED_LOGS",       // Update 收到的日志条数
	"RECEIVED_REQ",        // 其中 request 类型条数
	"CONVERT_FAILED",      // 行组装失败条数
	"SENT_TO_MYSQL",       // 成功入队条数
	"SENT_MYSQL_CHN_FULL", // 队列满丢弃条数（背压）
	"SEND_MYSQL_FAILED",   // 重试耗尽丢弃批数
	"WRITE_BATCH_SIZE",    // 每批实际条数累计（diff 求均值观测批大小）
}

// recordWriter is a small interface to make ModuleLogMysql testable
type recordWriter interface {
	Enqueue(row []interface{}) bool
	Start()
	Close()
}

// fieldMapper is a small interface to make ModuleLogMysql testable
type fieldMapper interface {
	ToRow(log *bfe_access_pb.BfeLog) ([]interface{}, error)
}

// ModuleLogMysql MySQL 输出模块：批量幂等写入明细表 bfe_ai_request_log
type ModuleLogMysql struct {
	name      string
	conf      *ConfModLogMysql
	state     module_state2.State
	stateDiff module_state2.CounterSlice
	mapper    fieldMapper
	writer    recordWriter
}

// NewModuleLogMysql 创建 ModuleLogMysql
func NewModuleLogMysql() *ModuleLogMysql {
	m := new(ModuleLogMysql)
	m.name = "mod_log_mysql"
	return m
}

// Name 返回模块名
func (m *ModuleLogMysql) Name() string {
	return m.name
}

func (m *ModuleLogMysql) getState(query url.Values) ([]byte, error) {
	s := m.state.GetAll()
	return s.FormatOutput(query)
}

func (m *ModuleLogMysql) getStateDiff(query url.Values) ([]byte, error) {
	s := m.stateDiff.Get()
	return s.FormatOutput(query)
}

func (m *ModuleLogMysql) monitorHandlers() map[string]interface{} {
	return map[string]interface{}{
		m.name:           m.getState,
		m.name + "_diff": m.getStateDiff,
	}
}

// Init 初始化模块
func (m *ModuleLogMysql) Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error {
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

	// connect to mysql and verify connectivity (fail-fast)
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4",
		m.conf.Mysql.User, m.conf.Mysql.Password, m.conf.Mysql.Addr, m.conf.Mysql.DBName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Logger.Error("%s.Init(): sql.Open(): %v", m.name, err)
		return fmt.Errorf("sql.Open(): %v", err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		log.Logger.Error("%s.Init(): db.Ping(): %v", m.name, err)
		return fmt.Errorf("db.Ping(): %v", err)
	}
	db.SetMaxOpenConns(m.conf.Writer.MaxOpenConns)
	db.SetMaxIdleConns(m.conf.Writer.MaxIdleConns)

	m.mapper = NewFieldMapper()
	m.writer = NewRecordWriter(db, m.conf, &m.state)
	m.writer.Start()

	err = web_monitor.RegisterHandlers(whs, web_monitor.WebHandleMonitor, m.monitorHandlers())
	if err != nil {
		log.Logger.Error("%s.Init(): RegisterHandlers(): %v", m.name, err)
		return fmt.Errorf("RegisterHandlers(): %v", err)
	}

	log.Logger.Info("%s.Init(): success", m.name)
	return nil
}

// Start 启动模块（写协程已在 Init 启动，此处无操作，与 mod_kafka 一致）
func (m *ModuleLogMysql) Start() {
	log.Logger.Info("%s.Start(): started", m.name)
}

// Update 处理 BfeLog 批次：仅处理 Request 类型，组装行后非阻塞入队
func (m *ModuleLogMysql) Update(bfeLogs []*bfe_access_pb.BfeLog) {
	for _, bfeLog := range bfeLogs {
		m.state.Inc("RECEIVED_LOGS", 1)

		if bfeLog.GetLogType() != bfe_access_pb.BfeLogType_Request {
			continue
		}
		m.state.Inc("RECEIVED_REQ", 1)

		row, err := m.mapper.ToRow(bfeLog)
		if err != nil {
			m.state.Inc("CONVERT_FAILED", 1)
			log.Logger.Error("%s.Update(): ToRow failed: %v", m.name, err)
			continue
		}
		if openDebug {
			log.Logger.Debug("%s.Update(): ToRow, row: %v", m.name, row)
		}

		if !m.writer.Enqueue(row) {
			m.state.Inc("SENT_MYSQL_CHN_FULL", 1)
			log.Logger.Warn("%s.Update(): record channel full, dropping log", m.name)
			continue
		}
		m.state.Inc("SENT_TO_MYSQL", 1)
	}
}

// Close 清理模块资源
func (m *ModuleLogMysql) Close() error {
	if m.writer != nil {
		m.writer.Close()
	}
	return nil
}
