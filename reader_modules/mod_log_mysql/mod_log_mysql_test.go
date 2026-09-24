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
	"errors"
	"net/url"
	"strings"
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

func TestModuleLogMysql_Name(t *testing.T) {
	m := NewModuleLogMysql()
	if m.Name() != "mod_log_mysql" {
		t.Errorf("Name() = %q, want mod_log_mysql", m.Name())
	}
}

func TestModuleLogMysql_Close_NilWriter(t *testing.T) {
	m := NewModuleLogMysql()
	if err := m.Close(); err != nil {
		t.Errorf("Close with nil writer should not error, got %v", err)
	}
}

// fakeRecordWriter implements recordWriter for Update tests.
type fakeRecordWriter struct {
	enqueueOk bool
	enqueued  int
	started   bool
	closed    bool
}

func (f *fakeRecordWriter) Enqueue(row []interface{}) bool {
	if f.enqueueOk {
		f.enqueued++
	}
	return f.enqueueOk
}
func (f *fakeRecordWriter) Start() { f.started = true }
func (f *fakeRecordWriter) Close() { f.closed = true }

// failMapper implements fieldMapper returning an error.
type failMapper struct{}

func (failMapper) ToRow(log *bfe_access_pb.BfeLog) ([]interface{}, error) {
	return nil, errors.New("row assemble failed")
}

func newTestModule() *ModuleLogMysql {
	m := NewModuleLogMysql()
	m.state.Init()
	m.state.CountersInit(COUNTER_KEYS)
	m.mapper = NewFieldMapper()
	m.writer = &fakeRecordWriter{enqueueOk: true}
	return m
}

func TestModuleLogMysql_Update(t *testing.T) {
	m := newTestModule()

	logid := uint64(1)
	ts := uint64(100)
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
		{
			Logid:   &logid,
			LogType: bfe_access_pb.BfeLogType_Session.Enum(),
		},
	}

	m.Update(logs)

	if got := m.state.GetCounter("RECEIVED_LOGS"); got != 2 {
		t.Errorf("RECEIVED_LOGS = %d, want 2", got)
	}
	if got := m.state.GetCounter("RECEIVED_REQ"); got != 1 {
		t.Errorf("RECEIVED_REQ = %d, want 1 (session log filtered out)", got)
	}
	if got := m.state.GetCounter("SENT_TO_MYSQL"); got != 1 {
		t.Errorf("SENT_TO_MYSQL = %d, want 1", got)
	}
	if got := m.state.GetCounter("SENT_MYSQL_CHN_FULL"); got != 0 {
		t.Errorf("SENT_MYSQL_CHN_FULL = %d, want 0", got)
	}
}

func TestModuleLogMysql_Update_ChannelFull(t *testing.T) {
	m := newTestModule()
	m.writer = &fakeRecordWriter{enqueueOk: false}

	logid := uint64(1)
	ts := uint64(100)
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
	}

	m.Update(logs)

	if got := m.state.GetCounter("SENT_TO_MYSQL"); got != 0 {
		t.Errorf("SENT_TO_MYSQL = %d, want 0", got)
	}
	if got := m.state.GetCounter("SENT_MYSQL_CHN_FULL"); got != 1 {
		t.Errorf("SENT_MYSQL_CHN_FULL = %d, want 1", got)
	}
}

func TestModuleLogMysql_Update_ConvertFailed(t *testing.T) {
	m := newTestModule()
	m.mapper = failMapper{}

	logid := uint64(1)
	ts := uint64(100)
	product := bfe_access_pb.ProductID_BFE
	logs := []*bfe_access_pb.BfeLog{
		{
			Logid:     &logid,
			Timestamp: &ts,
			Product:   &product,
			LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
			RequestLog: &bfe_access_pb.RequestLog{
				ErrCode: strPtr(""),
				ErrMsg:  strPtr(""),
			},
		},
	}

	m.Update(logs)

	if got := m.state.GetCounter("CONVERT_FAILED"); got != 1 {
		t.Errorf("CONVERT_FAILED = %d, want 1", got)
	}
	if got := m.state.GetCounter("SENT_TO_MYSQL"); got != 0 {
		t.Errorf("SENT_TO_MYSQL = %d, want 0", got)
	}
}

func TestModuleLogMysql_MonitorHandlers(t *testing.T) {
	m := newTestModule()
	m.state.SetKeyPrefix(m.name)
	m.state.SetProgramName("log_reader")

	handlers := m.monitorHandlers()
	if _, ok := handlers["mod_log_mysql"]; !ok {
		t.Error("monitorHandlers should contain mod_log_mysql")
	}
	if _, ok := handlers["mod_log_mysql_diff"]; !ok {
		t.Error("monitorHandlers should contain mod_log_mysql_diff")
	}

	getState, ok := handlers["mod_log_mysql"].(func(url.Values) ([]byte, error))
	if !ok {
		t.Fatal("mod_log_mysql handler should be func(url.Values) ([]byte, error)")
	}
	body, err := getState(url.Values{})
	if err != nil {
		t.Fatalf("getState failed: %v", err)
	}
	for _, key := range COUNTER_KEYS {
		if !strings.Contains(string(body), key) {
			t.Errorf("getState output should contain counter %q, got:\n%s", key, body)
		}
	}

	getStateDiff, ok := handlers["mod_log_mysql_diff"].(func(url.Values) ([]byte, error))
	if !ok {
		t.Fatal("mod_log_mysql_diff handler should be func(url.Values) ([]byte, error)")
	}
	if _, err := getStateDiff(url.Values{}); err != nil {
		t.Fatalf("getStateDiff failed: %v", err)
	}
}
