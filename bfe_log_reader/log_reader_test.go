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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
)

func createTempLogFile(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "pb_access.log")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSetReadFromBegin(t *testing.T) {
	SetReadFromBegin(true)
	if !isReadFromBegin {
		t.Error("isReadFromBegin should be true")
	}
	SetReadFromBegin(false)
	if isReadFromBegin {
		t.Error("isReadFromBegin should be false")
	}
}

func TestNewLogFileReader(t *testing.T) {
	path := createTempLogFile(t, []byte("test"))
	lr := newLogFileReader(path, nil, "cluster_a")
	if lr.logPath != path {
		t.Errorf("logPath = %q, want %q", lr.logPath, path)
	}
	if lr.clusterName != "cluster_a" {
		t.Errorf("clusterName = %q, want cluster_a", lr.clusterName)
	}
	if lr.MaxSizePerBatch != -1 {
		t.Errorf("MaxSizePerBatch should default to -1, got %d", lr.MaxSizePerBatch)
	}
	if lr.state == nil {
		t.Error("state should be initialized")
	}
}

func TestLogFileReader_logFileOpen(t *testing.T) {
	data, err := ioutil.ReadFile("test_data/pb_access_1.log")
	if err != nil {
		t.Fatal("fail to open testing data")
	}
	path := createTempLogFile(t, data)

	lr := newLogFileReader(path, nil, "")
	if err := lr.logFileOpen(); err != nil {
		t.Fatalf("logFileOpen failed: %v", err)
	}
	if lr.logFd == nil {
		t.Fatal("logFd should not be nil")
	}
	if lr.fileInfo == nil {
		t.Fatal("fileInfo should not be nil")
	}
	if !lr.initDone {
		t.Error("initDone should be true")
	}
	lr.logFdClose()
}

func TestLogFileReader_logFileOpen_NotExist(t *testing.T) {
	lr := newLogFileReader("/nonexistent/path/pb_access.log", nil, "")
	if err := lr.logFileOpen(); err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestLogFileReader_fRead(t *testing.T) {
	SetReadFromBegin(true)
	defer SetReadFromBegin(false)

	data, err := ioutil.ReadFile("test_data/pb_access_1.log")
	if err != nil {
		t.Fatal("fail to open testing data")
	}
	path := createTempLogFile(t, data)

	lr := newLogFileReader(path, nil, "")
	if err := lr.logFileOpen(); err != nil {
		t.Fatal(err)
	}
	defer lr.logFdClose()

	readData, err := lr.fRead(32)
	if err != nil {
		t.Fatalf("fRead failed: %v", err)
	}
	if len(readData) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(readData))
	}
}

func TestLogFileReader_fRead_ExceedMax(t *testing.T) {
	path := createTempLogFile(t, []byte("test"))
	lr := newLogFileReader(path, nil, "")
	if err := lr.logFileOpen(); err != nil {
		t.Fatal(err)
	}
	defer lr.logFdClose()

	if _, err := lr.fRead(MAX_BUFF_SIZE + 1); err == nil {
		t.Error("expected error when exceeding max buffer size")
	}
}

func TestLogFileReader_isLogCut(t *testing.T) {
	data, err := ioutil.ReadFile("test_data/pb_access_1.log")
	if err != nil {
		t.Fatal("fail to open testing data")
	}
	path := createTempLogFile(t, data)

	lr := newLogFileReader(path, nil, "")
	if err := lr.logFileOpen(); err != nil {
		t.Fatal(err)
	}
	defer lr.logFdClose()

	if lr.isLogCut() {
		t.Error("should not detect log cut for same file")
	}
}

func TestLogFileReader_SetMaxSizePerBatch(t *testing.T) {
	lr := newLogFileReader("/tmp/test.log", nil, "")
	lr.SetMaxSizePerBatch(100)
	if lr.MaxSizePerBatch != 100 {
		t.Errorf("MaxSizePerBatch = %d, want 100", lr.MaxSizePerBatch)
	}
}

func TestLogFileReader_State(t *testing.T) {
	state := new(module_state2.State)
	state.Init()
	lr := newLogFileReader("/tmp/test.log", state, "")
	if lr.state != state {
		t.Error("state should use provided state")
	}
}
