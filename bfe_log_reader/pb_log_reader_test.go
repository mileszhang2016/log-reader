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
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/bfenetworks/log-reader/reader_module"
)

func TestNewPbLogReader(t *testing.T) {
	lr := NewPbLogReader("/tmp/pb_access.log", nil, "cluster_a")
	if lr == nil {
		t.Fatal("NewPbLogReader should not return nil")
	}
	if lr.logPath != "/tmp/pb_access.log" {
		t.Errorf("logPath = %q", lr.logPath)
	}
	if lr.clusterName != "cluster_a" {
		t.Errorf("clusterName = %q", lr.clusterName)
	}
}

func TestPbLogReader_dataBufferParse(t *testing.T) {
	data, err := ioutil.ReadFile("test_data/pb_access_1.log")
	if err != nil {
		t.Fatal("fail to open testing data")
	}

	lr := NewPbLogReader("/tmp/pb_access.log", nil, "")
	lr.dataBuffer = data
	records := lr.dataBufferParse()
	if len(records) != 9 {
		t.Errorf("expected 9 records, got %d", len(records))
	}
	if len(lr.dataBuffer) != 0 {
		t.Errorf("expected empty buffer, got %d bytes", len(lr.dataBuffer))
	}
}

func TestPbLogReader_logRead(t *testing.T) {
	SetReadFromBegin(true)
	defer SetReadFromBegin(false)

	data, err := ioutil.ReadFile("test_data/pb_access_1.log")
	if err != nil {
		t.Fatal("fail to open testing data")
	}
	path := createTempLogFile(t, data)

	lr := NewPbLogReader(path, nil, "")
	records, err := lr.logRead()
	if err != nil {
		t.Fatalf("logRead failed: %v", err)
	}
	if len(records) != 9 {
		t.Errorf("expected 9 records, got %d", len(records))
	}
	lr.logFdClose()
}

func TestPbLogReader_logRead_FileNotExist(t *testing.T) {
	lr := NewPbLogReader("/nonexistent/path/pb_access.log", nil, "")
	_, err := lr.logRead()
	if err == nil {
		t.Error("expected error for non-existent file")
	}
}

func TestPbLogReader_Bind(t *testing.T) {
	lr := NewPbLogReader("/tmp/pb_access.log", nil, "")
	rm := reader_module.NewReaderModules()
	lr.Bind(rm)
	if lr.modules != rm {
		t.Error("modules should be bound")
	}
}

func TestPbLogReader_BatchSplit(t *testing.T) {
	lr := NewPbLogReader("/tmp/pb_access.log", nil, "")
	lr.SetMaxSizePerBatch(3)

	records := make([]*bfe_access_pb.BfeLog, 10)
	for i := range records {
		logid := uint64(i)
		records[i] = &bfe_access_pb.BfeLog{Logid: &logid}
	}

	var batches [][]*bfe_access_pb.BfeLog
	for i := 0; i < len(records); {
		var batch []*bfe_access_pb.BfeLog
		if lr.MaxSizePerBatch > 0 {
			end := i + lr.MaxSizePerBatch
			if end > len(records) {
				end = len(records)
			}
			size := end - i
			batch = make([]*bfe_access_pb.BfeLog, size)
			copy(batch, records[i:end])
			i = end
		} else {
			batch = records
			i = len(batch)
		}
		batches = append(batches, batch)
	}

	if len(batches) != 4 {
		t.Errorf("expected 4 batches, got %d", len(batches))
	}
	if len(batches[0]) != 3 || len(batches[3]) != 1 {
		t.Errorf("unexpected batch sizes: %v", batches)
	}
}
