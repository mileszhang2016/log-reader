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
	"testing"

	"github.com/bfenetworks/log-reader/reader_conf"
)

func TestNewBfeLogReader(t *testing.T) {
	config := &reader_conf.ReaderConfig{
		Main: reader_conf.ConfBasic{
			HttpPort:        8992,
			MaxCpus:         4,
			MonitorInterval: 20,
			ProgramName:     "log-reader",
		},
		PbAccessLogConf: reader_conf.PbAccessLogConf{
			LogFile:         "/tmp/pb_access.log",
			Modules:         []string{"mod_kafka"},
			MaxSizePerBatch: 128,
		},
	}

	br, err := NewBfeLogReader(config, "v1.0")
	if err != nil {
		t.Fatalf("NewBfeLogReader failed: %v", err)
	}
	if br == nil {
		t.Fatal("BfeLogReader should not be nil")
	}
	if br.Config != config {
		t.Error("Config should be set")
	}
	if br.WebServer == nil {
		t.Error("WebServer should be initialized")
	}
	if br.WebHandlers == nil {
		t.Error("WebHandlers should be initialized")
	}
	if len(br.logReaders) != 1 {
		t.Errorf("expected 1 log reader, got %d", len(br.logReaders))
	}
}

func TestNewBfeLogReader_NoLogFile(t *testing.T) {
	config := &reader_conf.ReaderConfig{
		Main: reader_conf.ConfBasic{
			HttpPort:        8992,
			MaxCpus:         4,
			MonitorInterval: 20,
			ProgramName:     "log-reader",
		},
		PbAccessLogConf: reader_conf.PbAccessLogConf{},
	}

	br, err := NewBfeLogReader(config, "v1.0")
	if err != nil {
		t.Fatalf("NewBfeLogReader failed: %v", err)
	}
	if len(br.logReaders) != 0 {
		t.Errorf("expected 0 log readers, got %d", len(br.logReaders))
	}
}

func TestBfeLogReader_SetReady(t *testing.T) {
	config := &reader_conf.ReaderConfig{
		Main: reader_conf.ConfBasic{
			HttpPort:        8992,
			MaxCpus:         4,
			MonitorInterval: 20,
			ProgramName:     "log-reader",
		},
	}

	br, err := NewBfeLogReader(config, "v1.0")
	if err != nil {
		t.Fatalf("NewBfeLogReader failed: %v", err)
	}

	br.SetReady()
	val := br.srvState.GetState("SERVER_READY")
	if val != "YES" {
		t.Errorf("SERVER_READY = %q, want YES", val)
	}
}
