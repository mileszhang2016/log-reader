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

package reader_conf

import (
	"testing"
)

func TestParseModule(t *testing.T) {
	tests := []struct {
		module   string
		wantName string
		wantEnable bool
	}{
		{"mod_kafka", "mod_kafka", true},
		{"mod_kafka:true", "mod_kafka", true},
		{"mod_kafka:false", "mod_kafka", false},
		{" mod_kafka : true ", "mod_kafka", true},
		{"mod_kafka:TRUE", "mod_kafka", true},
		{"mod_kafka:False", "mod_kafka", false},
		{"mod_kafka:", "mod_kafka", true},
	}

	for _, tt := range tests {
		name, enable := ParseModule(tt.module)
		if name != tt.wantName {
			t.Errorf("ParseModule(%q) name = %q, want %q", tt.module, name, tt.wantName)
		}
		if enable != tt.wantEnable {
			t.Errorf("ParseModule(%q) enable = %v, want %v", tt.module, enable, tt.wantEnable)
		}
	}
}

func TestPbAccessLogConfCheck_Empty(t *testing.T) {
	cfg := &PbAccessLogConf{}
	if err := cfg.Check(); err != nil {
		t.Errorf("empty config should pass, got err: %v", err)
	}
	if cfg.MaxSizePerBatch != -1 {
		t.Errorf("MaxSizePerBatch should be -1, got %d", cfg.MaxSizePerBatch)
	}
}

func TestPbAccessLogConfCheck_LogFileWithoutModules(t *testing.T) {
	cfg := &PbAccessLogConf{
		LogFile: "/tmp/pb_access.log",
	}
	if err := cfg.Check(); err == nil {
		t.Error("expected error when LogFile set but Modules empty")
	}
}

func TestPbAccessLogConfCheck_ModulesWithoutLogFile(t *testing.T) {
	cfg := &PbAccessLogConf{
		Modules: []string{"mod_kafka"},
	}
	if err := cfg.Check(); err == nil {
		t.Error("expected error when Modules set but LogFile empty")
	}
}

func TestPbAccessLogConfCheck_UnsupportedModule(t *testing.T) {
	cfg := &PbAccessLogConf{
		LogFile: "/tmp/pb_access.log",
		Modules: []string{"mod_unknown"},
	}
	if err := cfg.Check(); err == nil {
		t.Error("expected error for unsupported module")
	}
}

func TestPbAccessLogConfCheck_EnabledModules(t *testing.T) {
	cfg := &PbAccessLogConf{
		LogFile: "/tmp/pb_access.log",
		Modules: []string{"mod_kafka:true", "mod_kafka:false"},
	}
	if err := cfg.Check(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.FinalModules) != 1 || cfg.FinalModules[0] != "mod_kafka" {
		t.Errorf("FinalModules should be [mod_kafka], got %v", cfg.FinalModules)
	}
}

func TestPbAccessLogConfCheck_MaxSizePerBatch(t *testing.T) {
	cfg := &PbAccessLogConf{
		LogFile:         "/tmp/pb_access.log",
		Modules:         []string{"mod_kafka"},
		MaxSizePerBatch: 128,
	}
	if err := cfg.Check(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MaxSizePerBatch != 128 {
		t.Errorf("MaxSizePerBatch should be 128, got %d", cfg.MaxSizePerBatch)
	}
}
