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

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/bfenetworks/log-reader/reader_conf"
	"github.com/bfenetworks/log-reader/reader_module"

	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"
)

type fakeReaderModule struct {
	name string
}

func (m *fakeReaderModule) Name() string { return m.name }
func (m *fakeReaderModule) Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error {
	return nil
}
func (m *fakeReaderModule) Start() {}
func (m *fakeReaderModule) Update(logs []*bfe_access_pb.BfeLog) {}
func (m *fakeReaderModule) Close() error { return nil }

func TestRegisterModules(t *testing.T) {
	reader_module.AddModule(&fakeReaderModule{name: "mod_fake"})
	rm := reader_module.NewReaderModules()
	registerModules(rm, []string{"mod_fake"})
	if len(rm.All()) != 1 {
		t.Errorf("expected 1 registered module, got %d", len(rm.All()))
	}
}

func TestRegisterModules_Unknown(t *testing.T) {
	rm := reader_module.NewReaderModules()
	registerModules(rm, []string{"mod_unknown"})
	if len(rm.All()) != 0 {
		t.Errorf("expected 0 registered modules, got %d", len(rm.All()))
	}
}

func TestBfeLogReader_RegisterModules(t *testing.T) {
	reader_module.AddModule(&fakeReaderModule{name: "mod_fake"})

	br := &BfeLogReader{
		logReaders: []LogReader{NewPbLogReader("/tmp/pb_access.log", nil, "")},
	}
	config := &reader_conf.ReaderConfig{
		PbAccessLogConf: reader_conf.PbAccessLogConf{
			FinalModules: []string{"mod_fake"},
		},
	}
	if err := br.RegisterModules(config); err != nil {
		t.Fatalf("RegisterModules failed: %v", err)
	}
}
