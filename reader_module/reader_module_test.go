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

package reader_module

import (
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/bfenetworks/log-reader/reader_conf"

	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"
)

type fakeModule struct {
	name string
}

func (m *fakeModule) Name() string { return m.name }
func (m *fakeModule) Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error {
	return nil
}
func (m *fakeModule) Start() {}
func (m *fakeModule) Update(logs []*bfe_access_pb.BfeLog) {}
func (m *fakeModule) Close() error { return nil }

func TestAddModule(t *testing.T) {
	moduleMap = make(map[string]ReaderModule)
	m := &fakeModule{name: "mod_fake"}
	AddModule(m)
	if _, ok := moduleMap["mod_fake"]; !ok {
		t.Error("module should be added")
	}
}

func TestRegisterModule(t *testing.T) {
	moduleMap = make(map[string]ReaderModule)
	workModuleNames = make([]string, 0)
	AddModule(&fakeModule{name: "mod_fake"})

	rm := NewReaderModules()
	if err := rm.RegisterModule("mod_fake"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rm.All()) != 1 {
		t.Errorf("expected 1 module, got %d", len(rm.All()))
	}
}

func TestRegisterModule_NotExist(t *testing.T) {
	moduleMap = make(map[string]ReaderModule)
	workModuleNames = make([]string, 0)
	rm := NewReaderModules()
	if err := rm.RegisterModule("mod_not_exist"); err == nil {
		t.Error("expected error for non-existent module")
	}
}

func TestGetWorkModules(t *testing.T) {
	moduleMap = make(map[string]ReaderModule)
	workModuleNames = make([]string, 0)
	AddModule(&fakeModule{name: "mod_a"})
	AddModule(&fakeModule{name: "mod_b"})

	rm := NewReaderModules()
	rm.RegisterModule("mod_a")

	modules := GetWorkModules()
	if len(modules) != 1 {
		t.Errorf("expected 1 work module, got %d", len(modules))
	}
	if _, ok := modules["mod_a"]; !ok {
		t.Error("mod_a should be in work modules")
	}
}

func TestModConfPath(t *testing.T) {
	path := ModConfPath("/home/work/log-reader/conf", "mod_kafka")
	want := "/home/work/log-reader/conf/mod_kafka/mod_kafka.conf"
	if path != want {
		t.Errorf("ModConfPath() = %q, want %q", path, want)
	}
}
