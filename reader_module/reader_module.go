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
	"fmt"
	"path"

	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/bfenetworks/log-reader/reader_conf"
)

type ReaderModule interface {
	// get name of module
	Name() string
	// initialize module
	// Params:
	//      - whs: web monitor handlers. for register web monitor handler
	//      - cr: config root path. for get config path of module
	Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error
	// start the module
	Start()
	// update pb access log to module (bfe_access_pb)
	Update([]*bfe_access_pb.BfeLog)
	// close the module for cleanup
	Close() error
}

var moduleMap = make(map[string]ReaderModule) // map mod_name to module, for ALL modules
var workModuleNames = make([]string, 0)       // name of work modules

type ReaderModules struct {
	// modules, configure in different logReader conf file
	modules map[string]ReaderModule
}

// create new ReaderModules
func NewReaderModules() *ReaderModules {
	readerModules := new(ReaderModules)
	readerModules.modules = make(map[string]ReaderModule)

	return readerModules
}

// add module to moduleMap
func AddModule(module ReaderModule) {
	moduleMap[module.Name()] = module
}

// register and record modules
func (rm *ReaderModules) RegisterModule(name string) error {
	module, ok := moduleMap[name]
	if !ok {
		return fmt.Errorf("no module for %s", name)
	}
	rm.modules[name] = module

	// record name for all work modules
	workModuleNames = append(workModuleNames, name)

	return nil
}

// get all modules register for one logReader
func (rm *ReaderModules) All() map[string]ReaderModule {
	return rm.modules
}

// get all work modules
func GetWorkModules() map[string]ReaderModule {
	modules := make(map[string]ReaderModule)

	for _, name := range workModuleNames {
		if module, ok := moduleMap[name]; ok {
			modules[name] = module
		}
	}
	return modules
}

// get full path of module config file
//
// format:  confRoot/<modName>/<modName>.conf
//
// e.g., confRoot = "/home/work/log-reader/conf", modName = "mod_kafka"
// return "/home/work/log-reader/conf/mod_kafka/mod_kafka.conf"
func ModConfPath(confRoot string, modName string) string {
	confPath := path.Join(confRoot, modName, modName+".conf")
	return confPath
}
