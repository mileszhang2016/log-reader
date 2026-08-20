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
	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/log-reader/reader_conf"
	"github.com/bfenetworks/log-reader/reader_module"
)

// registerModules registers modules from a string list, parsing "mod_name:enabled" format
func registerModules(rm *reader_module.ReaderModules, modules []string) {
	for _, module := range modules {
		err := rm.RegisterModule(module)
		if err != nil {
			log.Logger.Error("RegisterModules failed, module:%s, err:%s", module, err.Error())
		}
	}
}

// Register reader modules
func (br *BfeLogReader) RegisterModules(config *reader_conf.ReaderConfig) error {

	for _, reader := range br.logReaders {
		logReaderModules := reader_module.NewReaderModules()
		registerModules(logReaderModules, config.PbAccessLogConf.FinalModules)
		reader.Bind(logReaderModules)
		log.Logger.Info("RegisterModules ok")
	}

	return nil
}
