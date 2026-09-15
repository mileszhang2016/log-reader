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
	gcfg "gopkg.in/gcfg.v1"
)

// all modules support
var modulesSupport = map[string]string{
	"mod_kafka":     "mod_kafka",
	"mod_log_mysql": "mod_log_mysql",
}

type ReaderConfig struct {
	Main            ConfBasic
	PbAccessLogConf PbAccessLogConf
}

/*
ReaderConfigLoad - load config for logReader

Params:
  - filePath: path of logReader config file

Returns:

	(ReaderConfig, error)
*/
func ReaderConfigLoad(filePath string) (ReaderConfig, error) {
	var cfg ReaderConfig
	var err error

	// read config from file
	err = gcfg.ReadFileInto(&cfg, filePath)
	if err != nil {
		return cfg, err
	}

	if err = cfg.Main.Check(); err != nil {
		return cfg, err
	}

	if err = cfg.PbAccessLogConf.Check(); err != nil {
		return cfg, err
	}

	return cfg, nil
}
