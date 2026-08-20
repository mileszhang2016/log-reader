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
	"fmt"

	"github.com/bfenetworks/go-lib/log"
)

/* main conf */
type ConfBasic struct {
	MaxCpus         int    // max cpus to use
	HttpPort        int    // http port for monitor and reload
	HttpAddr        string // http listen address for monitor and reload (empty means all interfaces)
	MonitorInterval int    // interval for getting diff of server-state

	ProgramName string // name of the program
}

func (cfg *ConfBasic) Check() error {
	return ConfBasicCheck(cfg)
}

func ConfBasicCheck(cfg *ConfBasic) error {
	return basicConfCheck(cfg)
}

func basicConfCheck(cfg *ConfBasic) error {
	// check MaxCpus
	if cfg.MaxCpus <= 0 {
		return fmt.Errorf("MaxCpus is too small([%d])", cfg.MaxCpus)
	}

	// check HttpPort
	if cfg.HttpPort < 0 || cfg.HttpPort > 65535 {
		return fmt.Errorf("HttpPort should be in [0, 65535], but now it's [%d]", cfg.HttpPort)
	}

	// check MonitorInterval
	if cfg.MonitorInterval <= 0 {
		// not set, use default value
		log.Logger.Warn("MonitorInterval not set, use default value(20)")
		cfg.MonitorInterval = 20
	} else if cfg.MonitorInterval > 60 {
		log.Logger.Warn("MonitorInterval[%d] > 60, use 60", cfg.MonitorInterval)
		cfg.MonitorInterval = 60
	} else {
		if 60%cfg.MonitorInterval > 0 {
			return fmt.Errorf("MonitorInterval[%d] can not divide 60", cfg.MonitorInterval)
		}

		if cfg.MonitorInterval < 20 {
			return fmt.Errorf("MonitorInterval[%d] is too small(<20)", cfg.MonitorInterval)
		}
	}

	return nil
}