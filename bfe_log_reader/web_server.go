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
	"github.com/bfenetworks/go-lib/web-monitor/web_monitor"
)

// all monitor handlers
func (srv *BfeLogReader) monitorHandlers() map[string]interface{} {
	handlers := map[string]interface{}{
		"bfe_reader":      srv.srvStateGet,     // for server state
		"bfe_reader_diff": srv.srvStateDiffGet, // for server state diff
	}
	return handlers
}

// all reload handlers
func (srv *BfeLogReader) reloadHandlers() map[string]interface{} {
	handlers := map[string]interface{}{}
	return handlers
}

func (srv *BfeLogReader) WebHandlersInit() error {
	// register handlers for monitor
	err := web_monitor.RegisterHandlers(srv.WebHandlers, web_monitor.WebHandleMonitor,
		srv.monitorHandlers())
	if err != nil {
		return err
	}

	// register handlers for for reload
	err = web_monitor.RegisterHandlers(srv.WebHandlers, web_monitor.WebHandleReload,
		srv.reloadHandlers())
	if err != nil {
		return err
	}

	return nil
}
