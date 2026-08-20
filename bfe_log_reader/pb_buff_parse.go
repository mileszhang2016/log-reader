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
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"google.golang.org/protobuf/proto"

	"github.com/bfenetworks/bfe-access-pb/b2log"
	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

func pbBuffParse(buffer []byte, state *module_state2.State) ([]*bfe_access_pb.BfeLog, []byte) {
	var recordStrs []b2log.Record

	// get records(in binary) from buffer
	recordStrs, buffer = b2log.BuffParse(buffer)

	// convert record string to pb record
	records := make([]*bfe_access_pb.BfeLog, 0)

	for _, recordStr := range recordStrs {
		record := new(bfe_access_pb.BfeLog)

		// convert record string to pb record
		err := proto.Unmarshal(recordStr, record)
		if err != nil {
			if state != nil {
				state.Inc("ERR_PB_DECODE", 1)
			}

			continue
		}

		records = append(records, record)
	}

	return records, buffer
}
