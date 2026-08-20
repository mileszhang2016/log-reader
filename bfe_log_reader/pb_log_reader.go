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
	"fmt"
	"os"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/bfenetworks/log-reader/reader_module"
)

type PbLogReader struct {
	*LogFileReader
}

func NewPbLogReader(logPath string, state *module_state2.State, clusterName string) *PbLogReader {
	return &PbLogReader{newLogFileReader(logPath, state, clusterName)}
}

// parse records from the data buffer
func (lr *PbLogReader) dataBufferParse() []*bfe_access_pb.BfeLog {
	var records []*bfe_access_pb.BfeLog

	// parse pb record from buffer
	records, lr.dataBuffer = pbBuffParse(lr.dataBuffer, lr.state)

	lr.state.Inc("SUM_PB_RECORD", len(records))

	return records
}

/*
logRead - Read data from bp log file

Returns:

	(pbRecords, error)
*/
func (lr *PbLogReader) logRead() ([]*bfe_access_pb.BfeLog, error) {
	var records []*bfe_access_pb.BfeLog
	var data []byte
	var err error
	var hasNewLog bool

	// check whether file is open
	if lr.logFd == nil {
		// check whether file exist
		_, err = os.Stat(lr.logPath)
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not exit: %s", lr.logPath)
		}

		// if not open, or sth goes wrong
		lr.dataBuffer = nil
		lr.logRelocate()

		if lr.logFd == nil {
			// Error happens
			return nil, fmt.Errorf("logRelocate() fail")
		}
	}

	// read data from opened log file
	data, err = lr.fileRead(MAX_BUFF_SIZE)
	lr.state.Inc("SUM_READ_DATA", 1)
	if err != nil {
		// Error happens
		return nil, fmt.Errorf("fileRead() fail:%s", err.Error())
	}

	if len(data) == 0 {
		lr.state.Inc("SUM_READ_DATA_EMPTY", 1)
		records = make([]*bfe_access_pb.BfeLog, 0)

		// End Of File
		hasNewLog, data, err = lr.eofHandler()
		if err == nil && len(data) != 0 {
			lr.dataBuffer = append(lr.dataBuffer, data...)

			// parse records from the data buffer
			records = lr.dataBufferParse()
		}

		// clear the read buffer, if there is new log file
		if hasNewLog {
			lr.dataBuffer = nil
		}
	} else {
		lr.dataBuffer = append(lr.dataBuffer, data...)

		// parse records from the data buffer
		records = lr.dataBufferParse()
	}

	return records, nil
}

// start log reader
func (lr *PbLogReader) Start() {
	log.Logger.Info("LogReader(for reading bfe access pb) Start")

	for {
		// read records from pb log
		records, err := lr.logRead()

		if err != nil {
			// error happens, sleep for a while
			log.Logger.Error("logReader():logRead():%s", err.Error())
			time.Sleep(CALC_SLEEP_TIME)

			continue
		}

		if len(records) == 0 {
			lr.state.Inc("SUM_READ_RECORDS_EMPTY", 1)
			// if there is no new data, also sleep for a while
			time.Sleep(CALC_SLEEP_TIME)
			continue
		}

		if lr.modules != nil {
			var batches [][]*bfe_access_pb.BfeLog
			for i := 0; i < len(records); {
				var batch []*bfe_access_pb.BfeLog
				if lr.MaxSizePerBatch > 0 {
					end := i + lr.MaxSizePerBatch
					if end > len(records) {
						end = len(records)
					}
					size := end - i
					batch = make([]*bfe_access_pb.BfeLog, size)
					copy(batch, records[i:end])
					i = end
				} else {
					batch = records
					i = len(batch)
				}
				batches = append(batches, batch)
			}
			lr.state.Inc("SUM_PB_BATCH_COUNT", len(batches))

			records = nil
			for idx, _ := range batches {
				allModules := lr.modules.All()
				for _, module := range allModules {
					go func(m reader_module.ReaderModule, batchData []*bfe_access_pb.BfeLog) {
						defer func() {
							if r := recover(); r != nil {
								log.Logger.Error("module %s panic: %v", m.Name(), r)
							}
						}()
						m.Update(batchData)
					}(module, batches[idx])
				}
				batches[idx] = nil
			}
			batches = nil
		} else {
			log.Logger.Error("modules are nil")
			time.Sleep(CALC_SLEEP_TIME)
		}
	}
}

// Bind modules to log reader
func (lr *PbLogReader) Bind(readerModules *reader_module.ReaderModules) {
	lr.modules = readerModules
}
