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
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/bfenetworks/go-lib/log"
	"github.com/bfenetworks/go-lib/web-monitor/module_state2"
	"github.com/bfenetworks/log-reader/reader_module"
)

const (
	MAX_BUFF_SIZE = 1024 * 1024 // max buffer, in bytes

	// when we get nothing in logReader, sleep for a while
	CALC_SLEEP_TIME = 10 * time.Millisecond
)

var isReadFromBegin bool

func SetReadFromBegin(isFromBegin bool) {
	isReadFromBegin = isFromBegin
}

type LogReader interface {
	Start()
	Bind(*reader_module.ReaderModules)
}

type LogFileReader struct {
	logPath  string      // path of pb log file
	logFd    *os.File    // file descriptor
	fileInfo os.FileInfo // fileInfo for pb log file. get from os.Stat()
	// It is used to detect whether log file is changed

	clusterName string // name of bfe cluster

	initDone   bool   // whether it's just started
	dataBuffer []byte // buffer to store data read from file

	state *module_state2.State // for collecting state data

	readBuffer [MAX_BUFF_SIZE]byte // buffer reused in reading from file

	modules         *reader_module.ReaderModules // modules (bind to LogReader)
	MaxSizePerBatch int                          // max element size per batch; <=0 unlimited
}

/*
NewLogReader - create new LogReader

Params:
- logPath: path of pb log file
- state: state of bfe-reader, to collect state informaiton
- clusterName: cluster of bfe

Returns:
- *LogReader
*/
func newLogFileReader(logPath string, state *module_state2.State, clusterName string) *LogFileReader {
	lr := new(LogFileReader)

	// set logPath
	lr.logPath = logPath

	// set clusterName
	lr.clusterName = clusterName

	// prepare state
	if state != nil {
		lr.state = state
	} else {
		// if state is nil, create it now. to make unittest easy
		lr.state = new(module_state2.State)
		lr.state.Init()
	}

	lr.MaxSizePerBatch = -1
	return lr
}

/*
logFileOpen -  Open the log file, file name is lr.logPath

If succeed, fd will be stored in lr.logFd, fileInfo will be stored in lr.fileInfo
*/
func (lr *LogFileReader) logFileOpen() error {
	// open log file
	fd, err := os.Open(lr.logPath)
	if err != nil {
		lr.state.Inc("ERR_PB_OPEN", 1)
		log.Logger.Error("logFileOpen():open(): path:[%s], err:[%s]", lr.logPath, err.Error())
		return fmt.Errorf("os.Open():%s", err.Error())
	}

	whence := 2 //end
	if isReadFromBegin {
		whence = 0 //begin
	}

	// when first start, seek to end of the file
	if !lr.initDone {

		_, err = fd.Seek(0, whence)
		log.Logger.Debug("Open the pb log first time, seek to [%d]", whence)
		if err != nil {
			lr.state.Inc("ERR_PB_SEEK", 1)
			log.Logger.Error("logFileOpen():seek(): path:[%s], err:[%s]", lr.logPath, err.Error())

			// close file
			errClose := fd.Close()
			if errClose != nil {
				lr.state.Inc("ERR_PB_CLOSE", 1)
				log.Logger.Warn("logFileOpen():close(): path:[%s], err:[%s]",
					lr.logPath, errClose.Error())
			}

			return fmt.Errorf("fd.Seek():%s", err.Error())
		}
	}

	// get file info
	fi, err := fd.Stat()
	if err != nil {
		lr.state.Inc("ERR_PB_STAT", 1)
		log.Logger.Error("logFileOpen():stat(): path:[%s], err:[%s]", lr.logPath, err.Error())

		// close file
		errClose := fd.Close()
		if errClose != nil {
			lr.state.Inc("ERR_PB_CLOSE", 1)
			log.Logger.Warn("logFileOpen():close(): path:[%s], err:[%s]",
				lr.logPath, errClose.Error())
		}

		return fmt.Errorf("fd.Stat():%s", err.Error())
	}

	// save fd and fi
	lr.logFd = fd
	lr.fileInfo = fi

	// modify flag of initDone
	lr.initDone = true

	return nil
}

/*
logRelocate -  relocate to the new log file

Close the old logFd(if logFd is not nil), and open the new logFile.
*/
func (lr *LogFileReader) logRelocate() {
	lr.state.Inc("PB_LOG_RELOCATE", 1)

	// close the old logFd, if logFd is not nil
	if lr.logFd != nil {
		errClose := lr.logFd.Close()
		log.Logger.Info("logRelocate():close(): path:[%s]", lr.logPath)

		if errClose != nil {
			lr.state.Inc("ERR_PB_CLOSE", 1)
			log.Logger.Warn("logRelocate():close(): path:[%s], err:[%s]",
				lr.logPath, errClose.Error())
		}

		// set logFd to nil
		lr.logFd = nil
	}

	// open log file again
	err := lr.logFileOpen()
	if err != nil {
		log.Logger.Error("logRelocate():logFileOpen():%s", err.Error())
	} else {
		log.Logger.Info("logRelocate():log file[%s] is cut and relocated", lr.logPath)
	}
}

// closeFileAndInit - close the file, and clear the state
func (lr *LogFileReader) closeFileAndInit() {
	// close the file
	err := lr.logFd.Close()
	if err != nil {
		lr.state.Inc("ERR_PB_CLOSE", 1)
		log.Logger.Warn("closeFileAndInit():close(): path:[%s], err:[%s]",
			lr.logPath, err.Error())
	}

	// clear the state
	lr.logFd = nil
	lr.dataBuffer = nil
	lr.initDone = false
}

// check whether there is new inode for the file
func (lr *LogFileReader) isLogCut() bool {
	// get file info for logPath
	fi, err := os.Stat(lr.logPath)
	if err != nil {
		// if there is error in getting file info, we assume
		// there is no new pb log file to use
		return false
	}

	return !os.SameFile(fi, lr.fileInfo)
}

// fRead - read from file
//
// # Seperate this func to easy unit-test
//
// Params:
//   - maxSize: max bytes to read
func (lr *LogFileReader) fRead(maxSize int) ([]byte, error) {
	var err error
	var n int
	var data []byte

	if maxSize > MAX_BUFF_SIZE {
		return nil, fmt.Errorf("exceed max buffer size(%d, %d)", MAX_BUFF_SIZE, maxSize)
	}

	if maxSize == 0 {
		// read all left data from file
		data, err = ioutil.ReadAll(lr.logFd)
	} else {
		// read out maxSize bytes of data
		n, err = lr.logFd.Read(lr.readBuffer[0:maxSize])
		if err == nil {
			// copy data from buffer to data
			data = make([]byte, n)
			copy(data, lr.readBuffer[0:n])
		} else if err == io.EOF {
			data = make([]byte, 0)
			err = nil
		}
	}

	return data, err
}

/*
fileRead - Read data from opened log file

Params:
  - maxSize: max size of bytes to read out ( < MAX_BUFF_SIZE)
    if 0, no limit on maxSize

Returns:

	(data, error)
*/
func (lr *LogFileReader) fileRead(maxSize int) ([]byte, error) {
	data, err := lr.fRead(maxSize)

	if err != nil {
		lr.state.Inc("ERR_PB_READ", 1)
		log.Logger.Error("fileRead():err in fRead():[%s]", err.Error())

		// close the file and clear the state
		lr.closeFileAndInit()
	}

	return data, err
}

/*
eofHandler - handle EOF case

Returns:

	(hasNewLog, dataFromOldLog, error)
	- hasNewLog: true, if there is new log file
	- dataFromOldLog: data read from old log
	- error: != nil, if some error happens
*/
func (lr *LogFileReader) eofHandler() (bool, []byte, error) {
	var data []byte
	var err error

	// check new inode
	if !lr.isLogCut() {
		// no new inode
		return false, data, nil
	}

	// have new inode
	// try again to read from old file (all left data)
	data, err = lr.fileRead(0)

	// relocate the log file
	lr.logRelocate()

	return true, data, err
}

// close logFd held by logReader
// This func is ONLY for unit-testing
func (lr *LogFileReader) logFdClose() {
	if lr.logFd != nil {
		lr.logFd.Close()
		lr.logFd = nil
	}
}

func (lr *LogFileReader) SetMaxSizePerBatch(maxSizePerBatch int) {
	lr.MaxSizePerBatch = maxSizePerBatch
}
