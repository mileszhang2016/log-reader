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

package cat

import (
	"sync"

	"github.com/rainway-ai-gateway/log-reader/bfe_log_reader"
)

func PblogCat(fp string, callback func(result []string)) error {
	pbr := bfe_log_reader.NewPbLogReader(fp, nil, "")
	err := pbr.LogFileOpen()
	defer pbr.CloseFileAndInit()
	if err != nil {
		return err
	}
	_, err = pbr.LogFd.Seek(0, 0)
	if err != nil {
		return err
	}

	bufChan := make(chan []byte, 1024)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			buf, err := pbr.FileRead(8192)
			if err != nil || len(buf) == 0 {
				close(bufChan)
				return
			}
			bufChan <- buf
		}
	}()

	go func() {
		defer wg.Done()
		for buf := range bufChan {
			pbr.DataBuffer = append(pbr.DataBuffer, buf...)
			records := pbr.DataBufferParse()
			var res = make([]string, 0)
			for _, r := range records {
				line := r.String()
				res = append(res, line)
			}
			callback(res)
		}
	}()

	wg.Wait()
	return nil
}
