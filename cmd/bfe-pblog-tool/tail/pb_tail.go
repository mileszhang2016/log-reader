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

package tail

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/bfe_log_reader"
)

// Default values and caps for tail behavior.
const (
	defaultTailN          = 10
	defaultFollowInterval = 500 * time.Millisecond
	MaxTailRecords        = 100000 // soft cap to avoid excessive memory usage
)

// ErrTailStop signals caller wants to stop tail follow loop gracefully.
var ErrTailStop = errors.New("tail stop")

// TailOptions defines configuration for tailing a pb log file.
type TailOptions struct {
	N        int           // number of last records to show; if 0 use defaultTailN
	Follow   bool          // whether to continue streaming newly appended records
	Interval time.Duration // polling interval when Follow is true; if 0 use defaultFollowInterval
}

// validateTailOptions applies defaults and validates values.
// Returns possibly adjusted options or error.
func validateTailOptions(opts TailOptions) (TailOptions, error) {
	if opts.N < 0 {
		return opts, fmt.Errorf("invalid n: %d", opts.N)
	}
	if opts.N == 0 {
		opts.N = defaultTailN
	}
	if opts.N > MaxTailRecords {
		// Cap but do not error; caller may choose to warn user.
		opts.N = MaxTailRecords
	}
	if opts.Follow {
		if opts.Interval < 0 {
			return opts, fmt.Errorf("invalid follow interval: %s", opts.Interval)
		}
		if opts.Interval == 0 {
			opts.Interval = defaultFollowInterval
		}
	}
	return opts, nil
}

// Placeholder signatures for subsequent tasks (to be implemented in later steps).
// Keeping them here allows earlier compilation once referenced by CLI code later.
// Actual logic will be added in tasks 2-4.

// tailLastN will read and emit the last N records; returns final offset & inode.
// Implementation pending.
func tailLastN(fp string, n int, emit func(*bfe_access_pb.BfeLog) error) (int64, uint64, error) {
	if n <= 0 {
		return 0, 0, fmt.Errorf("invalid n: %d", n)
	}
	f, err := os.Open(fp)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := fi.Size()
	if size == 0 {
		// empty file
		inode := getInode(fi)
		return 0, inode, nil
	}

	// heuristic reverse chunk growth
	const (
		initialChunk = int64(8 * 1024 * 1024)  // 8MB
		maxChunk     = int64(64 * 1024 * 1024) // 64MB cap
	)
	var (
		offset        = size
		chunkSize     = initialChunk
		buffer        = make([]byte, 0)
		parsedRecords []*bfe_access_pb.BfeLog
		leftover      []byte // remainder from PbBuffParse across iterations
	)

	// We collect chunks backwards and rebuild forward parse buffer each iteration.
	for offset > 0 && len(parsedRecords) < n {
		if chunkSize > offset {
			chunkSize = offset
		}
		offset -= chunkSize
		// seek & read chunk
		_, err = f.Seek(offset, io.SeekStart)
		if err != nil {
			return 0, 0, fmt.Errorf("seek failed: %w", err)
		}
		chunk := make([]byte, chunkSize)
		_, err = io.ReadFull(f, chunk)
		if err != nil {
			return 0, 0, fmt.Errorf("read failed: %w", err)
		}

		// prepend chunk before previous buffer to maintain chronological order
		// structure: [older ... newer]
		buffer = append(chunk, append(leftover, buffer...)...)
		// parse repeatedly
		var loopBuf = buffer
		for len(loopBuf) > 0 && len(parsedRecords) < n {
			records, rem := bfe_log_reader.PbBuffParse(loopBuf, nil)
			if len(records) == 0 && len(rem) == len(loopBuf) {
				// no progress (partial record), store remainder and break
				leftover = rem
				break
			}
			parsedRecords = append(parsedRecords, records...)
			loopBuf = rem
			leftover = rem
		}

		// grow chunk size exponentially up to cap
		chunkSize *= 2
		if chunkSize > maxChunk {
			chunkSize = maxChunk
		}
	}

	// We may have collected more than needed; keep last n of parsedRecords
	if len(parsedRecords) > n {
		parsedRecords = parsedRecords[len(parsedRecords)-n:]
	}

	// Emit in chronological order
	for _, r := range parsedRecords {
		if r == nil {
			continue
		}
		if err := emit(r); err != nil {
			return size, getInode(fi), err
		}
	}

	return size, getInode(fi), nil
}

// followFrom will continue emitting new records starting from offset until stopped.
// Implementation pending.
func followFrom(fp string, startOffset int64, startInode uint64, interval time.Duration, emit func(*bfe_access_pb.BfeLog) error) error {
	f, err := os.Open(fp)
	if err != nil {
		return err
	}
	defer f.Close()

	// Seek to startOffset (end of file after initial tail)
	if startOffset > 0 {
		if _, err = f.Seek(startOffset, io.SeekStart); err != nil {
			return fmt.Errorf("initial seek failed: %w", err)
		}
	}

	buffer := make([]byte, 0)
	var remainder []byte
	var currentOffset = startOffset
	var currentInode = startInode

	for {
		// Stat file each loop for size/inode
		fi, statErr := os.Stat(fp)
		if statErr != nil {
			// transient error; sleep and retry
			time.Sleep(interval)
			continue
		}
		size := fi.Size()
		inode := getInode(fi)

		// Rotation or truncate detection
		if inode != 0 && currentInode != 0 && inode != currentInode || size < currentOffset {
			// reopen and reset
			f.Close()
			f, err = os.Open(fp)
			if err != nil {
				return fmt.Errorf("reopen after rotate failed: %w", err)
			}
			currentInode = inode
			currentOffset = 0
			remainder = nil
			buffer = buffer[:0]
		}

		// Read new appended data if any
		if size > currentOffset {
			if _, err = f.Seek(currentOffset, io.SeekStart); err != nil {
				return fmt.Errorf("seek append failed: %w", err)
			}
			toRead := size - currentOffset
			// read in chunks to avoid huge single allocation
			for toRead > 0 {
				chunkSize := toRead
				if chunkSize > bfe_log_reader.MAX_BUFF_SIZE {
					chunkSize = bfe_log_reader.MAX_BUFF_SIZE
				}
				chunk := make([]byte, chunkSize)
				n, rErr := io.ReadFull(f, chunk)
				if rErr != nil && rErr != io.ErrUnexpectedEOF {
					if rErr == io.EOF { // no more data
						break
					}
					return fmt.Errorf("read append failed: %w", rErr)
				}
				chunk = chunk[:n]
				currentOffset += int64(n)
				toRead -= int64(n)
				// append to buffer (include leftover remainder first)
				buffer = append(remainder, chunk...)
				loopBuf := buffer
				for len(loopBuf) > 0 {
					records, rem := bfe_log_reader.PbBuffParse(loopBuf, nil)
					if len(records) == 0 && len(rem) == len(loopBuf) {
						// partial record only; store remainder and break awaiting more data
						remainder = rem
						break
					}
					for _, r := range records {
						if r == nil {
							continue
						}
						if err := emit(r); err != nil {
							if errors.Is(err, ErrTailStop) {
								return ErrTailStop
							}
							return err
						}
					}
					loopBuf = rem
					remainder = rem
				}
			}
		}

		// Sleep until next poll
		time.Sleep(interval)
	}
}

// PblogTail orchestrates tailing (last N then optional follow).
// Implementation will be completed after tailLastN and followFrom are in place.
func PblogTail(fp string, opts TailOptions, emit func(string) error) error {
	validated, err := validateTailOptions(opts)
	if err != nil {
		return err
	}
	// First fetch last N
	endOffset, inode, err := tailLastN(fp, validated.N, func(r *bfe_access_pb.BfeLog) error {
		return emit(r.String())
	})
	if err != nil {
		return err
	}
	if !validated.Follow {
		return nil
	}
	// Follow mode
	fErr := followFrom(fp, endOffset, inode, validated.Interval, func(r *bfe_access_pb.BfeLog) error {
		return emit(r.String())
	})
	if errors.Is(fErr, ErrTailStop) {
		return nil
	}
	return fErr
}
