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

package lr02

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
	"github.com/rainway-ai-gateway/log-reader/tests/integration/common"
)

// testEnv holds all resources for a single LR02 integration test.
type testEnv struct {
	t          *testing.T
	processEnv *common.ProcessEnv
	workDir    string
	logFile    string
	logGen     *common.LogGenerator
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	e := &testEnv{t: t}
	e.processEnv = common.NewProcessEnv(t)
	e.processEnv.BuildPblogTool()
	e.workDir = e.processEnv.WorkDir()
	e.logFile = filepath.Join(e.workDir, "pb_access3.log")
	return e
}

func (e *testEnv) writeTestLogs(count int) {
	e.t.Helper()
	if e.logGen == nil {
		e.logGen = common.NewLogGenerator(e.t, e.logFile)
	}
	for i := 0; i < count; i++ {
		log := common.MakeRequestLog(uint64(10000+i), bfe_access_pb.ProductID_BFE, "test.example.org", "/v1/test", "test-model")
		e.logGen.MustWriteBfeLog(e.t, log)
	}
}

func (e *testEnv) createEmptyFile(name string) string {
	e.t.Helper()
	path := filepath.Join(e.workDir, name)
	f, err := os.Create(path)
	if err != nil {
		e.t.Fatalf("create empty file failed: %v", err)
	}
	f.Close()
	return path
}

func (e *testEnv) Close() {
	if e.logGen != nil {
		e.logGen.Close()
	}
	e.processEnv.Cleanup()
}

// countRecords counts non-empty output lines, excluding "Time taken:" lines.
func countRecords(stdout string) int {
	lines := strings.Split(stdout, "\n")
	count := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "Time taken:") {
			count++
		}
	}
	return count
}

func TestLR02_CatBasic(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	e.writeTestLogs(9)

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("cat", e.logFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("cat failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}
	if stdout == "" {
		t.Fatal("expected non-empty stdout")
	}
	if !strings.Contains(stdout, "Time taken:") {
		t.Fatal("expected 'Time taken:' in stdout")
	}

	records := countRecords(stdout)
	if records != 9 {
		t.Fatalf("expected 9 records, got %d", records)
	}
}

func TestLR02_CatWithLineNumbers(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	e.writeTestLogs(9)

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("cat", "-n", e.logFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("cat -n failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	dataLines := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Time taken:") {
			continue
		}
		dataLines++
		// Each data line should start with a number followed by a space.
		if !strings.Contains(line, " ") {
			t.Errorf("expected line number prefix, got: %s", line)
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if parts[0] == "" {
			t.Errorf("missing line number in: %s", line)
		}
	}
	if dataLines != 9 {
		t.Fatalf("expected 9 data lines, got %d", dataLines)
	}
}

func TestLR02_CatNonexistentFile(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	_, stderr, exitCode, err := e.processEnv.RunPblogTool("cat", "/nonexistent/path/to/file.log")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
	if exitCode == 0 {
		t.Fatal("expected non-zero exit code for nonexistent file")
	}
	if stderr == "" && err.Error() == "" {
		t.Fatal("expected error message for nonexistent file")
	}

	// The error should mention the file or "stat" or "no such file".
	combined := stderr + err.Error()
	if !strings.Contains(strings.ToLower(combined), "stat") &&
		!strings.Contains(strings.ToLower(combined), "no such") &&
		!strings.Contains(strings.ToLower(combined), "nonexistent") {
		t.Logf("expected error message about nonexistent file, got: %s", combined)
	}
}

func TestLR02_CatEmptyFile(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	emptyFile := e.createEmptyFile("empty.log")

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("cat", emptyFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("cat empty file failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}

	records := countRecords(stdout)
	if records != 0 {
		t.Fatalf("expected 0 records for empty file, got %d", records)
	}
}

func TestLR02_TailDefault(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	e.writeTestLogs(9)

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("tail", e.logFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("tail failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}

	records := countRecords(stdout)
	if records != 9 {
		t.Fatalf("expected 9 records (file has only 9, default N=10), got %d", records)
	}
}

func TestLR02_TailN3(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	e.writeTestLogs(9)

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("tail", "-n", "3", e.logFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("tail -n 3 failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}

	records := countRecords(stdout)
	if records != 3 {
		t.Fatalf("expected 3 records, got %d", records)
	}
}

func TestLR02_TailN0(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	e.writeTestLogs(9)

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("tail", "-n", "0", e.logFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("tail -n 0 failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}

	// tail -n 0 should default to 10, and since file has 9 records, we get all 9.
	records := countRecords(stdout)
	if records != 9 {
		t.Fatalf("expected 9 records (defaults to 10, file has 9), got %d", records)
	}
}

func TestLR02_TailNonexistentFile(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	_, stderr, exitCode, err := e.processEnv.RunPblogTool("tail", "/nonexistent/path/to/file.log")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
	if exitCode == 0 {
		t.Fatal("expected non-zero exit code for nonexistent file")
	}
	if stderr == "" && err.Error() == "" {
		t.Fatal("expected error message for nonexistent file")
	}
}

func TestLR02_TailEmptyFile(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	emptyFile := e.createEmptyFile("empty.log")

	stdout, stderr, exitCode, err := e.processEnv.RunPblogTool("tail", emptyFile)
	if err != nil {
		t.Logf("stderr: %s", stderr)
		t.Fatalf("tail empty file failed: %v", err)
	}
	if exitCode != 0 {
		t.Fatalf("expected exitCode 0, got %d", exitCode)
	}

	records := countRecords(stdout)
	if records != 0 {
		t.Fatalf("expected 0 records for empty file, got %d", records)
	}
}

func TestLR02_TailFollowNewData(t *testing.T) {
	e := newTestEnv(t)
	defer e.Close()

	// Write 5 initial records.
	e.writeTestLogs(5)

	// Start tail in follow mode.
	stdoutReader, _, stop, err := e.processEnv.StartPblogTool("tail", "-n", "5", "-f", "--interval", "100", e.logFile)
	if err != nil {
		t.Fatalf("start tail -f failed: %v", err)
	}
	defer stop()

	// Wait briefly for the initial 5 records to be output.
	time.Sleep(500 * time.Millisecond)

	// Append 3 more records.
	e.writeTestLogs(3)

	// Read stdout with a timeout, collecting new lines.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	linesCh := make(chan string, 100)
	doneCh := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdoutReader)
		for scanner.Scan() {
			linesCh <- scanner.Text()
		}
		close(doneCh)
	}()

	var allLines []string
	timeout := false
	for !timeout {
		select {
		case line := <-linesCh:
			allLines = append(allLines, line)
		case <-ctx.Done():
			timeout = true
		case <-doneCh:
			timeout = true
		}
	}

	// Count non-empty records.
	recordCount := 0
	for _, line := range allLines {
		line = strings.TrimSpace(line)
		if line != "" {
			recordCount++
		}
	}

	// We should see at least 8 records (5 initial + 3 new).
	// The tail -n 5 initially shows the last 5, then follow mode picks up 3 more.
	if recordCount < 8 {
		t.Fatalf("expected at least 8 records (5 initial + 3 new), got %d", recordCount)
	}
}