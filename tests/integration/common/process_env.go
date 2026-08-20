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

package common

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const binCacheDir = ".integration-test-bin"

// ProcessEnv manages the log-reader binary and runtime directories.
type ProcessEnv struct {
	t       *testing.T
	workDir string
	binPath string
}

// NewProcessEnv creates a new test environment.
func NewProcessEnv(t *testing.T) *ProcessEnv {
	t.Helper()
	workDir, err := os.MkdirTemp("", "log-reader-integration-*")
	if err != nil {
		t.Fatalf("create work dir failed: %v", err)
	}

	return &ProcessEnv{
		t:       t,
		workDir: workDir,
	}
}

// WorkDir returns the temporary work directory.
func (p *ProcessEnv) WorkDir() string {
	return p.workDir
}

// Build compiles the log-reader binary and caches it.
func (p *ProcessEnv) Build() {
	t := p.t
	t.Helper()

	repoRoot, err := findRepoRoot()
	if err != nil {
		t.Fatalf("find repo root failed: %v", err)
	}

	cacheDir := filepath.Join(repoRoot, "tests", "integration", binCacheDir)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		t.Fatalf("create bin cache dir failed: %v", err)
	}

	binName := "log-reader" + goosBinSuffix()
	p.binPath = filepath.Join(cacheDir, binName)

	// Rebuild if binary does not exist.
	if _, err := os.Stat(p.binPath); err == nil {
		t.Logf("using cached binary: %s", p.binPath)
		return
	}

	t.Logf("building log-reader binary...")
	cmd := exec.Command("go", "build", "-o", p.binPath, "./main")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("build log-reader failed: %v", err)
	}
}

// StartLogReader starts the log-reader process with the given config and log directories.
// It returns the monitor port and a stop function.
func (p *ProcessEnv) StartLogReader(confDir, logDir string) (int, func()) {
	t := p.t
	t.Helper()

	monitorPort := freePort(t)

	cmd := exec.Command(p.binPath,
		"-c", confDir,
		"-l", logDir,
		"-b", // read from begin
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start log-reader failed: %v", err)
	}

	stop := func() {
		if cmd.Process != nil {
			cmd.Process.Signal(os.Interrupt)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-ctx.Done():
				cmd.Process.Kill()
				cmd.Wait()
			}
		}
	}

	// Wait briefly for the process to start listening.
	time.Sleep(500 * time.Millisecond)
	return monitorPort, stop
}

// Cleanup removes the work directory.
func (p *ProcessEnv) Cleanup() {
	os.RemoveAll(p.workDir)
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			module, err := os.ReadFile(filepath.Join(dir, "go.mod"))
			if err == nil {
				if string(module[:30]) == "module github.com/bfenetworks" || contains(string(module), "github.com/bfenetworks/log-reader") {
					return dir, nil
				}
			}
		}
		if dir == filepath.Dir(dir) {
			break
		}
	}
	return "", fmt.Errorf("repo root not found")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func goosBinSuffix() string {
	if os.PathSeparator == '\\' {
		return ".exe"
	}
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port failed: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
