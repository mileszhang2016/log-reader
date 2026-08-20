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

package reader_util

import (
	"strings"
	"sync"
	"testing"
)

func TestGetHostname(t *testing.T) {
	hostname, err := getHostname()
	if err != nil {
		t.Fatalf("getHostname failed: %v", err)
	}
	if hostname == "" {
		t.Error("hostname should not be empty")
	}
}

func TestGetNetworkNamespace(t *testing.T) {
	ns, err := getNetworkNamespace()
	if err != nil {
		// /proc/self/ns/net is only available on Linux
		t.Skipf("getNetworkNamespace not supported on this OS: %v", err)
	}
	if ns == "" {
		t.Error("network namespace should not be empty")
	}
}

func TestGetHostIdImpl(t *testing.T) {
	id := getHostIdImpl()
	if id == "" {
		t.Error("host id should not be empty")
	}
	if !strings.Contains(id, "_") {
		t.Errorf("host id should contain underscore separator, got %q", id)
	}
}

func TestGetHostId(t *testing.T) {
	// reset cached hostId for testing
	hostId = ""
	once = sync.Once{}

	id1 := GetHostId()
	if id1 == "" {
		t.Error("GetHostId should not return empty")
	}

	id2 := GetHostId()
	if id1 != id2 {
		t.Errorf("GetHostId should return cached value: %q != %q", id1, id2)
	}
}
