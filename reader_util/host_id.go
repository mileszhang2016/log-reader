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
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/bfenetworks/go-lib/log"
)

var (
	hostId string
	once   sync.Once
)

func getHostname() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("failed to get host: %v", err)
	}
	return hostname, nil
}

func getNetworkNamespace() (string, error) {
	linkTarget, err := os.Readlink("/proc/self/ns/net")
	if err != nil {
		return "", fmt.Errorf("failed to get network namespace: %v", err)
	}

	parts := strings.Split(linkTarget, ":")
	if len(parts) != 2 || parts[0] != "net" {
		return "", fmt.Errorf("invalid network namespace format: %s", linkTarget)
	}

	inode := strings.Trim(parts[1], "[]")
	return inode, nil
}

func getHostIdImpl() string {
	hostname, err := getHostname()
	if err != nil {
		log.Logger.Warn("Failed to getHostname, err:%s", err.Error())
		hostname = "default"
	}

	netns, err := getNetworkNamespace()
	if err != nil {
		log.Logger.Warn("Failed to getNetworkNamespace, err:%s", err.Error())
		netns = "default"
	}

	return hostname + "_" + netns
}

func GetHostId() string {
	once.Do(func() {
		hostId = getHostIdImpl()
	})

	return hostId
}
