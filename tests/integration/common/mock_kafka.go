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
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/segmentio/kafka-go/protocol/produce"
)

// MockKafka is a minimal Kafka server that satisfies kafka-go Writer.
type MockKafka struct {
	t      *testing.T
	ln     net.Listener
	addr   string
	mu     sync.Mutex
	done   chan struct{}
	closed bool

	// Topics is the list of topics advertised in Metadata responses.
	Topics []string

	// Messages holds all record values received by the mock server.
	Messages [][]byte
}

// NewMockKafka starts a new fake Kafka server on a random port.
func NewMockKafka(t *testing.T, topics ...string) *MockKafka {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	mk := &MockKafka{
		t:      t,
		ln:     ln,
		addr:   ln.Addr().String(),
		done:   make(chan struct{}),
		Topics: topics,
	}

	go mk.serve()
	return mk
}

// Addr returns the broker address in host:port form.
func (mk *MockKafka) Addr() string {
	return mk.addr
}

// Close stops the mock server.
func (mk *MockKafka) Close() {
	mk.mu.Lock()
	if mk.closed {
		mk.mu.Unlock()
		return
	}
	mk.closed = true
	mk.mu.Unlock()

	mk.ln.Close()
	select {
	case <-mk.done:
	case <-time.After(2 * time.Second):
		mk.t.Logf("mock kafka close timeout")
	}
}

// ReceivedMessages returns a copy of all received record values.
func (mk *MockKafka) ReceivedMessages() [][]byte {
	mk.mu.Lock()
	defer mk.mu.Unlock()
	out := make([][]byte, len(mk.Messages))
	for i, m := range mk.Messages {
		out[i] = append([]byte(nil), m...)
	}
	return out
}

func (mk *MockKafka) serve() {
	defer close(mk.done)
	for {
		conn, err := mk.ln.Accept()
		if err != nil {
			mk.mu.Lock()
			closed := mk.closed
			mk.mu.Unlock()
			if closed {
				return
			}
			mk.t.Logf("mock kafka accept error: %v", err)
			return
		}
		go mk.handleConn(conn)
	}
}

func (mk *MockKafka) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		apiVersion, correlationID, _, msg, err := protocol.ReadRequest(conn)
		if err != nil {
			// Connection closed or read error, expected on shutdown.
			return
		}

		var resp protocol.Message
		switch req := msg.(type) {
		case *apiversions.Request:
			resp = mk.handleApiVersions(apiVersion)
		case *metadata.Request:
			resp = mk.handleMetadata(apiVersion, req)
		case *produce.Request:
			resp = mk.handleProduce(apiVersion, req)
		default:
			mk.t.Logf("mock kafka: unsupported request type %T", msg)
			return
		}

		if err := protocol.WriteResponse(conn, apiVersion, correlationID, resp); err != nil {
			mk.t.Logf("mock kafka write response error: %v", err)
			return
		}
	}
}

func (mk *MockKafka) handleApiVersions(_ int16) *apiversions.Response {
	return &apiversions.Response{
		ErrorCode: 0,
		ApiKeys: []apiversions.ApiKeyResponse{
			{ApiKey: int16(protocol.Produce), MinVersion: 0, MaxVersion: 8},
			{ApiKey: int16(protocol.Metadata), MinVersion: 0, MaxVersion: 8},
			{ApiKey: int16(protocol.ApiVersions), MinVersion: 0, MaxVersion: 2},
		},
		ThrottleTimeMs: 0,
	}
}

func (mk *MockKafka) handleMetadata(apiVersion int16, req *metadata.Request) *metadata.Response {
	host, portStr, _ := net.SplitHostPort(mk.addr)
	port := int32(0)
	fmt.Sscanf(portStr, "%d", &port)

	resp := &metadata.Response{
		Brokers: []metadata.ResponseBroker{
			{NodeID: 1, Host: host, Port: port},
		},
		Topics: []metadata.ResponseTopic{},
	}

	topics := req.TopicNames
	if len(topics) == 0 {
		topics = mk.Topics
	}

	for _, topicName := range topics {
		resp.Topics = append(resp.Topics, metadata.ResponseTopic{
			ErrorCode: 0,
			Name:      topicName,
			Partitions: []metadata.ResponsePartition{
				{
					ErrorCode:      0,
					PartitionIndex: 0,
					LeaderID:       1,
					ReplicaNodes:   []int32{1},
					IsrNodes:       []int32{1},
				},
			},
		})
	}

	return resp
}

func (mk *MockKafka) handleProduce(_ int16, req *produce.Request) *produce.Response {
	mk.mu.Lock()
	defer mk.mu.Unlock()

	resp := &produce.Response{Topics: []produce.ResponseTopic{}}
	for _, topic := range req.Topics {
		rt := produce.ResponseTopic{Topic: topic.Topic, Partitions: []produce.ResponsePartition{}}
		for _, partition := range topic.Partitions {
			reader := partition.RecordSet.Records
			for {
				rec, err := reader.ReadRecord()
				if err != nil {
					break
				}
				value, err := protocol.ReadAll(rec.Value)
				if err != nil {
					mk.t.Logf("read record value failed: %v", err)
					continue
				}
				mk.Messages = append(mk.Messages, append([]byte(nil), value...))
			}
			rt.Partitions = append(rt.Partitions, produce.ResponsePartition{
				Partition:  partition.Partition,
				ErrorCode:  0,
				BaseOffset: 0,
			})
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return resp
}

// WaitForMessages blocks until at least n messages are received or timeout.
func (mk *MockKafka) WaitForMessages(n int, timeout time.Duration) ([][]byte, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := mk.ReceivedMessages()
		if len(msgs) >= n {
			return msgs, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return mk.ReceivedMessages(), false
}

// Ensure MockKafka is closed when test context is done.
func (mk *MockKafka) WithContext(ctx context.Context) {
	go func() {
		<-ctx.Done()
		mk.Close()
	}()
}
