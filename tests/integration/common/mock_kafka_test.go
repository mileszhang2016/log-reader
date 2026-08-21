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
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestMockKafka_ReceiveMessages(t *testing.T) {
	mk := NewMockKafka(t, "test_topic")
	defer mk.Close()

	w := &kafka.Writer{
		Addr:         kafka.TCP(mk.Addr()),
		Topic:        "test_topic",
		Balancer:     &kafka.Hash{},
		BatchSize:    1,
		BatchTimeout: 100 * time.Millisecond,
		MaxAttempts:  1,
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := w.WriteMessages(ctx, kafka.Message{Value: []byte("hello")}); err != nil {
		t.Fatalf("write message failed: %v", err)
	}

	msgs, ok := mk.WaitForMessages(1, 5*time.Second)
	if !ok {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if string(msgs[0]) != "hello" {
		t.Errorf("expected 'hello', got %q", string(msgs[0]))
	}
}
