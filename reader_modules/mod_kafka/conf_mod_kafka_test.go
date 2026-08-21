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

package mod_kafka

import (
	"os"
	"path/filepath"
	"testing"
)

func writeTempKafkaConf(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mod_kafka.conf")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestConfModKafkaCheck_Defaults(t *testing.T) {
	cfg := &ConfModKafka{
		Kafka: ConfKafka{
			Brokers:     "kafka1:9092",
			Topic:       "test_topic",
			Compression: "zstd",
		},
	}
	if err := ConfModKafkaCheck(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Kafka.BatchSize != 1000 {
		t.Errorf("BatchSize should default to 1000, got %d", cfg.Kafka.BatchSize)
	}
	if cfg.Kafka.LingerMs != 100 {
		t.Errorf("LingerMs should default to 100, got %d", cfg.Kafka.LingerMs)
	}
	if cfg.Kafka.MaxRetries != 3 {
		t.Errorf("MaxRetries should default to 3, got %d", cfg.Kafka.MaxRetries)
	}
}

func TestConfModKafkaCheck_EmptyBrokers(t *testing.T) {
	cfg := &ConfModKafka{
		Kafka: ConfKafka{Topic: "test_topic"},
	}
	if err := ConfModKafkaCheck(cfg); err == nil {
		t.Error("expected error for empty brokers")
	}
}

func TestConfModKafkaCheck_EmptyTopic(t *testing.T) {
	cfg := &ConfModKafka{
		Kafka: ConfKafka{Brokers: "kafka1:9092"},
	}
	if err := ConfModKafkaCheck(cfg); err == nil {
		t.Error("expected error for empty topic")
	}
}

func TestConfModKafkaCheck_InvalidCompression(t *testing.T) {
	cfg := &ConfModKafka{
		Kafka: ConfKafka{
			Brokers:     "kafka1:9092",
			Topic:       "test_topic",
			Compression: "invalid",
		},
	}
	if err := ConfModKafkaCheck(cfg); err == nil {
		t.Error("expected error for invalid compression")
	}
}

func TestConfModKafkaCheck_AllCompressions(t *testing.T) {
	for _, comp := range []string{"", "none", "snappy", "gzip", "lz4", "zstd"} {
		cfg := &ConfModKafka{
			Kafka: ConfKafka{
				Brokers:     "kafka1:9092",
				Topic:       "test_topic",
				Compression: comp,
			},
		}
		if err := ConfModKafkaCheck(cfg); err != nil {
			t.Errorf("compression %q should be valid, got err: %v", comp, err)
		}
	}
}

func TestLoadConfig(t *testing.T) {
	content := `
[Basic]
DataPath = kafka_config.data
OpenDebug = true

[kafka]
Brokers = kafka1:9092,kafka2:9092
Topic = test_topic
DeadLetterTopic = test_dlq
Compression = snappy
BatchSize = 200
LingerMs = 50
MaxRetries = 5
`
	path := writeTempKafkaConf(t, content)
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.Basic.DataPath != "kafka_config.data" {
		t.Errorf("DataPath = %q, want kafka_config.data", cfg.Basic.DataPath)
	}
	if !cfg.Basic.OpenDebug {
		t.Error("OpenDebug should be true")
	}
	if cfg.Kafka.Brokers != "kafka1:9092,kafka2:9092" {
		t.Errorf("Brokers = %q", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "test_topic" {
		t.Errorf("Topic = %q", cfg.Kafka.Topic)
	}
	if cfg.Kafka.DeadLetterTopic != "test_dlq" {
		t.Errorf("DeadLetterTopic = %q", cfg.Kafka.DeadLetterTopic)
	}
	if cfg.Kafka.Compression != "snappy" {
		t.Errorf("Compression = %q", cfg.Kafka.Compression)
	}
	if cfg.Kafka.BatchSize != 200 {
		t.Errorf("BatchSize = %d", cfg.Kafka.BatchSize)
	}
	if cfg.Kafka.LingerMs != 50 {
		t.Errorf("LingerMs = %d", cfg.Kafka.LingerMs)
	}
	if cfg.Kafka.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d", cfg.Kafka.MaxRetries)
	}
}

func TestLoadConfig_Invalid(t *testing.T) {
	path := writeTempKafkaConf(t, "[kafka]\nBrokers = \nTopic = \n")
	if _, err := LoadConfig(path); err == nil {
		t.Error("expected error for invalid config")
	}
}
