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

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "kafka_config.data")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadKafkaDataConfig_FileNotExist(t *testing.T) {
	cfg, err := LoadKafkaDataConfig("/nonexistent/path/kafka_config.data")
	if err == nil {
		t.Fatalf("expected error for nonexistent file")
	}
	if cfg != nil {
		t.Fatal("expected nil config for nonexistent file")
	}
}

func TestLoadKafkaDataConfig_EmptyFile(t *testing.T) {
	path := writeTempFile(t, "")
	cfg, err := LoadKafkaDataConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	of := cfg.ResolveFields()
	expectedCount := len(DefaultFields())
	if len(of.Set) != expectedCount {
		t.Fatalf("expected %d fields (default + extra required), got %d", expectedCount, len(of.Set))
	}
}

func TestResolveFields_Require(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode: "require",
		},
	}
	of := cfg.ResolveFields()
	required := RequiredFields()
	if len(of.Set) != len(required) {
		t.Fatalf("expected %d required fields, got %d", len(required), len(of.Set))
	}
	for _, name := range required {
		if !of.Set[name] {
			t.Errorf("required field %q not in output set", name)
		}
	}
}

func TestResolveFields_Default(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode: "default",
		},
	}
	of := cfg.ResolveFields()
	// default (48) = 48
	expectedCount := len(DefaultFields())
	if len(of.Set) != expectedCount {
		t.Fatalf("expected %d fields, got %d", expectedCount, len(of.Set))
	}
}

func TestResolveFields_All(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode: "all",
		},
	}
	of := cfg.ResolveFields()
	all := AllFields()
	if len(of.Set) != len(all) {
		t.Fatalf("expected %d fields, got %d", len(all), len(of.Set))
	}
}

func TestResolveFields_Customized(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode:  "customized",
			FieldNames: []string{"ai_apikey_id", "ai_requested_model", "ai_target_model", "ai_input_tokens", "ai_provider", "referrer"},
		},
	}
	of := cfg.ResolveFields()

	if !of.Set["ai_apikey_id"] {
		t.Error("expected ai_apikey_id in output")
	}
	if !of.Set["ai_requested_model"] {
		t.Error("expected ai_requested_model in output")
	}
	if !of.Set["ai_target_model"] {
		t.Error("expected ai_target_model in output")
	}
	if !of.Set["ai_input_tokens"] {
		t.Error("expected ai_input_tokens in output")
	}
	if !of.Set["ai_provider"] {
		t.Error("expected ai_provider in output")
	}
	if !of.Set["referrer"] {
		t.Error("expected referrer in output")
	}

	for _, name := range RequiredFields() {
		if !of.Set[name] {
			t.Errorf("required field %q not in output", name)
		}
	}

	if of.Set["ai_stream"] {
		t.Error("ai_stream should not be in output")
	}
}

func TestResolveFields_CustomizedEmptyNames(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode:  "customized",
			FieldNames: []string{},
		},
	}
	of := cfg.ResolveFields()
	required := RequiredFields()
	if len(of.Set) != len(required) {
		t.Fatalf("expected %d required fields (empty customized), got %d", len(required), len(of.Set))
	}
}

func TestResolveFields_DuplicateNames(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode:  "customized",
			FieldNames: []string{"ai_apikey_id", "ai_apikey_id", "ai_apikey_id"},
		},
	}
	of := cfg.ResolveFields()
	count := 0
	for _, name := range RequiredFields() {
		if name == "ai_apikey_id" {
			continue
		}
		if of.Set[name] {
			count++
		}
	}
	if !of.Set["ai_apikey_id"] {
		t.Error("ai_apikey_id should be in output")
	}
}

func TestResolveFields_UnknownFieldMode(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode: "invalid_mode",
		},
	}
	of := cfg.ResolveFields()
	expectedCount := len(DefaultFields())
	if len(of.Set) != expectedCount {
		t.Fatalf("expected fallback to default (%d fields), got %d", expectedCount, len(of.Set))
	}
}

func TestResolveFields_UnknownFieldName(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode:  "customized",
			FieldNames: []string{"ai_apikey_id", "nonexistent_field", "another_fake"},
		},
	}
	of := cfg.ResolveFields()
	if !of.Set["ai_apikey_id"] {
		t.Error("ai_apikey_id should be in output")
	}
	if of.Set["nonexistent_field"] {
		t.Error("nonexistent_field should not be in output")
	}
}

func TestResolveFields_EmptyMode(t *testing.T) {
	cfg := &KafkaDataConfig{
		ConfFields: ConfKafkaFields{
			FieldMode: "",
		},
	}
	of := cfg.ResolveFields()
	expectedCount := len(DefaultFields())
	if len(of.Set) != expectedCount {
		t.Fatalf("expected default for empty mode (%d fields), got %d", expectedCount, len(of.Set))
	}
}

func TestDefaultOutputFields(t *testing.T) {
	of := DefaultOutputFields()
	def := DefaultFields()
	if len(of.Set) != len(def) {
		t.Fatalf("DefaultOutputFields: expected %d, got %d", len(def), len(of.Set))
	}
}
