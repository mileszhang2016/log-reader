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
	"errors"
	"os"

	"github.com/bfenetworks/go-lib/log"
	gcfg "gopkg.in/gcfg.v1"
)

// KafkaDataConfig is the top-level structure for kafka_config.data, extensible
type KafkaDataConfig struct {
	ConfFields ConfKafkaFields
}

// ConfKafkaFields field selection config
type ConfKafkaFields struct {
	FieldMode  string   // require | default | all | customized
	FieldNames []string // effective when FieldMode = customized
}

// OutputFields holds the resolved set of JSON fields to output
type OutputFields struct {
	Set map[string]bool
}

// LoadKafkaDataConfig loads kafka_config.data, returns nil if file does not exist
func LoadKafkaDataConfig(filePath string) (*KafkaDataConfig, error) {
	if _, err := os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	var cfg KafkaDataConfig
	if err := gcfg.ReadFileInto(&cfg, filePath); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// ResolveFields resolves the final output field set based on FieldMode and FieldNames
func (c *KafkaDataConfig) ResolveFields() *OutputFields {
	fieldSet := make(map[string]bool)

	// always include required fields
	for _, name := range RequiredFields() {
		fieldSet[name] = true
	}

	mode := c.ConfFields.FieldMode
	if mode == "" {
		mode = "default"
	}

	switch mode {
	case "require":
		// only required fields, already added above

	case "default":
		for _, name := range DefaultFields() {
			fieldSet[name] = true
		}

	case "all":
		for _, f := range AllFields() {
			fieldSet[f.Name] = true
		}

	case "customized":
		for _, name := range c.ConfFields.FieldNames {
			if name == "" {
				continue
			}
			if !IsValidField(name) {
				log.Logger.Warn("kafka_data_config: unknown field %q, ignored", name)
				continue
			}
			fieldSet[name] = true
		}

	default:
		log.Logger.Warn("kafka_data_config: unknown FieldMode %q, fallback to default", mode)
		for _, name := range DefaultFields() {
			fieldSet[name] = true
		}
	}

	return &OutputFields{Set: fieldSet}
}

// DefaultOutputFields returns OutputFields with the default field set
func DefaultOutputFields() *OutputFields {
	fieldSet := make(map[string]bool)
	for _, name := range DefaultFields() {
		fieldSet[name] = true
	}
	return &OutputFields{Set: fieldSet}
}
