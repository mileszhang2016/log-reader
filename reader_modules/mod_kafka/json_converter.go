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
	"encoding/json"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

// ApikeyTagJSON API Key tag
type ApikeyTagJSON struct {
	Tagname  string `json:"tagname"`
	Tagvalue string `json:"tagvalue"`
}

// AiRateLimitHitJSON AI rate limit hit record
type AiRateLimitHitJSON struct {
	RateLimitPolicyID string   `json:"rate_limit_policy_id"`
	RateLimitType     string   `json:"rate_limit_type"`
	RuleNames         []string `json:"rule_names"`
}

// AIRouteRuleHitJSON AI routing rule hit record
type AIRouteRuleHitJSON struct {
	RuleOwner     string `json:"rule_owner"`
	RuleOwnerType string `json:"rule_owner_type"`
	RuleName      string `json:"rule_name"`
}

// ClusterKeyNameJSON cluster and key name pair
type ClusterKeyNameJSON struct {
	ClusterName string `json:"cluster_name"`
	KeyName     string `json:"key_name"`
}

// HttpHeaderJSON HTTP request or response header
type HttpHeaderJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ConvertBfeLogToJSON converts BfeLog to JSON bytes using the given output field set
func ConvertBfeLogToJSON(bfeLog *bfe_access_pb.BfeLog, of *OutputFields) ([]byte, error) {
	if of == nil {
		of = DefaultOutputFields()
	}

	m := make(map[string]interface{}, len(of.Set))
	for fieldName := range of.Set {
		val, isZero := Extract(fieldName, bfeLog)
		if isZero {
			//continue
		}
		m[fieldName] = val
	}

	return json.Marshal(m)
}
