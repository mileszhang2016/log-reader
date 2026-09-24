// Copyright(c) 2026 The Rainway AI Gateway (壬远AI网关) Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

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
	"github.com/rainway-ai-gateway/log-reader/reader_modules/mod_fields"
)

// ApikeyTagJSON API Key tag
type ApikeyTagJSON = mod_fields.ApikeyTagJSON

// AiRateLimitHitJSON AI rate limit hit record
type AiRateLimitHitJSON = mod_fields.AiRateLimitHitJSON

// AIRouteRuleHitJSON AI routing rule hit record
type AIRouteRuleHitJSON = mod_fields.AIRouteRuleHitJSON

// ClusterKeyNameJSON cluster and key name pair
type ClusterKeyNameJSON = mod_fields.ClusterKeyNameJSON

// HttpHeaderJSON HTTP request or response header
type HttpHeaderJSON = mod_fields.HttpHeaderJSON

// ConvertBfeLogToJSON converts BfeLog to JSON bytes using the given output field set
func ConvertBfeLogToJSON(bfeLog *bfe_access_pb.BfeLog, of *OutputFields) ([]byte, error) {
	if of == nil {
		of = DefaultOutputFields()
	}

	m := make(map[string]interface{}, len(of.Set))
	for fieldName := range of.Set {
		val, isZero := mod_fields.Extract(fieldName, bfeLog)
		if isZero {
			//continue
		}
		m[fieldName] = val
	}

	return json.Marshal(m)
}
