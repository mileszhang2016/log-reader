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
	"testing"

	bfe_access_pb "github.com/bfenetworks/bfe-access-pb/bfe_access_pb"
)

func makeBfeLog() *bfe_access_pb.BfeLog {
	logid := uint64(12345)
	timestamp := uint64(1782353290)
	product := bfe_access_pb.ProductID_BFE
	logTag := "req_BFE"

	return &bfe_access_pb.BfeLog{
		Product:   &product,
		Timestamp: &timestamp,
		Logid:     &logid,
		LogTag:    &logTag,
		LogType:   bfe_access_pb.BfeLogType_Request.Enum(),
		RequestLog: &bfe_access_pb.RequestLog{
			ErrCode:            strPtr(""),
			ErrMsg:             strPtr(""),
			ReqHeaderLen:       uint32Ptr(189),
			ReqBodyLen:         uint32Ptr(0),
			ClientIp:           uint32Ptr(0x0A000001), // 10.0.0.1
			ReqNum:             uint32Ptr(1),
			Proto:              strPtr("HTTP/1.1"),
			HeaderHost:         strPtr("example.com"),
			OriginUri:          strPtr("/api/v1/test"),
			Method:             strPtr("POST"),
			ContentType:        strPtr("application/json"),
			Cluster:            strPtr("cluster_ai"),
			SubCluster:         strPtr("pool_bj"),
			BackendInfo:        &bfe_access_pb.InstanceInfo{IpAddr: uint32Ptr(0x0A000002), Port: uint32Ptr(8080)},
			BackendRetry:       uint32Ptr(0),
			ResStatusCode:      uint32Ptr(200),
			ResHeaderLen:       uint32Ptr(154),
			ResBodyLen:         uint32Ptr(459),
			ResContentType:     strPtr("application/json"),
			AllTime:            uint32Ptr(11),
			ReadClientTime:     uint32Ptr(2),
			ClusterServeTime:   uint32Ptr(5),
			BackendServeTime:   uint32Ptr(4),
			WriteClientTime:    uint32Ptr(1),
			SessionOffsetTime:  uint32Ptr(9),
			ConnectBackendTime: uint32Ptr(1),
			ProxyDelayTime:     uint32Ptr(3),
			AiApikeyId:         strPtr("key-id-123"),
			AiApikeytags: []*bfe_access_pb.ApikeyTag{
				{Tagname: strPtr("dep"), Tagvalue: strPtr("ops"), Taglevel: int32Ptr(1)},
				{Tagname: strPtr("team"), Tagvalue: strPtr("bfe"), Taglevel: int32Ptr(2)},
			},
			AiRequestedModel:   strPtr("test-model"),
			AiTargetModel:      strPtr("gpt-5"),
			AiStream:           boolPtr(false),
			AiInputTokens:      int64Ptr(34),
			AiOutputTokens:     int64Ptr(182),
			AiTotalTokens:      int64Ptr(216),
			AiTtftUs:           int64Ptr(5486),
			AiTpotUs:           int64Ptr(3),
			AiProvider:         strPtr("openai"),
			AiRetryCount:       uint32Ptr(1),
			AiCostValue:        int64Ptr(5000),
			AiCostCurrency:     strPtr("USD"),
			AiRouteRuleHits: []*bfe_access_pb.AIRouteRuleHit{
				{
					RuleOwner:     strPtr("ak_user_a"),
					RuleOwnerType: strPtr("apikey"),
					RuleName:      strPtr("user_a-rule1"),
				},
			},
			AiClusterKeyNames: []*bfe_access_pb.ClusterKeyName{
				{
					ClusterName: strPtr("cluster-a"),
					KeyName:     strPtr("key-001"),
				},
			},
			AiAuthHitQuotaPlans: []string{"hit-plan-a"},
		},
	}
}

func strPtr(s string) *string    { return &s }
func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }
func int64Ptr(v int64) *int64    { return &v }
func int32Ptr(v int32) *int32    { return &v }
func boolPtr(v bool) *bool       { return &v }

func TestFieldRegistry_DefaultFieldsCount(t *testing.T) {
	def := DefaultFields()
	if len(def) != 57 {
		t.Fatalf("expected 57 default fields, got %d: %v", len(def), def)
	}
}

func TestFieldRegistry_RequiredFieldsCount(t *testing.T) {
	req := RequiredFields()
	if len(req) != 22 {
		t.Fatalf("expected 22 required fields, got %d: %v", len(req), req)
	}
}

func TestFieldRegistry_AllFieldsCount(t *testing.T) {
	all := AllFields()
	if len(all) < 67 {
		t.Fatalf("expected at least 67 fields, got %d", len(all))
	}
}

func TestFieldRegistry_IsValidField(t *testing.T) {
	if !IsValidField("logid") {
		t.Error("logid should be valid")
	}
	if !IsValidField("hostid") {
		t.Error("hostid should be valid")
	}
	if !IsValidField("ai_apikey_id") {
		t.Error("ai_apikey_id should be valid")
	}
	if IsValidField("ai_apikey") {
		t.Error("ai_apikey should not be valid (renamed to ai_apikey_id)")
	}
	if !IsValidField("referrer") {
		t.Error("referrer should be valid")
	}
	if IsValidField("nonexistent") {
		t.Error("nonexistent should not be valid")
	}
}

func TestFieldRegistry_Extract(t *testing.T) {
	log := makeBfeLog()

	tests := []struct {
		name     string
		expected interface{}
	}{
		{"logid", uint64(12345)},
		{"timestamp", uint64(1782353290)},
		{"hostid", "TODO"},
		{"product", "BFE"},
		{"log_tag", "req_BFE"},
		{"client_ip", "10.0.0.1"},
		{"err_code", ""},
		{"proto", "HTTP/1.1"},
		{"header_host", "example.com"},
		{"origin_uri", "/api/v1/test"},
		{"method", "POST"},
		{"content_type", "application/json"},
		{"cluster", "cluster_ai"},
		{"sub_cluster", "pool_bj"},
		{"backend_info", "10.0.0.2:8080"},
		{"res_status_code", uint32(200)},
		{"all_time", uint32(11)},
		{"read_client_time", uint32(2)},
		{"cluster_serve_time", uint32(5)},
		{"backend_serve_time", uint32(4)},
		{"ai_apikey_id", "key-id-123"},
		{"ai_requested_model", "test-model"},
		{"ai_target_model", "gpt-5"},
		{"ai_stream", false},
		{"ai_input_tokens", int64(34)},
		{"ai_output_tokens", int64(182)},
		{"ai_total_tokens", int64(216)},
		{"ai_ttft_us", int64(5486)},
		{"ai_tpot_us", int64(3)},
		{"ai_provider", "openai"},
		{"ai_retry_count", uint32(1)},
		{"ai_cost_value", int64(5000)},
		{"ai_cost_currency", "USD"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, isZero := Extract(tt.name, log)
			if tt.name == "hostid" {
				if isZero {
					t.Errorf("hostid should not be zero")
				}
				if _, ok := val.(string); !ok {
					t.Errorf("hostid should be string, got %T", val)
				}
				return
			}
			if val != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, val)
			}
			_ = isZero
		})
	}
}

func TestFieldRegistry_ExtractAIRouteRuleHits(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_route_rule_hits", log)
	if isZero {
		t.Fatal("ai_route_rule_hits should not be zero")
	}
	hits, ok := val.([]AIRouteRuleHitJSON)
	if !ok {
		t.Fatalf("expected []AIRouteRuleHitJSON, got %T", val)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	if hits[0].RuleOwner != "ak_user_a" || hits[0].RuleOwnerType != "apikey" || hits[0].RuleName != "user_a-rule1" {
		t.Errorf("unexpected hit: %+v", hits[0])
	}
}

func TestFieldRegistry_ExtractAIClusterKeyNames(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_cluster_key_names", log)
	if isZero {
		t.Fatal("ai_cluster_key_names should not be zero")
	}
	pairs, ok := val.([]ClusterKeyNameJSON)
	if !ok {
		t.Fatalf("expected []ClusterKeyNameJSON, got %T", val)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair, got %d", len(pairs))
	}
	if pairs[0].ClusterName != "cluster-a" || pairs[0].KeyName != "key-001" {
		t.Errorf("unexpected pair: %+v", pairs[0])
	}
}

func TestFieldRegistry_ExtractAIAuthHitQuotaPlans(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_auth_hit_quota_plans", log)
	if isZero {
		t.Fatal("ai_auth_hit_quota_plans should not be zero")
	}
	plans, ok := val.([]string)
	if !ok {
		t.Fatalf("expected []string, got %T", val)
	}
	if len(plans) != 1 || plans[0] != "hit-plan-a" {
		t.Errorf("unexpected plans: %v", plans)
	}
}

func TestFieldRegistry_ExtractAiApikeytags(t *testing.T) {
	log := makeBfeLog()
	val, isZero := Extract("ai_apikeytags", log)
	if isZero {
		t.Fatal("ai_apikeytags should not be zero")
	}
	tags, ok := val.(map[string]interface{})
	if !ok {
		t.Fatalf("expected map[string]interface{}, got %T", val)
	}
	if len(tags) != 5 {
		t.Fatalf("expected 5 levels, got %d", len(tags))
	}
	lvl1, ok := tags["level1"].(ApikeyTagJSON)
	if !ok || lvl1.Tagname != "dep" || lvl1.Tagvalue != "ops" {
		t.Errorf("unexpected level1: %+v", tags["level1"])
	}
	lvl2, ok := tags["level2"].(ApikeyTagJSON)
	if !ok || lvl2.Tagname != "team" || lvl2.Tagvalue != "bfe" {
		t.Errorf("unexpected level2: %+v", tags["level2"])
	}
	for _, lvl := range []string{"level3", "level4", "level5"} {
		m, ok := tags[lvl].(map[string]interface{})
		if !ok || len(m) != 0 {
			t.Errorf("expected %s to be empty object, got %+v", lvl, tags[lvl])
		}
	}
}

func TestFieldRegistry_ExtractZeroValues(t *testing.T) {
	log := makeBfeLog()

	val, isZero := Extract("err_code", log)
	if !isZero {
		t.Error("err_code should be zero")
	}
	_ = val

	val, isZero = Extract("req_body_len", log)
	if !isZero {
		t.Error("req_body_len should be zero")
	}
	_ = val

	val, isZero = Extract("ai_stream", log)
	if !isZero {
		t.Error("ai_stream should be zero (false)")
	}
	_ = val
}

func TestFieldRegistry_ExtractNilRequestLog(t *testing.T) {
	logid := uint64(1)
	ts := uint64(100)
	bfeLog := &bfe_access_pb.BfeLog{
		Logid:     &logid,
		Timestamp: &ts,
	}

	val, isZero := Extract("logid", bfeLog)
	if isZero || val.(uint64) != 1 {
		t.Errorf("expected logid=1, got %v", val)
	}

	_, isZero = Extract("err_code", bfeLog)
	if !isZero {
		t.Error("err_code should be zero when RequestLog is nil")
	}
}

func TestFieldRegistry_IpConversion(t *testing.T) {
	log := makeBfeLog()
	log.RequestLog.ClientIp = uint32Ptr(0xC0A80101) // 192.168.1.1

	val, _ := Extract("client_ip", log)
	if val != "192.168.1.1" {
		t.Errorf("expected 192.168.1.1, got %v", val)
	}
}

func TestFieldRegistry_AddrInfoFlattened(t *testing.T) {
	log := makeBfeLog()
	log.RequestLog.AddrInfo = &bfe_access_pb.ConnAddrInfo{
		BfeIp:        uint32Ptr(0x0A000064), // 10.0.0.100
		SockSrcIp:    uint32Ptr(0xC0A80164), // 192.168.1.100
		IsTrustSrcIp: boolPtr(true),
		Vip:          uint32Ptr(0x0A0000C8), // 10.0.0.200
	}

	if val, _ := Extract("bfe_ip", log); val != "10.0.0.100" {
		t.Errorf("bfe_ip: expected 10.0.0.100, got %v", val)
	}
	if val, _ := Extract("sock_src_ip", log); val != "192.168.1.100" {
		t.Errorf("sock_src_ip: expected 192.168.1.100, got %v", val)
	}
	if val, _ := Extract("is_trust_src_ip", log); val != true {
		t.Errorf("is_trust_src_ip: expected true, got %v", val)
	}
	if val, _ := Extract("vip", log); val != "10.0.0.200" {
		t.Errorf("vip: expected 10.0.0.200, got %v", val)
	}
}
