# Log Reader Output Fields Reference

Document History:
- V1.0, Yunxi Ye, 2026/07/08

---

## 1. Overview

This document lists all registered JSON output fields in the `mod_kafka` module of `log_reader`, including for each field:

- **JSON Field Name**: the JSON key in the Kafka output message
- **Type**: Go type and corresponding JSON type
- **Required**: whether the field is always output (regardless of FieldMode)
- **Default**: whether the field is in the default field set (output when `FieldMode = default`)
- **Description**: field meaning and data source

---

## 2. Field Mode Description

Output fields are controlled via `FieldMode` in `kafka_config.data`:

| Mode | Description |
|------|-------------|
| `require` | Output only Required fields |
| `default` | Output the Default field set |
| `all` | Output all registered fields |
| `customized` | Output the union of Required fields + fields specified in `FieldNames` |

> **Note**: Regardless of the selected mode, Required fields are always output.

---

## 3. Complete Field List

### 3.1. BfeLog Top-Level Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `logid` | uint64 | ✅ | ✅ | Unique request identifier, assigned by BFE for each request |
| `timestamp` | uint64 | ✅ | ✅ | Request timestamp (Unix seconds) |
| `product` | string | ✅ | ✅ | Product identifier, prefers `RequestLog.Product`, falls back to `BfeLog.Product` when empty |
| `log_tag` | string | ❌ | ❌ | Log classification tag, used to distinguish normal/error logs |
| `hostid` | string | ✅ | ✅ | log reader hostid (hostname_netns) |

### 3.2. Client Connection Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `client_ip` | string | ✅ | ✅ | Client IP address (IPv4 dotted decimal or IPv6 string) |
| `client_ip6` | string | ✅ | ✅ | Client IPv6 address |
| `client_network` | string | ❌ | ❌ | Client network type (IPv4 / IPv6) |
| `req_num` | uint32 | ❌ | ❌ | Request sequence number on the connection |
| `session_id` | uint64 | ❌ | ❌ | Session ID |

### 3.3. Request Basic Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `err_code` | string | ✅ | ✅ | Error code, empty for normal requests |
| `err_msg` | string | ✅ | ✅ | Error details |
| `req_header_len` | uint32 | ✅ | ✅ | Request header length (bytes) |
| `req_body_len` | uint32 | ✅ | ✅ | Request body length (bytes) |

### 3.4. Request Header Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `proto` | string | ✅ | ✅ | HTTP protocol version (e.g. `HTTP/1.1`, `HTTP/2`) |
| `header_host` | string | ✅ | ✅ | Request target Host |
| `origin_uri` | string | ✅ | ✅ | Original request URI |
| `final_uri` | string | ❌ | ✅ | Final URI after routing (post-rewrite) |
| `method` | string | ✅ | ✅ | HTTP method (GET / POST / PUT / DELETE, etc.) |
| `content_type` | string | ❌ | ✅ | Request Content-Type |
| `referrer` | string | ❌ | ❌ | Request Referer |
| `user_agent` | string | ❌ | ❌ | Request User-Agent |
| `x_forward_for` | string | ❌ | ✅ | X-Forwarded-For header |
| `accept_language` | string | ❌ | ✅ | Accept-Language header |
| `authorization` | string | ❌ | ✅ | Authorization header |
| `transfer_encoding` | string | ❌ | ✅ | Transfer-Encoding header |
| `delegation` | string | ❌ | ❌ | Delegation identifier |
| `uid` | string | ❌ | ❌ | User ID |

### 3.5. Cookie Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `cookie` | string | ❌ | ❌ | Raw request Cookie string |

### 3.6. Request Header List

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `req_headers` | []object | ❌ | ❌ | Request header list, each item as `{"key": "...", "value": "..."}` |

### 3.7. Routing Information Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `cluster` | string | ❌ | ✅ | Target cluster name |
| `sub_cluster` | string | ❌ | ✅ | Target sub-cluster name |
| `backend_info` | string | ❌ | ✅ | Backend instance info, format `"ip:port"` |
| `backend_retry` | uint32 | ❌ | ✅ | Backend retry count |

### 3.8. Response Information Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `res_status_code` | uint32 | ✅ | ✅ | Response HTTP status code |
| `res_header_len` | uint32 | ✅ | ✅ | Response header length (bytes) |
| `res_body_len` | uint32 | ✅ | ✅ | Response body length (bytes) |
| `res_content_type` | string | ❌ | ✅ | Response Content-Type |
| `res_location` | string | ❌ | ❌ | Response Location header (redirect URL) |
| `res_transfer_encoding` | string | ❌ | ❌ | Response Transfer-Encoding header |

### 3.9. Response Header List

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `res_headers` | []object | ❌ | ❌ | Response header list, each item as `{"key": "...", "value": "..."}` |

### 3.10. Timing Information Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `all_time` | uint32 | ✅ | ✅ | Total request duration (milliseconds) |
| `read_client_time` | uint32 | ✅ | ✅ | Time to read client request (milliseconds) |
| `cluster_serve_time` | uint32 | ✅ | ✅ | Cluster-layer processing time (milliseconds) |
| `backend_serve_time` | uint32 | ✅ | ✅ | Backend service processing time (milliseconds) |
| `write_client_time` | uint32 | ✅ | ✅ | Time to write response to client (milliseconds) |
| `session_offset_time` | uint32 | ❌ | ❌ | Session offset time (milliseconds) |
| `connect_backend_time` | uint32 | ❌ | ✅ | Time to connect to backend (milliseconds) |
| `proxy_delay_time` | uint32 | ✅ | ✅ | Proxy delay time (milliseconds) |

### 3.11. AI Observability Fields

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `ai_apikey_id` | string | ❌ | ✅ | API Key internal identifier |
| `ai_apikeytags` | []object | ❌ | ✅ | API Key tag list, each item as `{"tagname": "...", "tagvalue": "..."}` |
| `ai_requested_model` | string | ❌ | ✅ | Model name requested by the client |
| `ai_target_model` | string | ❌ | ✅ | Actual target model name after routing |
| `ai_stream` | bool | ❌ | ✅ | Whether the request is a streaming request |
| `ai_input_tokens` | int64 | ❌ | ✅ | Number of input tokens consumed |
| `ai_output_tokens` | int64 | ❌ | ✅ | Number of output tokens consumed (may be -1 when rate limited) |
| `ai_total_tokens` | int64 | ❌ | ✅ | Total number of tokens consumed |
| `ai_ttft_us` | int64 | ❌ | ✅ | Time To First Token latency (microseconds) |
| `ai_tpot_us` | int64 | ❌ | ✅ | Time Per Output Token latency (microseconds) |
| `ai_rate_limit_hits` | []object | ❌ | ✅ | Rate limit hit records, each item as `{"rate_limit_policy_id": "...", "rate_limit_type": "...", "rule_names": [...]}` |
| `ai_auth_reject_reason` | string | ❌ | ✅ | Authentication/authorization rejection reason |
| `ai_auth_reject_quota_plans` | []string | ❌ | ✅ | List of rejected quota plan names |
| `ai_provider` | string | ❌ | ✅ | Upstream model provider |
| `ai_retry_count` | uint32 | ❌ | ✅ | Model invocation retry count |
| `ai_cost_value` | int64 | ❌ | ✅ | Fixed-point cost value |
| `ai_cost_currency` | string | ❌ | ✅ | Cost currency |
| `ai_route_rule_hits` | []object | ❌ | ✅ | AI routing rule hit records |
| `ai_cluster_key_names` | []object | ❌ | ✅ | Tried cluster and key name pairs |
| `ai_auth_hit_quota_plans` | []string | ❌ | ✅ | List of hit quota plan names for successful requests |

### 3.12. Address Information Fields (Flattened from ConnAddrInfo)

| JSON Field | Type | Required | Default | Description |
|------------|------|----------|---------|-------------|
| `bfe_ip` | string | ❌ | ❌ | BFE instance IP address |
| `sock_src_ip` | string | ❌ | ❌ | Socket source IP address |
| `is_trust_src_ip` | bool | ❌ | ✅ | Whether the source IP is trusted |
| `vip` | string | ❌ | ❌ | VIP address (IPv4) |
| `vip6` | string | ❌ | ❌ | VIP address (IPv6) |

---

## 4. Statistics Summary

| Category | Field Count | Required Count | Default Count |
|----------|-------------|----------------|---------------|
| BfeLog Top-Level | 4 | 3 | 3 |
| Client Connection | 5 | 2 | 2 |
| Request Basic | 4 | 4 | 4 |
| Request Headers | 14 | 5 | 9 |
| Cookie | 1 | 0 | 0 |
| Request Header List | 1 | 0 | 0 |
| Routing Information | 4 | 0 | 4 |
| Response Information | 6 | 3 | 4 |
| Response Header List | 1 | 0 | 0 |
| Timing Information | 8 | 5 | 6 |
| AI Observability | 20 | 0 | 20 |
| Address Information | 5 | 0 | 1 |
| **Total** | **73** | **22** | **53** |

---

## 5. Composite Object Structure Reference

### 5.1. ai_apikeytags ([]object)

```json
[
    {"tagname": "dep", "tagvalue": "op"},
    {"tagname": "team", "tagvalue": "bfe"}
]
```

| Sub-field | Type | Description |
|-----------|------|-------------|
| `tagname` | string | Tag name |
| `tagvalue` | string | Tag value |

### 5.2. ai_rate_limit_hits ([]object)

```json
[
    {
        "rate_limit_policy_id": "rlp-0002",
        "rate_limit_type": "tpm",
        "rule_names": ["tpm1"]
    }
]
```

| Sub-field | Type | Description |
|-----------|------|-------------|
| `rate_limit_policy_id` | string | Rate limit policy ID |
| `rate_limit_type` | string | Rate limit type (e.g. `tpm`, `rpm`) |
| `rule_names` | []string | List of matched rule names |

### 5.3. ai_route_rule_hits ([]object)

```json
[
    {
        "rule_owner": "ak_user_a",
        "rule_owner_type": "apikey",
        "rule_name": "user_a-rule1"
    }
]
```

| Sub-field | Type | Description |
|-----------|------|-------------|
| `rule_owner` | string | Route table owner |
| `rule_owner_type` | string | Route table type (e.g. `apikey`, `entity`, `global`) |
| `rule_name` | string | Rule name |

### 5.4. ai_cluster_key_names ([]object)

```json
[
    {"cluster_name": "cluster-a", "key_name": "key-001"}
]
```

| Sub-field | Type | Description |
|-----------|------|-------------|
| `cluster_name` | string | Cluster name |
| `key_name` | string | API Key name/identifier |

### 5.5. req_headers / res_headers ([]object)

```json
[
    {"key": "Content-Type", "value": "application/json"},
    {"key": "Authorization", "value": "Bearer sk-xxx"}
]
```

| Sub-field | Type | Description |
|-----------|------|-------------|
| `key` | string | Header name |
| `value` | string | Header value |

---

## 6. Configuration Examples

### 6.1. Default Mode (Output Default Fields)

```ini
[ConfFields]
FieldMode = default
```

### 6.2. Customized Mode (Output 22 Required + Specified Fields)

```ini
[ConfFields]
FieldMode = customized
FieldNames = ai_apikey_id
FieldNames = ai_apikeytags
FieldNames = ai_requested_model
FieldNames = ai_target_model
FieldNames = ai_stream
FieldNames = ai_input_tokens
FieldNames = ai_output_tokens
FieldNames = ai_total_tokens
FieldNames = ai_ttft_us
FieldNames = ai_tpot_us
FieldNames = ai_rate_limit_hits
FieldNames = ai_auth_reject_reason
FieldNames = referrer
FieldNames = user_agent
```

### 6.3. Required Fields Only

```ini
[ConfFields]
FieldMode = require
```

### 6.4. All Fields

```ini
[ConfFields]
FieldMode = all
```