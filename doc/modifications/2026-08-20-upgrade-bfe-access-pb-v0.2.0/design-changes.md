# log-reader 适配 bfe-access-pb v0.2.0 设计变更

## 1. 背景

`bfe-access-pb` 访问日志协议已从 `v0.1.0` 升级到 `v0.2.0`，对 AI 可观测字段进行了扩展与重命名（详见 `bfe-access-pb/docs/protobuf.md`）。主要变化包括：

1. **字段重命名**（protobuf 编号保持不变）：
   - `ai_apikey` → `ai_apikey_id`（记录 API Key 内部标识，不再记录原始 key 值）
   - `ai_mapped_model` → `ai_target_model`
   - `ai_prompt_tokens` → `ai_input_tokens`
2. **新增字段**：
   - `ai_provider`：上游模型提供商标识
   - `ai_retry_count`：模型调用层重试次数
   - `ai_cost_value` / `ai_cost_currency`：RMB/USD 成本计量
   - `ai_route_rule_hits`：命中的 AI 路由规则列表
   - `ai_cluster_key_names`：请求处理过程中尝试过的 (cluster, key) 列表
   - `ai_auth_hit_quota_plans`：成功请求时命中的 Quota Plan ID 列表
3. **新增辅助消息**：
   - `AIRouteRuleHit`：描述命中路由规则的 owner / owner_type / name
   - `ClusterKeyName`：描述尝试过的 cluster 与 key 名称组合

`log-reader` 当前依赖 `github.com/bfenetworks/bfe-access-pb v0.1.0`，字段注册表（`field_registry.go`）和集成测试生成器（`log_generator.go`）中仍引用旧字段名（`AiApikey`、`AiMappedModel`、`AiPromptTokens`）。为使 `log-reader` 与新版协议对齐，需要在本模块进行配套改造。

与 BFE 不同，`log-reader` 是日志消费端，不负责生产字段值，只负责：

- 正确解析包含新字段的 protobuf 访问日志；
- 按 `kafka_config.data` 配置将字段（含重命名和新增字段）输出为 JSON；
- 保持原有字段模式（`require` / `default` / `all` / `customized`）行为不变。

---

## 2. 目标

1. 将 `log-reader` 依赖的 `bfe-access-pb` 升级到 `v0.2.0`。
2. 修正 `mod_kafka` 字段注册表中对重名字段的引用。
3. 在字段注册表中注册新增字段，并定义对应的 JSON 输出结构。
4. 更新生产配置 `conf/mod_kafka/kafka_config.data` 与配置文档，使用新字段名并补充新增字段。
5. 更新字段参考文档（`doc/fields/logreader-output-fields-reference.md` 及中文版本）。
6. 更新单元测试与集成测试，覆盖重命名与新增字段。
7. 保持非 AI 请求和未升级配置场景下的向后兼容。

---

## 3. 变更总览

| Proto 字段 | 编号 | 旧 `log-reader` 字段名 | 新 `log-reader` 字段名 | JSON 类型 | 默认输出 | 需修改的文件 |
|------------|------|------------------------|------------------------|-----------|----------|--------------|
| `ai_apikey_id` | 701 | `ai_apikey` | `ai_apikey_id` | string | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |
| `ai_apikeytags` | 702 | `ai_apikeytags` | `ai_apikeytags` | []object | Y | 不变 |
| `ai_requested_model` | 703 | `ai_requested_model` | `ai_requested_model` | string | Y | 不变 |
| `ai_target_model` | 704 | `ai_mapped_model` | `ai_target_model` | string | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |
| `ai_stream` | 705 | `ai_stream` | `ai_stream` | bool | Y | 不变 |
| `ai_input_tokens` | 706 | `ai_prompt_tokens` | `ai_input_tokens` | int64 | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |
| `ai_output_tokens` | 707 | `ai_output_tokens` | `ai_output_tokens` | int64 | Y | 不变 |
| `ai_total_tokens` | 708 | `ai_total_tokens` | `ai_total_tokens` | int64 | Y | 不变 |
| `ai_ttft_us` | 709 | `ai_ttft_us` | `ai_ttft_us` | int64 | Y | 不变 |
| `ai_tpot_us` | 710 | `ai_tpot_us` | `ai_tpot_us` | int64 | Y | 不变 |
| `ai_rate_limit_hits` | 711 | `ai_rate_limit_hits` | `ai_rate_limit_hits` | []object | Y | 不变 |
| `ai_auth_reject_reason` | 712 | `ai_auth_reject_reason` | `ai_auth_reject_reason` | string | Y | 不变 |
| `ai_auth_reject_quota_plans` | 713 | `ai_auth_reject_quota_plans` | `ai_auth_reject_quota_plans` | []string | Y | 不变 |
| `ai_provider` | 714 | — | `ai_provider` | string | Y | `field_registry.go`, `json_converter.go`, `kafka_config.data`, 文档, 测试 |
| `ai_retry_count` | 715 | — | `ai_retry_count` | uint32 | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |
| `ai_cost_value` | 761 | — | `ai_cost_value` | int64 | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |
| `ai_cost_currency` | 762 | — | `ai_cost_currency` | string | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |
| `ai_route_rule_hits` | 801 | — | `ai_route_rule_hits` | []object | Y | `field_registry.go`, `json_converter.go`, `kafka_config.data`, 文档, 测试 |
| `ai_cluster_key_names` | 802 | — | `ai_cluster_key_names` | []object | Y | `field_registry.go`, `json_converter.go`, `kafka_config.data`, 文档, 测试 |
| `ai_auth_hit_quota_plans` | 841 | — | `ai_auth_hit_quota_plans` | []string | Y | `field_registry.go`, `kafka_config.data`, 文档, 测试 |

> 说明：上表中的"默认输出"指 `field_registry.go` 中 `Default` 标记。新增字段建议默认输出，以便在 `default` 字段模式下即可使用。

---

## 4. 详细设计

### 4.1 升级 `bfe-access-pb` 依赖

**文件：** `log-reader/go.mod`

将依赖版本从 `v0.1.0` 升级到 `v0.2.0`：

```go
require (
    ...
    github.com/bfenetworks/bfe-access-pb v0.2.0
    ...
)
```

本地开发时可临时启用 replace：

```go
replace github.com/bfenetworks/bfe-access-pb => ../bfe-access-pb
```

升级后执行：

```bash
cd log-reader
go mod tidy
go mod download
```

> 注意：由于 protobuf 字段编号不变，旧 BFE 生成的二进制日志在新版 `.pb.go` 中仍可解析；但 Go 字段名变化会导致 `log-reader` 编译失败，因此需要同步更新代码引用。

---

### 4.2 更新 JSON 输出结构定义

**文件：** `log-reader/reader_modules/mod_kafka/json_converter.go`

新增两个 JSON 结构，用于 `ai_route_rule_hits` 和 `ai_cluster_key_names` 字段输出：

```go
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
```

---

### 4.3 更新字段注册表

**文件：** `log-reader/reader_modules/mod_kafka/field_registry.go`

#### 4.3.1 重名字段

将以下注册项更新为新字段名，并调用新版 `RequestLog` 的 getter：

| 旧注册名 | 新注册名 | 新 getter |
|----------|----------|-----------|
| `ai_apikey` | `ai_apikey_id` | `reqLog.GetAiApikeyId()` |
| `ai_mapped_model` | `ai_target_model` | `reqLog.GetAiTargetModel()` |
| `ai_prompt_tokens` | `ai_input_tokens` | `reqLog.GetAiInputTokens()` |

示例：

```go
registerField("ai_apikey_id", "string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiApikeyId()
        }
        return ""
    },
    isZeroString,
)
```

#### 4.3.2 新增字段

在 AI 可观测字段区域（`ai_auth_reject_quota_plans` 之后）补充以下注册项：

```go
// ai_provider
registerField("ai_provider", "string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiProvider()
        }
        return ""
    },
    isZeroString,
)

// ai_retry_count
registerField("ai_retry_count", "uint32", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiRetryCount()
        }
        return uint32(0)
    },
    isZeroUint32,
)

// ai_cost_value
registerField("ai_cost_value", "int64", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiCostValue()
        }
        return int64(0)
    },
    isZeroInt64,
)

// ai_cost_currency
registerField("ai_cost_currency", "string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiCostCurrency()
        }
        return ""
    },
    isZeroString,
)

// ai_route_rule_hits
registerField("ai_route_rule_hits", "[]object", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            hits := reqLog.GetAiRouteRuleHits()
            result := make([]AIRouteRuleHitJSON, 0, len(hits))
            for _, h := range hits {
                result = append(result, AIRouteRuleHitJSON{
                    RuleOwner:     h.GetRuleOwner(),
                    RuleOwnerType: h.GetRuleOwnerType(),
                    RuleName:      h.GetRuleName(),
                })
            }
            return result
        }
        return []AIRouteRuleHitJSON{}
    },
    isZeroSlice,
)

// ai_cluster_key_names
registerField("ai_cluster_key_names", "[]object", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            pairs := reqLog.GetAiClusterKeyNames()
            result := make([]ClusterKeyNameJSON, 0, len(pairs))
            for _, p := range pairs {
                result = append(result, ClusterKeyNameJSON{
                    ClusterName: p.GetClusterName(),
                    KeyName:     p.GetKeyName(),
                })
            }
            return result
        }
        return []ClusterKeyNameJSON{}
    },
    isZeroSlice,
)

// ai_auth_hit_quota_plans
registerField("ai_auth_hit_quota_plans", "[]string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiAuthHitQuotaPlans()
        }
        return []string{}
    },
    isZeroSlice,
)
```

> 说明：`isZeroSlice` 已能处理 `[]ApikeyTagJSON`、`[]AiRateLimitHitJSON`、`[]HttpHeaderJSON`、`[]string` 等类型，需要扩展以支持 `[]AIRouteRuleHitJSON` 和 `[]ClusterKeyNameJSON`。

---

### 4.4 更新生产配置 `conf/mod_kafka/kafka_config.data`

**文件：** `log-reader/conf/mod_kafka/kafka_config.data`

当前生产配置已开启以下 AI 字段（v0.1.0 名称）：

```ini
FieldNames= ai_apikey
FieldNames= ai_apikeytags
FieldNames= ai_requested_model
FieldNames= ai_mapped_model
FieldNames= ai_stream
FieldNames= ai_prompt_tokens
FieldNames= ai_output_tokens
FieldNames= ai_total_tokens
FieldNames= ai_ttft_us
FieldNames= ai_tpot_us
FieldNames= ai_rate_limit_hits
FieldNames= ai_auth_reject_reason
FieldNames= ai_auth_reject_quota_plans
```

升级到 `bfe-access-pb v0.2.0` 后，需要：

1. **更新已更名字段**：
   - `ai_apikey` → `ai_apikey_id`
   - `ai_mapped_model` → `ai_target_model`
   - `ai_prompt_tokens` → `ai_input_tokens`

2. **补充新增加的全部 AI 字段**：
   - `ai_provider`
   - `ai_retry_count`
   - `ai_cost_value`
   - `ai_cost_currency`
   - `ai_route_rule_hits`
   - `ai_cluster_key_names`
   - `ai_auth_hit_quota_plans`

更新后的 AI 字段段落应为：

```ini
FieldNames= ai_apikey_id
FieldNames= ai_apikeytags
FieldNames= ai_requested_model
FieldNames= ai_target_model
FieldNames= ai_stream
FieldNames= ai_input_tokens
FieldNames= ai_output_tokens
FieldNames= ai_total_tokens
FieldNames= ai_ttft_us
FieldNames= ai_tpot_us
FieldNames= ai_rate_limit_hits
FieldNames= ai_auth_reject_reason
FieldNames= ai_auth_reject_quota_plans
FieldNames= ai_provider
FieldNames= ai_retry_count
FieldNames= ai_cost_value
FieldNames= ai_cost_currency
FieldNames= ai_route_rule_hits
FieldNames= ai_cluster_key_names
FieldNames= ai_auth_hit_quota_plans
```

> 说明：字段在 `kafka_config.data` 中的顺序不影响输出；建议按字段编号（701-715、761-762、801-802、841）分组排列，便于维护。

---

### 4.5 更新配置文档与字段参考文档

**文件：**

- `log-reader/doc/configuration/mod_kafka/kafka_config.data.md`
- `log-reader/doc/fields/logreader-output-fields-reference.md`
- `log-reader/doc/fields/logreader-output-fields-reference.cn.md`

在 `doc/configuration/mod_kafka/kafka_config.data.md` 中：

1. 将字段表格中的 `ai_apikey` 替换为 `ai_apikey_id`，描述更新为"API Key 内部标识"；
2. 将 `ai_mapped_model` 替换为 `ai_target_model`，描述更新为"实际路由目标模型名"；
3. 将 `ai_prompt_tokens` 替换为 `ai_input_tokens`；
4. 新增 7 个 AI 字段的说明、类型和示例。

在字段参考文档中同步更新：

- 字段列表与类型；
- Required / Default 标记；
- 示例 JSON 输出。

---

### 4.6 更新单元测试

**文件：**

- `log-reader/reader_modules/mod_kafka/field_registry_test.go`
- `log-reader/reader_modules/mod_kafka/json_converter_test.go`
- `log-reader/reader_modules/mod_kafka/kafka_data_config_test.go`

#### 4.6.1 `field_registry_test.go`

- 将 `IsValidField("ai_apikey")` 断言改为 `ai_apikey_id`。
- 将 `ai_apikey`、`ai_mapped_model`、`ai_prompt_tokens` 提取测试用例替换为新字段名。
- 新增针对 `ai_provider`、`ai_retry_count`、`ai_cost_value`、`ai_cost_currency`、`ai_route_rule_hits`、`ai_cluster_key_names`、`ai_auth_hit_quota_plans` 的提取测试用例。

#### 4.6.2 `json_converter_test.go`

- 将 `require` 模式断言中的旧字段名替换为新字段名。
- 将 `customized` 模式测试中的字段名替换为新字段名。
- 新增对 `ai_route_rule_hits`、`ai_cluster_key_names`、`ai_auth_hit_quota_plans` 的 JSON 输出测试。

#### 4.6.3 `kafka_data_config_test.go`

- 将测试配置中的 `ai_apikey`、`ai_mapped_model` 等旧字段名替换为新字段名。
- 新增对新增字段的解析测试。

---

### 4.7 更新集成测试

**文件：**

- `log-reader/tests/integration/common/log_generator.go`
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data`
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go`
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md`
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md`
- `log-reader/tests/integration/README.md`

#### 4.7.1 `log_generator.go`

- 将 `AiApikey` 替换为 `AiApikeyId`。
- 将 `AiMappedModel` 替换为 `AiTargetModel`。
- 将 `AiPromptTokens` 替换为 `AiInputTokens`。
- 新增字段填充：
  - `AiProvider`
  - `AiRetryCount`
  - `AiCostValue`
  - `AiCostCurrency`
  - `AiRouteRuleHits`
  - `AiClusterKeyNames`
  - `AiAuthHitQuotaPlans`

#### 4.7.2 集成测试配置

将 `testdata/mod_kafka/kafka_config.data` 中的旧字段名替换为新字段名，并补充新增字段。

#### 4.7.3 `lr01_basic_flow_test.go`

- 更新 `ai_apikey` → `ai_apikey_id` 的断言。
- 更新 `ai_mapped_model` → `ai_target_model` 的断言。
- 更新 `ai_prompt_tokens` → `ai_input_tokens` 的断言。
- 新增对新增字段的断言，包括对象数组字段的专用断言。

#### 4.7.4 集成测试文档

同步更新 TC-01、场景说明和 README 中的字段列表。

---

## 5. 涉及文件清单

| 文件 | 修改内容 |
|------|----------|
| `log-reader/go.mod` | 升级 `bfe-access-pb` 到 `v0.2.0`（或启用 replace） |
| `log-reader/go.sum` | 随 `go mod tidy` 自动更新 |
| `log-reader/reader_modules/mod_kafka/json_converter.go` | 新增 `AIRouteRuleHitJSON`、`ClusterKeyNameJSON` |
| `log-reader/reader_modules/mod_kafka/field_registry.go` | 重命名字段注册；新增 7 个字段注册；扩展 `isZeroSlice` |
| `log-reader/conf/mod_kafka/kafka_config.data` | 使用新字段名，补充新增字段 |
| `log-reader/doc/configuration/mod_kafka/kafka_config.data.md` | 同步更新字段说明与示例 |
| `log-reader/doc/fields/logreader-output-fields-reference.md` | 同步更新字段参考（英文） |
| `log-reader/doc/fields/logreader-output-fields-reference.cn.md` | 同步更新字段参考（中文） |
| `log-reader/reader_modules/mod_kafka/field_registry_test.go` | 更新字段名，新增新字段测试用例 |
| `log-reader/reader_modules/mod_kafka/json_converter_test.go` | 更新字段名，新增新字段 JSON 测试 |
| `log-reader/reader_modules/mod_kafka/kafka_data_config_test.go` | 更新字段名，新增新字段配置解析测试 |
| `log-reader/tests/integration/common/log_generator.go` | 使用新 proto 字段名，填充新增字段 |
| `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data` | 使用新字段名，补充新增字段 |
| `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go` | 更新断言，覆盖新增字段 |
| `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md` | 更新字段列表 |
| `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md` | 更新字段列表 |
| `log-reader/tests/integration/README.md` | 更新字段覆盖描述 |

---

## 6. 测试计划

### 6.1 单元测试

```bash
cd log-reader
go test ./reader_modules/mod_kafka/...
```

验证点：

1. `field_registry_test.go` 中所有重命名字段和新增字段的提取结果正确。
2. `json_converter_test.go` 中 `require` / `customized` / `default` / `all` 模式输出正确的字段集合。
3. `kafka_data_config_test.go` 中能正确解析包含新字段的配置。

### 6.2 集成测试

```bash
cd log-reader
go test ./tests/integration/implementation/scenario-LR01-basic-flow/... -v
```

验证点：

1. `MockKafka` 收到包含 `ai_apikey_id`、`ai_target_model`、`ai_input_tokens` 的 JSON 消息。
2. 新增字段 `ai_provider`、`ai_retry_count`、`ai_cost_value`、`ai_cost_currency`、`ai_route_rule_hits`、`ai_cluster_key_names`、`ai_auth_hit_quota_plans` 的值与 `MakeRequestLog` 输入一致。
3. 旧字段名（`ai_apikey`、`ai_mapped_model`、`ai_prompt_tokens`）不再出现在输出中。
4. 未配置的字段不输出。

### 6.3 全量测试

```bash
cd log-reader
go test ./...
```

---

## 7. 兼容性说明

1. **protobuf 二进制兼容**：字段编号不变，旧 BFE（v0.1.0）生成的日志在新版 `log-reader` 中仍可解析，新增字段将为空/零值。
2. **JSON 输出字段名变化**：下游消费方若依赖旧字段名 `ai_apikey`、`ai_mapped_model`、`ai_prompt_tokens`，需要同步更新为 `ai_apikey_id`、`ai_target_model`、`ai_input_tokens`。
3. **配置兼容**：用户现有的 `kafka_config.data` 若包含旧字段名，启动时会因 `IsValidField` 校验失败而忽略这些字段（并打印 warn 日志）。需要在升级后同步更新配置。
4. **新增字段可选**：新增字段均为 `optional`，未配置时不影响原有输出。

---

## 8. 风险与回滚

### 8.1 主要风险

| 风险 | 说明 | 规避措施 |
|------|------|----------|
| 编译失败 | `bfe-access-pb` v0.2.0 删除了旧 Go 字段名 | 按本方案一次性更新所有引用 |
| 下游消费方依赖旧字段名 | 下游解析 `ai_apikey`、`ai_mapped_model`、`ai_prompt_tokens` 会失败 | 提前通知下游；建议下游按 proto 编号做映射或随 `log-reader` 同步升级 |
| 配置遗漏 | 生产环境 `kafka_config.data` 未同步更新，导致 AI 字段缺失 | 将 `conf/mod_kafka/kafka_config.data` 与配置文档同步更新，并纳入发布检查清单 |
| 新字段零值被过滤 | 若 BFE 未升级，新增字段为空，`isZero*` 会过滤，不输出 | 符合预期；BFE 升级后才会输出 |

### 8.2 回滚方案

如需回滚到旧协议：

1. 将 `log-reader/go.mod` 中的 `bfe-access-pb` 版本改回 `v0.1.0`。
2. 回滚 `field_registry.go`、`json_converter.go` 到旧字段名和旧结构。
3. 回滚 `conf/mod_kafka/kafka_config.data`、配置文档、字段参考文档。
4. 回滚测试代码。
5. 重新编译部署。

> 注意：回滚后新版 BFE 生成的日志仍可解析（编号兼容），但新增字段不会输出到 JSON。

---

## 9. 后续可选扩展

1. **字段别名支持**：在 `kafka_data_config.go` 中增加旧字段名到新字段名的别名映射，实现配置层面的平滑升级。
2. **字段弃用提示**：当配置中出现旧字段名时，除了 warn 日志外，提示用户应使用的新字段名。
3. **动态字段文档生成**：基于 `field_registry.go` 自动生成 `doc/fields/logreader-output-fields-reference.md`，避免手工维护遗漏。

---

*文档生成日期：2026-08-20*
