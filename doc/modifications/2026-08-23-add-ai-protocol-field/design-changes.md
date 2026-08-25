# log-reader 补齐 bfe-access-pb AI 可观测字段

## 背景

`bfe-access-pb` v0.3.4 在 `RequestLog` 的 AI 可观测性区间（701-900）新增了多个字段，但 `log-reader` 的 `mod_kafka` 字段注册表此前未全部注册。本次变更一次性补齐所有已在 protobuf 中定义、但尚未在 `log-reader` 中输出的 AI 可观测字段，确保 Kafka JSON 输出与访问日志协议保持一致。

涉及的字段如下：

| 字段 | 编号 | 类型 | 说明 |
|------|------|------|------|
| `ai_protocol` | 717 | string | AI 协议风格，如 `openai`、`anthropic` |
| `ai_mode` | 716 | string | AI 请求模式，如 `chat`、`image_generation`、`embedding`、`audio_speech` 等 |
| `ai_audio_input_tokens` | 783 | int64 | 音频输入 Token 数，已包含在 `ai_input_tokens` 中 |
| `ai_audio_output_tokens` | 784 | int64 | 音频输出 Token 数，已包含在 `ai_output_tokens` 中 |
| `ai_image_count` | 785 | int64 | 图像生成模式下生成的图像张数 |

> 其中 `ai_protocol` 是 `bfe-access-pb` v0.3.4 相对 v0.3.1 新增的字段，其余 4 个字段在 v0.3.4 中已存在但 `log-reader` 此前未注册。

## 修改目标

1. 将 `log-reader` 依赖的 `bfe-access-pb` 从 `v0.3.1` 升级到 `v0.3.4`。
2. 在 `reader_modules/mod_kafka/field_registry.go` 中注册上述 5 个字段。
3. 在 `log-reader/doc/modules/mod_kafka/output-fields.md` 的 **AI 可观测字段** 章节中补充字段说明，并更新统计汇总。
4. 在 `log-reader/doc/configuration/mod_kafka/kafka_config.data.md` 的可用字段列表中补充上述字段。
5. 更新 `reader_modules/mod_kafka/field_registry_test.go` 中 Default 字段数断言。
6. 同步更新集成测试的数据准备、断言与相关设计文档。

## 详细改动

### 1. 依赖升级

**文件：** `log-reader/go.mod`

```go
require (
    ...
    github.com/bfenetworks/bfe-access-pb v0.3.4
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

### 2. 字段注册

**文件：** `log-reader/reader_modules/mod_kafka/field_registry.go`

在合适的 AI 可观测字段区域分别注册：

```go
// 714-760 区间：模型与请求基础信息
registerField("ai_protocol", "string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiProtocol()
        }
        return ""
    },
    isZeroString,
)
registerField("ai_mode", "string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiMode()
        }
        return ""
    },
    isZeroString,
)

// 761-800 区间：Token、成本、Cache、Audio 与 Image 计量
registerField("ai_audio_input_tokens", "int64", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiAudioInputTokens()
        }
        return int64(0)
    },
    isZeroInt64,
)
registerField("ai_audio_output_tokens", "int64", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiAudioOutputTokens()
        }
        return int64(0)
    },
    isZeroInt64,
)
registerField("ai_image_count", "int64", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiImageCount()
        }
        return int64(0)
    },
    isZeroInt64,
)
```

所有新增字段类型、Required/Default 标记如下：

| 字段 | 类型 | Required | Default |
|------|------|----------|---------|
| `ai_protocol` | string | ❌ | ✅ |
| `ai_mode` | string | ❌ | ✅ |
| `ai_audio_input_tokens` | int64 | ❌ | ✅ |
| `ai_audio_output_tokens` | int64 | ❌ | ✅ |
| `ai_image_count` | int64 | ❌ | ✅ |

### 3. 字段文档更新

**文件：** `log-reader/doc/modules/mod_kafka/output-fields.md`

在 `3.11. AI 可观测字段` 表格中补充：

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `ai_protocol` | string | ❌ | ✅ | AI 协议风格，如 `openai`、`anthropic` |
| `ai_mode` | string | ❌ | ✅ | AI 请求模式，如 `chat`、`image_generation`、`embedding`、`audio_speech` 等 |
| `ai_audio_input_tokens` | int64 | ❌ | ✅ | 音频输入 Token 数，已包含在 `ai_input_tokens` 中 |
| `ai_audio_output_tokens` | int64 | ❌ | ✅ | 音频输出 Token 数，已包含在 `ai_output_tokens` 中 |
| `ai_image_count` | int64 | ❌ | ✅ | 图像生成模式下生成的图像张数 |

并更新底部统计汇总：

| 类别 | 修改前 | 修改后 |
|------|--------|--------|
| AI 可观测字段数 | 22 | 27 |
| 总字段数 | 75 | 80 |
| Default 字段总数 | 55 | 60 |

**文件：** `log-reader/doc/configuration/mod_kafka/kafka_config.data.md`

在 **AI 可观测字段** 表格中同步补充上述字段。

### 4. 生产配置（可选）

**文件：** `log-reader/conf/mod_kafka/kafka_config.data`

如需在生产环境中输出新增字段，可在 `FieldNames` 列表中新增对应行：

```ini
FieldNames= ai_protocol
FieldNames= ai_mode
FieldNames= ai_audio_input_tokens
FieldNames= ai_audio_output_tokens
FieldNames= ai_image_count
```

当 `FieldMode = default` 或 `all` 时，由于这些字段已加入 Default 字段集，无需额外配置即可输出。

### 5. 单元测试更新

**文件：** `log-reader/reader_modules/mod_kafka/field_registry_test.go`

将 `TestFieldRegistry_DefaultFieldsCount` 的期望值从 57 更新为 62（新增 5 个 Default 字段）。

可补充针对新增字段的提取测试，例如：

```go
func TestFieldRegistry_ExtractAiMode(t *testing.T) {
    log := makeBfeLog()
    val, isZero := Extract("ai_mode", log)
    if isZero {
        t.Fatal("ai_mode should not be zero")
    }
    if got, want := val.(string), "chat"; got != want {
        t.Errorf("ai_mode = %q, want %q", got, want)
    }
}
```

### 6. 集成测试更新

**文件：**

- `log-reader/tests/integration/common/log_generator.go`
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go`
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data`
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md`
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md`
- `log-reader/tests/integration/README.md`

#### 6.1 `log_generator.go`

在 `MakeRequestLog` 中为 `RequestLog` 填充新增字段的测试数据：

```go
AiProtocol:          strPtr("openai"),
AiMode:              strPtr("chat"),
AiAudioInputTokens:  int64Ptr(100),
AiAudioOutputTokens: int64Ptr(50),
AiImageCount:        int64Ptr(1),
```

#### 6.2 `lr01_basic_flow_test.go`

在基础流程断言中增加对新增字段的校验：

```go
assertFieldEquals(t, payload, "ai_protocol", "openai")
assertFieldEquals(t, payload, "ai_mode", "chat")
assertFieldEquals(t, payload, "ai_audio_input_tokens", int64(100))
assertFieldEquals(t, payload, "ai_audio_output_tokens", int64(50))
assertFieldEquals(t, payload, "ai_image_count", int64(1))
```

#### 6.3 集成测试配置

将 `testdata/mod_kafka/kafka_config.data` 中的字段列表同步更新，确保 `default` 模式或 `customized` 模式覆盖新增字段。

#### 6.4 集成测试文档

同步更新 TC-01、场景说明和 README 中的字段覆盖描述与预期输出示例。

## 代码变更说明

- `log-reader/go.mod` / `go.sum`：`bfe-access-pb` 升级到 v0.3.4。
- `log-reader/reader_modules/mod_kafka/field_registry.go`：注册 `ai_protocol`、`ai_mode`、`ai_audio_input_tokens`、`ai_audio_output_tokens`、`ai_image_count`。
- `log-reader/reader_modules/mod_kafka/field_registry_test.go`：Default 字段数断言更新为 62，可补充新增字段提取测试。
- `log-reader/doc/modules/mod_kafka/output-fields.md`：文档新增字段说明与统计更新。
- `log-reader/doc/configuration/mod_kafka/kafka_config.data.md`：新增可用字段说明。
- `log-reader/conf/mod_kafka/kafka_config.data`：可选：增加新增字段输出。
- `log-reader/tests/integration/common/log_generator.go`：填充新增字段测试数据。
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go`：新增字段断言。
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data`：同步字段列表。
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md`：更新字段列表。
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md`：更新字段列表。
- `log-reader/tests/integration/README.md`：更新字段覆盖描述。
- `log-reader/doc/modifications/2026-08-23-add-ai-protocol-field/design-changes.md`：本设计变更文档。

## 验证步骤

1. 确认文档字段已注册：

```bash
cd log-reader
grep -n "ai_protocol\|ai_mode\|ai_audio_input_tokens\|ai_audio_output_tokens\|ai_image_count" doc/modules/mod_kafka/output-fields.md
grep -n "ai_protocol\|ai_mode\|ai_audio_input_tokens\|ai_audio_output_tokens\|ai_image_count" doc/configuration/mod_kafka/kafka_config.data.md
```

2. 确认字段注册代码：

```bash
cd log-reader
grep -n "ai_protocol\|ai_mode\|ai_audio_input_tokens\|ai_audio_output_tokens\|ai_image_count" reader_modules/mod_kafka/field_registry.go
```

3. 运行单元测试：

```bash
cd log-reader
go test ./reader_modules/mod_kafka/...
```

4. 运行集成测试：

```bash
cd log-reader
go test ./tests/integration/implementation/scenario-LR01-basic-flow/... -v
```

5. 全量编译检查：

```bash
cd log-reader
go build ./...
```

## 影响范围

| 文件 | 影响 |
|---|---|
| `log-reader/go.mod` / `go.sum` | `bfe-access-pb` 升级到 v0.3.4 |
| `log-reader/reader_modules/mod_kafka/field_registry.go` | 注册 5 个新增 AI 字段 |
| `log-reader/reader_modules/mod_kafka/field_registry_test.go` | Default 字段数断言更新为 62 |
| `log-reader/doc/modules/mod_kafka/output-fields.md` | 文档新增字段说明与统计更新 |
| `log-reader/doc/configuration/mod_kafka/kafka_config.data.md` | 可用字段列表补充新增字段 |
| `log-reader/conf/mod_kafka/kafka_config.data` | 可选：增加新增字段输出 |
| `log-reader/tests/integration/common/log_generator.go` | 填充新增字段测试数据 |
| `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go` | 新增字段断言 |
| `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data` | 同步字段列表 |
| `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md` | 更新字段列表 |
| `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md` | 更新字段列表 |
| `log-reader/tests/integration/README.md` | 更新字段覆盖描述 |
| `log-reader/doc/modifications/2026-08-23-add-ai-protocol-field/design-changes.md` | 本设计变更文档 |

## 依赖与兼容性

- 依赖 `bfe-access-pb` v0.3.4 或更高版本。
- 新增字段均为可选字段，未使用时不影响现有逻辑；下游消费方按需解析即可。
- 新增字段已纳入 Default 字段集，`FieldMode = default` 时自动输出。
- 集成测试中需确保 `bfe-access-pb` v0.3.4 的 `.pb.go` 包含 `GetAiMode()`、`GetAiProtocol()`、`GetAiAudioInputTokens()`、`GetAiAudioOutputTokens()`、`GetAiImageCount()` 等 getter。

---

*文档生成日期：2026-08-23*
