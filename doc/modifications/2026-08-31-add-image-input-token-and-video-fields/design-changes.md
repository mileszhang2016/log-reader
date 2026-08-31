# log-reader 输出字段增加图片输入 Token 与视频数量字段

## 背景

BFE 在 AI 网关场景下扩展了图片输入 token 计费与视频生成按次计费能力，访问日志 protobuf（`bfe-access-pb` v0.3.5）中新增了两个 AI 可观测字段：

- `ai_image_input_tokens`（字段编号 786）
- `ai_video_count`（字段编号 787）

`log-reader` 的 Kafka 输出字段注册表与文档需要同步更新，以便下游消费方了解新增字段的含义与类型。

## 修改目标

1. 将 `log-reader` 依赖的 `bfe-access-pb` 从 `v0.3.4` 升级到 `v0.3.5`。
2. 在 `log-reader/reader_modules/mod_kafka/field_registry.go` 中注册 `ai_image_input_tokens` 和 `ai_video_count`。
3. 在 `log-reader/doc/modules/mod_kafka/output-fields.md` 的 **AI 可观测字段** 章节中补充字段说明，并更新统计汇总。
4. 在 `log-reader/doc/configuration/mod_kafka/kafka_config.data.md` 的可用字段列表中补充上述字段。
5. 在 `log-reader/conf/mod_kafka/kafka_config.data` 中补充上述字段（ customized 模式示例）。
6. 更新 `log-reader/reader_modules/mod_kafka/field_registry_test.go` 中 Default 字段数断言。
7. 同步更新集成测试的数据准备、断言与相关设计文档。

## 详细改动

### 1. 依赖升级

**文件：** `log-reader/go.mod`

```go
require (
    ...
    github.com/bfenetworks/bfe-access-pb v0.3.5
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

在 `ai_image_count` 之后新增：

```go
registerField("ai_image_input_tokens", "int64", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiImageInputTokens()
        }
        return int64(0)
    },
    isZeroInt64,
)
registerField("ai_video_count", "int64", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil {
            return reqLog.GetAiVideoCount()
        }
        return int64(0)
    },
    isZeroInt64,
)
```

所有新增字段类型、Required/Default 标记如下：

| 字段 | 类型 | Required | Default |
|------|------|----------|---------|
| `ai_image_input_tokens` | int64 | ❌ | ✅ |
| `ai_video_count` | int64 | ❌ | ✅ |

### 3. 字段文档更新

**文件：** `log-reader/doc/modules/mod_kafka/output-fields.md`

在 `3.11. AI 可观测字段` 表格中新增：

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `ai_image_input_tokens` | int64 | ❌ | ✅ | 图片输入 Token 数，已包含在 `ai_input_tokens` 中 |
| `ai_video_count` | int64 | ❌ | ✅ | 视频生成模式下生成的视频数量 |

字段位置建议放在 `ai_image_count` 之后、`ai_ttft_us` 之前，与 image/video 计量字段保持语义相邻。

并更新底部统计汇总：

| 类别 | 修改前 | 修改后 |
|------|--------|--------|
| AI 可观测字段数 | 27 | 29 |
| 总字段数 | 80 | 82 |
| Default 字段总数 | 60 | 62 |

**文件：** `log-reader/doc/configuration/mod_kafka/kafka_config.data.md`

在 **AI 可观测字段** 表格中同步补充上述字段。

### 4. 生产配置（可选）

**文件：** `log-reader/conf/mod_kafka/kafka_config.data`

如需在生产环境中输出新增字段，可在 `FieldNames` 列表中新增对应行：

```ini
FieldNames= ai_image_count
FieldNames= ai_image_input_tokens
FieldNames= ai_video_count
```

当 `FieldMode = default` 或 `all` 时，由于这些字段已加入 Default 字段集，无需额外配置即可输出。

### 5. 单元测试更新

**文件：** `log-reader/reader_modules/mod_kafka/field_registry_test.go`

将 `TestFieldRegistry_DefaultFieldsCount` 的期望值从 62 更新为 64（新增 2 个 Default 字段）。

可补充针对新增字段的提取测试，例如：

```go
func TestFieldRegistry_ExtractAiImageInputTokens(t *testing.T) {
    log := makeBfeLog()
    val, isZero := Extract("ai_image_input_tokens", log)
    if isZero {
        t.Fatal("ai_image_input_tokens should not be zero")
    }
    if got, want := val.(int64), int64(200); got != want {
        t.Errorf("ai_image_input_tokens = %d, want %d", got, want)
    }
}

func TestFieldRegistry_ExtractAiVideoCount(t *testing.T) {
    log := makeBfeLog()
    val, isZero := Extract("ai_video_count", log)
    if isZero {
        t.Fatal("ai_video_count should not be zero")
    }
    if got, want := val.(int64), int64(3); got != want {
        t.Errorf("ai_video_count = %d, want %d", got, want)
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

#### 6.1 `log_generator.go`

在 `MakeRequestLog` 中为 `RequestLog` 填充新增字段的测试数据：

```go
AiImageCount:         int64Ptr(1),
AiImageInputTokens:   int64Ptr(200),
AiVideoCount:         int64Ptr(3),
```

#### 6.2 `lr01_basic_flow_test.go`

在基础流程断言中增加对新增字段的校验：

```go
assertFieldEquals(t, payload, "ai_image_count", float64(1))
assertFieldEquals(t, payload, "ai_image_input_tokens", float64(200))
assertFieldEquals(t, payload, "ai_video_count", float64(3))
```

#### 6.3 集成测试配置

将 `testdata/mod_kafka/kafka_config.data` 中的字段列表同步更新，确保 `default` 模式或 `customized` 模式覆盖新增字段。

#### 6.4 集成测试文档

同步更新 TC-01、场景说明中的字段覆盖描述、字段数量（62 → 64）与预期输出示例。

## 代码变更说明

- `log-reader/go.mod` / `go.sum`：`bfe-access-pb` 升级到 v0.3.5。
- `log-reader/reader_modules/mod_kafka/field_registry.go`：注册 `ai_image_input_tokens`、`ai_video_count`。
- `log-reader/reader_modules/mod_kafka/field_registry_test.go`：Default 字段数断言更新为 64，可补充新增字段提取测试。
- `log-reader/doc/modules/mod_kafka/output-fields.md`：文档新增字段说明与统计更新。
- `log-reader/doc/configuration/mod_kafka/kafka_config.data.md`：新增可用字段说明。
- `log-reader/conf/mod_kafka/kafka_config.data`：增加新增字段输出。
- `log-reader/tests/integration/common/log_generator.go`：填充新增字段测试数据。
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go`：新增字段断言。
- `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data`：同步字段列表。
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md`：更新字段列表与数量。
- `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md`：更新字段列表与数量。
- `log-reader/doc/modifications/2026-08-31-add-image-input-token-and-video-fields/design-changes.md`：本设计变更文档。

## 验证步骤

1. 确认文档字段已注册：

```bash
cd log-reader
grep -n "ai_image_input_tokens\|ai_video_count" doc/modules/mod_kafka/output-fields.md
grep -n "ai_image_input_tokens\|ai_video_count" doc/configuration/mod_kafka/kafka_config.data.md
```

2. 确认字段注册代码：

```bash
cd log-reader
grep -n "ai_image_input_tokens\|ai_video_count" reader_modules/mod_kafka/field_registry.go
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
| `log-reader/go.mod` / `go.sum` | `bfe-access-pb` 升级到 v0.3.5 |
| `log-reader/reader_modules/mod_kafka/field_registry.go` | 注册 `ai_image_input_tokens`、`ai_video_count` |
| `log-reader/reader_modules/mod_kafka/field_registry_test.go` | Default 字段数断言更新为 64 |
| `log-reader/doc/modules/mod_kafka/output-fields.md` | 文档新增字段说明与统计更新 |
| `log-reader/doc/configuration/mod_kafka/kafka_config.data.md` | 可用字段列表补充新增字段 |
| `log-reader/conf/mod_kafka/kafka_config.data` | 增加新增字段输出 |
| `log-reader/tests/integration/common/log_generator.go` | 填充新增字段测试数据 |
| `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/lr01_basic_flow_test.go` | 新增字段断言 |
| `log-reader/tests/integration/implementation/scenario-LR01-basic-flow/testdata/mod_kafka/kafka_config.data` | 同步字段列表 |
| `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md` | 更新字段列表 |
| `log-reader/tests/integration/测试设计文档/scenario-LR01-basic-flow/场景说明.md` | 更新字段列表 |
| `log-reader/doc/modifications/2026-08-31-add-image-input-token-and-video-fields/design-changes.md` | 本设计变更文档 |

## 依赖与兼容性

- 依赖 `bfe-access-pb` v0.3.5 或更高版本（已发布 `ai_image_input_tokens` / `ai_video_count`）。
- 新增字段均为可选字段，下游消费方未使用时不影响现有逻辑；使用方按需解析即可。
- 新增字段已纳入 Default 字段集，`FieldMode = default` 时自动输出。
- 集成测试中需确保 `bfe-access-pb` v0.3.5 的 `.pb.go` 包含 `GetAiImageInputTokens()`、`GetAiVideoCount()` 等 getter。

---

*文档生成日期：2026-08-31*
