# log-reader 输出字段增加 Cache Token 计费字段

## 背景

BFE 在 AI 网关场景下扩展了 cache 计费能力，访问日志 protobuf（`bfe-access-pb` v0.3.1）中新增了两个 AI 可观测字段：

- `ai_cache_read_tokens`（字段编号 781）
- `ai_cache_write_tokens`（字段编号 782）

`log-reader` 的 Kafka 输出字段文档需要同步更新，以便下游消费方了解新增字段的含义与类型。

## 修改目标

1. 在 `log-reader/doc/modules/mod_kafka/output-fields.md` 的 **AI 可观测字段** 章节中注册 `ai_cache_read_tokens` 和 `ai_cache_write_tokens`。
2. 更新文档底部的字段统计汇总（AI 可观测字段数、总字段数、Default 字段数）。

## 详细改动

### 1. 字段列表更新

在 `log-reader/doc/modules/mod_kafka/output-fields.md` 的 `3.11. AI 可观测字段` 表格中新增：

| JSON 字段 | 类型 | Required | Default | 说明 |
|-----------|------|----------|---------|------|
| `ai_cache_read_tokens` | int64 | ❌ | ✅ | 从 cache 读取的 Token 数 |
| `ai_cache_write_tokens` | int64 | ❌ | ✅ | 写入 cache 的 Token 数 |

字段位置建议放在 `ai_total_tokens` 之后、`ai_ttft_us` 之前，与 token 计量字段保持语义相邻。

### 2. 统计汇总更新

| 类别 | 修改前 | 修改后 |
|------|--------|--------|
| AI 可观测字段数 | 20 | 22 |
| 总字段数 | 73 | 75 |
| Default 字段总数 | 53 | 55 |

## 代码变更说明

`log-reader` 的 `mod_kafka` 字段注册为显式注册，因此需要同步修改代码：

1. **依赖升级**：`log-reader/go.mod` 中 `github.com/bfenetworks/bfe-access-pb` 从 `v0.3.0` 升级到 `v0.3.1`。
2. **字段注册**：在 `reader_modules/mod_kafka/field_registry.go` 中，`ai_total_tokens` 之后新增：
   - `ai_cache_read_tokens`：调用 `reqLog.GetAiCacheReadTokens()`
   - `ai_cache_write_tokens`：调用 `reqLog.GetAiCacheWriteTokens()`
   - 类型均为 `int64`，Default 字段集为 `true`，零值判断使用 `isZeroInt64`。
3. **单元测试**：更新 `reader_modules/mod_kafka/field_registry_test.go` 中 `TestFieldRegistry_DefaultFieldsCount` 的期望值，从 55 改为 57。

## 验证步骤

1. 确认文档字段数统计正确：

```bash
cd log-reader
grep -n "ai_cache_read_tokens\|ai_cache_write_tokens" doc/modules/mod_kafka/output-fields.md
```

2. 若 `log-reader` 为显式字段注册，检查字段注册代码：

```bash
grep -n "ai_cache_read_tokens\|ai_cache_write_tokens" reader_modules/mod_kafka/field_registry.go
```

3. 运行单元测试：

```bash
cd log-reader
go test ./reader_modules/mod_kafka/...
```

## 影响范围

| 文件 | 影响 |
|---|---|
| `log-reader/go.mod` / `go.sum` | `bfe-access-pb` 升级到 v0.3.1 |
| `log-reader/reader_modules/mod_kafka/field_registry.go` | 注册 `ai_cache_read_tokens`、`ai_cache_write_tokens` |
| `log-reader/reader_modules/mod_kafka/field_registry_test.go` | Default 字段数断言更新为 57 |
| `log-reader/doc/modules/mod_kafka/output-fields.md` | 文档新增 cache 字段说明与统计更新 |
| `log-reader/doc/modifications/2026-08-22-add-cache-token-fields/design-changes.md` | 本设计变更文档 |

## 依赖与兼容性

- 依赖 `bfe-access-pb` v0.3.1 或更高版本（已发布 `ai_cache_read_tokens` / `ai_cache_write_tokens`）。
- 新增字段为可选字段，下游消费方未使用时不影响现有逻辑；使用方按需解析即可。
