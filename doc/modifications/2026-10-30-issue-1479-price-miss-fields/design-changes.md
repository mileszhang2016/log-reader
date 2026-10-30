# log-reader 输出与入库字段增加缺价信号 `ai_price_miss` / `ai_price_warn`（bfe issue #1479）

> 日期：2026-10-30　　issue：[bfenetworks/bfe#1479](https://github.com/bfenetworks/bfe/issues/1479)
> 状态：**已实施**（代码、文档、单元测试与 LR03 集成用例均已落地，见 §7 实施记录；尚未提交/推送）
> 上游（均已交付）：
> - `bfe-access-pb` **v0.3.17**：新增 `ai_price_warn`（optional string，field **766**）；`ai_price_miss`（optional bool，field **763**）为既有字段
> - `bfe` `v1.9.0-dev` commit `39c15e10`：缺价动作三态 + 信号四层，`ai_price_miss` 写出时机改为请求级（非 2xx 的 reject 行也写出）、`ai_price_warn` 形态枚举
> - `ai-gateway-api` `v0.1.0-dev` commit `508e552`：`db_ddl_report_mysql.sql` 明细表 **99 → 101 列**（`ai_price_miss TINYINT DEFAULT NULL`、`ai_price_warn VARCHAR(32) DEFAULT NULL`，追加在 `ai_intent_questions_version` 之后）+ 报表查询投影/扫描/`price_warn` 过滤
> 本仓库是链路上**剩余的一环**：`mod_log_mysql` 的 `field_mapper.go` 是写入侧列清单与列序的唯一权威，列序必须与 ai-gateway-api DDL 完全一致；`mod_fields` 决定 Kafka JSON 输出是否携带这两字段。

## 1. 背景

问题3（v0.7.0.1 测试）：模型缺少某接口定价时该接口请求不扣费。bfe #1408 先做了"回退 chat 价 + `ai_price_miss` 标记"，#1479 补齐三态动作（`fallback_chat`/`bill_zero`/`reject`）和形态化信号：

| 字段 | 语义 | 取值 |
|------|------|------|
| `ai_price_miss`（bool，763） | 该请求是否发生过缺价 | `true` / 未设置；**非 2xx 的 reject 行也会出现**（bfe 写出时机为请求级） |
| `ai_price_warn`（string，766） | 缺价形态 | `fallback` / `no_entry` / `no_baseline` / `reject` / `subitem_missing` / `no_model_table`；多因取最具体；未缺价为空 |

两字段当前在本仓库**均未注册**：既不进 `mod_fields` 的 Kafka 输出，也不进 `mod_log_mysql` 的明细表列清单——导致控制台"命中缺定价接口"的报表口径缺数。

明确不涉及的字段：无。本批只加这两列，不改既有列语义、不改幂等覆盖语义（与 Doris UNIQUE KEY 一致，见 `2026-10-13-issue-1417-dedup-key-logid`）。

## 2. 修改目标

| # | 目标 | 验证标准 |
|---|------|----------|
| 1 | `mod_log_mysql` 写入 101 列 | `columnDefs` 99 → 101，两列追加在 `ai_intent_questions_version` 之后，**列序与 ai-gateway-api `db_ddl_report_mysql.sql` 逐字一致** |
| 2 | `mod_fields` 注册两字段 | Kafka JSON 输出含 `ai_price_miss`/`ai_price_warn`，零值省略规则与既有 optional 字段一致 |
| 3 | 依赖升级 | `go.mod`：`bfe-access-pb v0.3.9 → v0.3.17`（`AiPriceMiss`/`AiPriceWarn` getter 仅新版本可用） |
| 4 | 文档同步 | `doc/modules/mod_log_mysql/output-columns.md`（99→101 列 + 两列定义）、`mod_kafka/output-fields.md`、`doc/modules/mod_log_mysql/mod_log_mysql.md` 列数表述 |
| 5 | 集成测试 | LR03 新增"两字段写入与零值规则"用例；LR03/LR04 内嵌 DDL 副本同步 101 列 |

## 3. 详细改动

### 3.1 依赖升级

`log-reader/go.mod`：`github.com/bfenetworks/bfe-access-pb v0.3.9 → v0.3.17`（该版本已在上游发布 Release，含 field 766）。本地如遇代理不可用，可临时启用 `go.mod` 中已注释的 `replace ... => ../bfe-access-pb`。

### 3.2 `reader_modules/mod_log_mysql/field_mapper.go`

1. **新增列类型**：现有 `kindBoolTiny` 是"bool → TINYINT NOT NULL（false→0/true→1）"，但两列都是**可空**（`DEFAULT NULL`，NULL = 未发生缺价），需新增：

   ```go
   kindOptionalBoolTiny // proto optional bool：指针 nil -> NULL（未设置），非 nil -> 1/0
   ```

   在 `field_mapper.go` 的列值抽取 switch 中实现（读取 `reqLog.AiPriceMiss != nil` 判存在性）。

2. **追加两列**（位置：`ai_intent_questions_version` 之后，即 `columnDefs` 末尾）：

   ```go
   // === 缺价信号列（2026-10-30 加列，issue #1479，bfe-access-pb v0.3.17 字段 763/766） ===
   // 列序与 ai-gateway-api db_ddl_report_mysql.sql 明细表新列顺序逐字一致；
   // NULL = 未发生缺价；ai_price_miss 在非 2xx 的 reject 行也会出现。
   {name: "ai_price_miss", field: "ai_price_miss", kind: kindOptionalBoolTiny},
   {name: "ai_price_warn", field: "ai_price_warn", kind: kindOptionalStr},
   ```

   其中 `kindOptionalStr` 若不存在需与 `kindOptionalBoolTiny` 一并新增（proto optional string：nil → NULL 而非 `""`，与 `kindNotNullStr` 的 `""` 语义区分）；若仓库已有等价列类型，直接复用并在注释标注。

1.1 **配套抽取函数**：`ToRow` 的列值统一经 `mod_fields.Extract(col.field, log)`（`field_mapper.go:322-338`），既有 `kindOptionalNum` 走专用 `extractOptionalNum` 按 proto 指针判存在。因此需新增 `extractOptionalBool`（`reqLog.AiPriceMiss != nil` → nil/1/0）与 `extractOptionalStr`（`reqLog.AiPriceWarn != nil` → nil/值），**不能**复用 `kindBoolTiny`（会把 NULL 写成 0）或 `kindNotNullStr`（会把 NULL 写成 `""`）。

3. `Columns()` 与 `ON DUPLICATE KEY UPDATE` 子句均由 `columnDefs` 生成，**无需改 `record_writer.go`**。

### 3.3 `reader_modules/mod_fields/field_registry.go`

按既有 `registerField(name, typ, required, default, extract, isZero)` 注册两字段（位置紧随 `ai_intent_questions_version`）：

```go
registerField("ai_price_miss", "bool", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil && reqLog.AiPriceMiss != nil {
            return *reqLog.AiPriceMiss
        }
        return nil
    },
    isZeroBool,
)
registerField("ai_price_warn", "string", false, true,
    func(bfeLog *bfe_access_pb.BfeLog) interface{} {
        if reqLog := bfeLog.GetRequestLog(); reqLog != nil && reqLog.AiPriceWarn != nil {
            return *reqLog.AiPriceWarn
        }
        return nil
    },
    isZeroString,
)
```

- **两字段都返回 `nil`（不是 `false`/`""`）**：`ConvertBfeLogToJSON` 对 `isZero` 不做省略（`json_converter.go:60-64` 的 `if isZero` 分支是注释掉的），`nil` 直接序列化为 JSON `null`，与 mod_log_mysql 写 `NULL` 同口径；若 `ai_price_warn` 用 `GetAiPriceWarn()`（未设置返回 `""`），Kafka 侧会写出空串、MySQL 侧写 NULL，两链路口径分叉；

- **`default=true`（关键）**：`DefaultFields()`（`field_registry.go:118-125`）只返回 `FieldDef.Default==true` 的字段，`mod_kafka.DefaultOutputFields()`（`kafka_data_config.go:121-127`）直接由其构建——若注册为 `false`，两字段**不会进入 Kafka 默认输出**（也不会出现在默认字段清单断言里）。同类先例均为 `required=false, default=true`：`ai_cache_status`（`:1008`）、`ai_intent_questions_version`（`:1073`）。`isZero*` 回调只影响 `Extract` 的第二个返回值，`ConvertBfeLogToJSON` 并不据此省略字段，未设置时输出的是字段零值/`nil`；
- 如产品明确要求"缺价字段默认不进 Kafka（控制日志体积）"，才改为 `default=false`，并要求运维在 `output_fields` 显式开启；本方案默认 `true`（缺价是可观测性字段，与 ai_cache/ai_intent 同族）。
- **`mod_kafka` 无需代码改动**：`json_converter.go:61` 经 `mod_fields.Extract(fieldName, bfeLog)` 取值，`DefaultOutputFields()` 由 `mod_fields.DefaultFields()` 构建——只要本批以 `default=true` 注册，Kafka JSON 自动携带两字段（`2026-10-01-align-mod-kafka-output-with-mod-log-mysql` 的对齐结论）。前置条件即上一条的 `default=true`；若遗漏，Kafka 侧静默缺字段且不会有编译错误，属本批最高风险的漏测点，测试须显式断言（§3.5）。

### 3.4 文档同步

| 文件 | 改动 |
|------|------|
| `doc/modules/mod_log_mysql/output-columns.md` | 列数 99 → 101；新增两列定义（类型/NULL 语义/取值枚举/非 2xx 行也会出现） |
| `doc/modules/mod_log_mysql/mod_log_mysql.md` | 列数表述与"最新字段批次"备注 |
| `doc/modules/mod_kafka/output-fields.md` | 新增两字段说明（字段集由 `mod_fields.DefaultFields()` 派生，`default=true` 即进默认输出），统计汇总 AI 可观测 39→41 / 总计 92→94 |
| `doc/sys_design/mod_log_mysql.md` | 列数表述 99→101、写入语句列数、零值规则含两列 |
| `CHANGELOG.md` | 新增 `[Unreleased]`：pb 升级、99→101 列、Kafka 默认集 92→94、TC-08 |
| `tests/integration/测试设计文档/scenario-LR03-mysql-write/` | 新增 `TC-08-缺价信号字段写入与NULL规则.md`；`场景说明.md` / `TC-01` / `TC-05` / `测试场景总体说明.md` 列数与用例清单同步 |
| `tests/integration/README.md` | LR03 覆盖表新增 TC-08 行，列数 99→101 |
| `.agents/skills/log-reader-rd-workflow/SKILL.md` | 去掉"99 列"硬编码表述，改为以权威 DDL 为准 |

### 3.5 集成测试

1. **LR03 新增用例 TC-08**（`scenario-LR03-mysql-write`，`TestLR03_PriceMissColumns`）：
   - Part 1 `fallback`（`ai_price_miss=true, ai_price_warn=fallback`）→ 读回 `1` / `'fallback'`；
   - Part 2 `reject`（另带 `res_status_code=503`）→ 非 2xx 行同样读回 `1` / `'reject'`，锁住"bfe 按请求级写出、不依赖 usage"的行为；
   - Part 3 两字段未设置 → 两列为 **NULL**（验证 `kindOptional*` 而非 `""`/`0`）；
   - Part 4 `ai_price_miss=false` → `0`（与 NULL 可区分）、`ai_price_warn` 仍为 NULL。
2. **DDL 副本同步**：`tests/integration/implementation/scenario-LR03-mysql-write/testdata/ddl/bfe_ai_request_log.sql` 与 `scenario-LR04-mysql-startup-connect-retry/testdata/ddl/bfe_ai_request_log.sql`（两处副本逐字节相同）追加两列，头部注明基准为 ai-gateway-api `508e552` / 101 列。
3. **单元测试**：
   - `mod_log_mysql` 的 mapper 测试：`columnDefs` 长度 101、两列 kind 与 NULL 语义、列序（`ai_intent_questions_version` 之后）；
   - `mod_fields` 的 field_registry 测试：字段清单/类型/零值规则（`field_registry_test.go` 中既有清单断言需同步扩展）。
4. 回归：LR03 既有幂等/零值用例、LR04 启动重试用例不回归。

## 4. 上线顺序与回滚

1. ai-gateway-api DDL 先行（已交付 `508e552`，101 列）——**若报表库仍是 99 列而 log-reader 先写 101 列会直接报 Unknown column**，顺序不可颠倒；
2. 本仓库发版（列清单 + mod_fields + 依赖升级）；
3. 无需改 BFE / conf-agent；bfe 上报两字段的时机与开关独立（`PriceMissAction` 默认 `fallback_chat`，行为向后兼容）。

回滚：本仓库回退到 99 列版本即可（新增两列保留在表上为 NULL，不影响旧写入语句——旧语句仅显式列 INSERT，未列出的列取默认 NULL）。

## 5. 风险与开放问题

- **列序一致性**：`columnDefs` 顺序必须与 DDL 一致，且 Doris / StarRocks / ClickHouse 三套 schema 需由 `ai-gateway-observability` 同批加列（独立卡），否则非 MySQL 后端缺少这两列；
- **`ai_price_miss` 覆盖范围扩大**：非 2xx 的 reject 行也会带 `true`，报表筛选"缺价"时建议叠加 `res_status_code`（此口径已写入 ai-gateway-api `report.md`）；
- **NULL vs 零值**：两列必须区分"未发生缺价（NULL）"与"缺价且形态为空（不应出现）"；mapper 若误用 `kindBoolTiny`/`kindNotNullStr` 会把 NULL 写成 0/""，导致报表统计口径失真；
- **Kafka 静默缺字段**：若 `mod_fields` 注册时漏了 `default=true`，MySQL 写入正常但 Kafka JSON 无声缺两字段（无编译错误、无测试红）；必须由 §3.5 的 `field_registry_test` 清单断言 + `json_converter_test` 兜住；
- **依赖版本**：v0.3.17 是当前 pb 最新发布版本；bfe 侧若后续再扩枚举值（如新增形态），log-reader 无需改列类型（`VARCHAR(32)` 已预留），仅需文档更新。

## 6. 监控与计数器影响

- **不新增监控计数器**：两列只是多两个字段值，写入路径（攒批/重试/断连/行剔除）与计数口径均不变；`SENT_TO_MYSQL`、`WRITE_BATCH_SIZE` 等指标含义不受影响（行宽略增，可忽略）。
- **无新配置项**：`mod_log_mysql` 始终写全量列，`mod_kafka` 的 `FieldMode=default` 自动包含两字段——不需要改 `mod_log_mysql.conf` / `kafka_config.data`，也不需要运维在 `output_fields` 里显式开启。
- **可观测入口不变**：缺价统计由报表侧在 `bfe_ai_request_log` 上按 `ai_price_miss` / `ai_price_warn` 过滤（ai-gateway-api 已实现 `price_warn` 查询过滤），不经过 `/monitor` 页面。

## 7. 参考

- bfe 修改记录：`bfe/docs/zh_cn/modifications/2026-10-30-issue-1479-price-miss-control-and-observability/design-changes.md`
- ai-gateway-api 修改记录：`design-docs/modifications/2026-10-30-issue-1479-price-miss-action/`（change-summary / api-changes / design-changes）
- DDL 权威：ai-gateway-api `db_ddl_report_mysql.sql`（明细表 101 列）
- 先例：`doc/modifications/2026-09-27-add-cache-mirror-intent-fields/design-changes.md`（同类"Kafka + MySQL 双注册"批次）

## 8. 实施记录（2026-10-30）

| 文件 | 改动 |
|------|------|
| `go.mod` / `go.sum` | `bfe-access-pb v0.3.9 → v0.3.17` |
| `reader_modules/mod_log_mysql/field_mapper.go` | 新增 `kindOptionalBoolTiny`、`kindOptionalStr`；`columnDefs` 追加 `ai_price_miss` / `ai_price_warn`（`ai_intent_questions_version` 之后）；`ToRow` 两条分支；新增 `extractOptionalBool` / `extractOptionalStr` |
| `reader_modules/mod_fields/field_registry.go` | 注册两字段（`required=false, default=true`），getter 均按 proto 指针返回 `nil`/值，保证 Kafka 未设置输出 `null` |
| `tests/integration/.../scenario-LR03/…/testdata/ddl/bfe_ai_request_log.sql`、`.../scenario-LR04/…/testdata/ddl/bfe_ai_request_log.sql` | 两份副本同步 101 列（`ai_intent_questions_version` 之后两列），头部基准改为 `508e552` / 101 列；`diff --strip-trailing-cr` 校验一致（两文件行尾本就不同：LR03 为 CRLF、LR04 为 LF，属既有状态） |
| `tests/integration/.../scenario-LR03/…/lr03_mysql_write_test.go` | `requestLogRow` 增两列；新增 `TestLR03_PriceMissColumns`（TC-08，四个 Part） |
| 单元测试 | `field_mapper_test.go`：列数 101、`TestToRow_PriceMissColumns`（未设置→nil、true→1、显式 false→0、字符串原值）；`field_registry_test.go`：默认集 94、`TestFieldRegistry_PriceMissFields`（含未设置返回 nil 断言）；`json_converter_test.go`：`TestConvertBfeLogToJSON_PriceMissFields` 与 `..._Unset`（未设置输出 `null`） |
| 文档 | 见 §3.4 清单（含 `CHANGELOG.md` 的 `[Unreleased]`） |

**验证结果**：

- `go test ./reader_modules/...` 四个包全绿（mod_fields / mod_kafka / mod_log_mysql / reader_modules）；
- `go vet ./tests/...` 通过；
- 真实 MySQL 8.4.9（`LR_MYSQL_DSN` 指向本机实例）下：`scenario-LR03-mysql-write`（含新增 TC-08）与 `scenario-LR04-mysql-startup-connect-retry` 全部通过。注意集成测试会缓存二进制（`tests/integration/.integration-test-bin/log-reader.exe`），改代码后必须先删除该缓存，否则跑的是旧二进制（本次踩到：TC-08 首跑因缓存二进制失败，删除后通过）。

**实施期间修正的设计偏差**：

1. 原方案 §3.3 让 `ai_price_warn` 用 `GetAiPriceWarn()`（未设置返回 `""`），会导致 Kafka 输出 `""` 而 MySQL 写 `NULL`，两链路口径分叉；已改为按 proto 指针返回 `nil`。
2. 首次落地时把 `ai_price_warn` 重复注册了两次（`registerField` 覆盖前值），表现为默认字段数 95、`Extract` 仍返回 `""`；已删除重复块。`field_registry_test` 的字段总数断言正是为此类笔误兜底。