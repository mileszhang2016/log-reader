# mod_log_mysql 模块设计（MySQL 输出）

> 状态：待实现（计划随 v1.4.0 发布）。实施细节与变更清单见 [doc/modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md](../modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md)。

## 1. 背景与目标

### 1.1 背景

报表标准形态依赖 Kafka/Doris/Grafana 三个外部组件，对小规模/私有化部署过重。新增「轻量形态」：访问日志由 log-reader 直接写入 MySQL，报表查询由 ai-gateway-api 提供 API、ai-gateway-web 展示，全程无需 Kafka/Doris/Grafana。

### 1.2 目标

1. 消费 pb 访问日志，批量幂等写入 MySQL 明细表 `bfe_ai_request_log`（与 Doris 明细表同名同列）；
2. 写路径异步化：缓冲队列 + 单写协程攒批，事务批插，对读取主流程零阻塞；
3. 背压保护：队列满丢弃计数；写失败退避重试；
4. 复用 mod_kafka 的字段抽取能力（公共包 `mod_fields`），保证两链路字段口径一致；
5. 完整监控计数与排障指引。

### 1.3 非目标

- 不做聚合计算——分钟级聚合由 ai-gateway-api 侧 JOB 完成（与本模块解耦）；
- 不做自动建表/改表——DDL 归 ai-gateway-api 仓库持有，本模块账号无 DDL 权限；
- 不做死信落盘——写失败丢弃计数，依赖幂等键支持 `-b` 补读重放；
- v1 仅 MySQL；PostgreSQL 预留 `database/sql` 驱动替换点，不实现。

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| 幂等键 | 唯一键 `(hostid, log_time, ai_apikey_id, ai_requested_model)`，冲突覆盖更新 |
| 打平列 | `level1Name/level1 … level5Name/level5`，写入时由 `ai_apikeytags` 打平生成 |
| 零值规则 | 字符串空串写 NULL、数值/布尔原样、空 JSON 写 NULL（下游聚合 IFNULL 归一） |
| 补读重放 | 以 `log_reader -b` 从头重读 pb 日志，依赖幂等键安全覆盖 |
| 分区管理 JOB | ai-gateway-api 侧的按天 RANGE 分区创建/清理任务（本模块不参与） |

## 3. 在系统中的位置

```
PbLogReader 批次分发
   └─► mod_log_mysql.Update(batch)
         ├─ 过滤：仅 BfeLogType_Request
         ├─ mod_fields 抽取（与 mod_kafka 共用）→ field_mapper 组装行（89 列固定列序）
         │    （字符串零值→NULL；apikeytags 打平；JSON 列 marshal）
         └─ RecordWriter.Enqueue(row)
               └─ recordCh（容量 QueueSize，非阻塞）
                     └─ 写协程：攒批（BatchSize 或 FlushIntervalMs）→ 事务批插
                           INSERT ... ON DUPLICATE KEY UPDATE（幂等覆盖）
                           ├─ 失败 → 指数退避重试 MaxRetries 次
                           └─ 耗尽 → 丢弃该批，计数 SEND_MYSQL_FAILED
```

## 4. 详细设计

### 4.1 写入语义

| 项 | 设计 |
|----|------|
| 语句 | 单事务多值 `INSERT INTO <table> (89 列) VALUES (...),(...) ON DUPLICATE KEY UPDATE col=VALUES(col), ...`（全列覆盖） |
| 幂等 | 唯一键四元组与 Doris UNIQUE KEY 完全一致；重发/重启/`-b` 补读均不产生重复行 |
| 唯一键取舍 | 唯一键**不含 `logid`**：同一分钟内同 Key 同模型的多条请求只留一条（与 Doris 语义一致，以聚合可接受为前提） |
| 零值规则 | 字符串空串 → NULL；数值/布尔原样（`ai_stream=0`、`ai_retry_count=0`）；nil/空 JSON → NULL |
| 时间列 | `log_time` 由 pb `timestamp`（Unix 秒）写时转 DATETIME（Doris 链路为 Routine Load `FROM_UNIXTIME`，同口径） |
| 标签打平 | `levelNName/levelN` 写时从 `ai_apikeytags` 对象打平；无标签 → NULL |

### 4.2 背压与重试

- **入队非阻塞**：`recordCh` 容量 `QueueSize`（默认 2000），满则丢弃并计数 `SENT_MYSQL_CHN_FULL`——与 mod_kafka 同策略，读取主流程永不被下游拖住；
- **攒批**：单写协程，条数 `BatchSize`（默认 200）或超时 `FlushIntervalMs`（默认 2000ms）先到先发；
- **重试**：失败按 200ms/400ms/800ms 指数退避重试至 `MaxRetries`（默认 3），耗尽丢弃该批并计数 `SEND_MYSQL_FAILED`（v1 不落盘死信，恢复后靠 `-b` 补读重放）；
- **关闭**：`Close()` 停 ticker → drain 残留批 → 最终 flush → 关连接池（进程优雅退出路径由框架保证调用）；
- **panic 防护**：写协程 recover；停滞可通过 `SENT_TO_MYSQL` 计数停增发现。

### 4.3 表结构（归 ai-gateway-api 仓库持有）

`bfe_ai_request_log` 89 列，与 Doris 同名同列；差异仅复合列类型（`ARRAY<STRUCT>` → `JSON`，共 7 个 JSON 列）。**DDL 权威文件在 ai-gateway-api 仓库 `db_ddl_report_mysql.sql`**（与聚合表 `bfe_ai_metrics_1m` 同文件），归属理由：

1. ai-gateway-api 依赖面最广：聚合 JOB 按列取数、分区管理 JOB 执行 `ALTER TABLE`、明细查询，且运行时持有 DDL 权限；本模块仅按固定列清单 INSERT，账号无 DDL 权限；
2. ai-gateway-api 是代码库中唯一有 MySQL DDL 管理传统的仓库（`db_ddl.sql` 体系），两表与其同发布、同演进；
3. 发布顺序 schema-first：先执行 DDL 再启用插件。

列清单与字段语义见 [output-columns.md](../modules/mod_log_mysql/output-columns.md)；`field_mapper` 代码内列常量表是写入侧列序的唯一权威，单测校验其与 DDL 一致。

### 4.4 索引与分区

- 唯一键 `uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model)`（约 2565 字节 < 3072 InnoDB 上限；含分区列满足 MySQL 分区表约束）；
- 二级索引面向报表查询过滤维度：`idx_model_time`、`idx_apikey_time`、`idx_provider_time`、`idx_host_time`、`idx_status_time`；
- 按天 `RANGE (TO_DAYS(log_time))` 分区；首个分区由建表 DDL 提供，新分区创建与过期分区 DROP 由 ai-gateway-api 分区管理 JOB 负责（对齐 Doris 动态分区语义，保留期默认 7 天）。

### 4.5 配置

`conf/mod_log_mysql/mod_log_mysql.conf`（INI，gcfg）：

| 节 | 配置项 | 默认 | 说明 |
|----|--------|------|------|
| `[Basic]` | `OpenDebug` | false | 打印每行组装结果 |
| `[mysql]` | `Addr` / `User` / `Password` | 必填 | DSN 三要素 |
| `[mysql]` | `DBName` / `Table` | 必填 | 目标库表（默认 `bfe_report` / `bfe_ai_request_log`） |
| `[Writer]` | `QueueSize` | 2000 | 缓冲队列容量 |
| `[Writer]` | `BatchSize` | 200 | 攒批条数 |
| `[Writer]` | `FlushIntervalMs` | 2000 | 攒批超时 |
| `[Writer]` | `MaxRetries` | 3 | 写失败重试次数 |
| `[Writer]` | `MaxOpenConns` / `MaxIdleConns` | 10 / 5 | 连接池 |

**凭据安全**：部署上为插件建专用最小权限 MySQL 账号——仅目标库目标表 INSERT/UPDATE 权限（`ON DUPLICATE KEY UPDATE` 需要 UPDATE），无 DDL/DELETE/CREATE。

启用（`config.conf`）：`Modules = mod_log_mysql`（与 mod_kafka 并存语法：`mod_kafka:false, mod_log_mysql:true`）。

### 4.6 监控指标

`mod_log_mysql(_diff)` 计数：

| 指标 | 含义 | 异常信号 |
|------|------|----------|
| `RECEIVED_LOGS` / `RECEIVED_REQ` | 收到条数 / request 条数 | 与 `bfe_reader.SUM_PB_RECORD` 对不上 |
| `CONVERT_FAILED` | 行组装失败 | 持续增长排查字段映射 |
| `SENT_TO_MYSQL` | 入队成功 | 停滞 = 写管道异常 |
| `SENT_MYSQL_CHN_FULL` | 队列满丢弃（背压） | 持续增长 = MySQL 写入跟不上 |
| `SEND_MYSQL_FAILED` | 重试耗尽丢弃批 | >0 即告警 |
| `WRITE_BATCH_SIZE` | 每批条数累计 | diff 均值观测批大小 |

## 5. 降级与兼容性

| 场景 | 行为 |
|------|------|
| MySQL 不可用 | 重试耗尽丢弃（计数），读取主流程不受影响；恢复后 `-b` 补读重放缺口 |
| 表不存在/权限不足 | Init 阶段 fail-fast，进程拒绝以该模块启动（不影响其他模块与读取） |
| pb 加字段 | 本模块列清单不变（与 Doris 表对齐演进，两侧同步加列）；未纳入字段不落库 |
| 与 mod_kafka 并存 | 支持（Modules 多模块），但单集群建议只启用一种落库形态，避免两条链路数据缺口窗口不一致 |
| 进程重启 | 从文件尾读（丢窗口）或 `-b` 全量重放（幂等覆盖），同框架语义 |
| 回滚 | 纯增量插件；停用 = 配置移除 + 重启；已写数据不受影响 |

## 6. 关键文件索引（计划新增）

| 文件 | 内容 |
|------|------|
| `reader_modules/mod_log_mysql/mod_log_mysql.go` | ModuleLogMysql：接口实现、Init/Update/Close、监控注册 |
| `reader_modules/mod_log_mysql/field_mapper.go` | 89 列固定列序行组装、零值规则、标签打平、JSON marshal |
| `reader_modules/mod_log_mysql/record_writer.go` | 缓冲队列、攒批、事务批插、退避重试、drain |
| `reader_modules/mod_log_mysql/conf_mod_log_mysql.go` | INI 配置解析与校验 |
| `reader_modules/mod_fields/` | 公共字段抽取包（自 mod_kafka 上移，两模块共用） |
| `reader_modules/all_modules.go` | 注册新模块 |
| `tests/integration/implementation/scenario-LR03-mysql-write/` | 集成测试 |

关联文档：[overview.md](./overview.md)（系统总体设计）、[mod_log_mysql.md](../modules/mod_log_mysql/mod_log_mysql.md)（模块参考）、[output-columns.md](../modules/mod_log_mysql/output-columns.md)（列清单）、[实施变更文档](../modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md)。
