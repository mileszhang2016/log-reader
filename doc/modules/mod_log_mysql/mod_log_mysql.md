# mod_log_mysql

## 模块简介

`mod_log_mysql` 是 `log-reader` 的 MySQL 输出模块。它负责消费解析后的 BFE 访问日志（`bfe-access-pb` protobuf 格式），将日志组装为固定列序的数据行，**批量幂等写入 MySQL 明细表 `bfe_ai_request_log`**，为 ai-gateway-api 的报表查询 API 提供数据源。

与 `mod_kafka`（转 JSON 发 Kafka，供 Doris 链路）不同，本模块面向「轻量形态」部署：无需 Kafka/Doris/Grafana，日志直接落 MySQL。

主要功能包括：

- 消费 BFE 访问日志（仅 `Request` 类型，会话日志丢弃），按固定列清单组装数据行
- 批量写入 MySQL：缓冲队列 + 单写协程攒批（条数/超时双阈值），单事务多值 `INSERT ... ON DUPLICATE KEY UPDATE` 幂等覆盖
- 背压保护：队列满非阻塞丢弃并计数，写失败指数退避重试
- web-monitor 监控：`mod_log_mysql` / `mod_log_mysql_diff`

## 数据流

```
Update(batch []*BfeLog)
  → 过滤仅 BfeLogType_Request（会话日志丢弃）
  → mod_fields 抽取（与 mod_kafka 共用字段注册表）→ field_mapper 组装行
    字符串零值 → NULL；数值/布尔原样；JSON 列 marshal；apikeytags 打平 level1~5
  → recordCh（容量 QueueSize，满丢弃计数 SENT_MYSQL_CHN_FULL）
写协程：攒批（BatchSize 或 FlushIntervalMs）→ 事务批插 → 失败退避重试
Close：drain 残留批 → 关闭连接池
```

## 基础配置

模块配置文件说明详见 [mod_log_mysql.conf](../../configuration/mod_log_mysql/mod_log_mysql.conf.md)。

启用方式（`config.conf`）：

```ini
[PbAccessLogConf]
LogFile = /home/work/bfe/log/pb_access3.log
Modules = mod_log_mysql
```

> 表结构 DDL 不在本仓库：随 ai-gateway-api 仓库 `db_ddl_report_mysql.sql` 发布（与聚合表 `bfe_ai_metrics_1m` 同文件），部署时 schema-first 先建表再启用插件。列清单与字段语义见 [output-columns.md](./output-columns.md)。

## 输出列

`bfe_ai_request_log` 共 89 列，与 Doris 明细表**同名同列**（差异仅 `ARRAY<STRUCT>` 列在本表为 `JSON` 类型）。完整列清单（含类型、来源、零值规则、JSON 结构）参见 [output-columns.md](./output-columns.md)。

| 列类别 | 主要列 |
| ------ | ------ |
| 主键/基础 | `hostid`、`log_time`、`ai_apikey_id`、`ai_requested_model`、`logid`、`product`、`log_tag` |
| 客户端连接 | `client_ip`、`client_network`、`is_trust_src_ip`、`req_num`、`session_id`、`bfe_ip`、`sock_src_ip`、`vip`、`vip6` |
| 错误 | `err_code`、`err_msg` |
| 请求 | `proto`、`header_host`、`origin_uri`、`final_uri`、`method`、`content_type`、`x_forward_for`、`accept_language`、`authorization`、`transfer_encoding`、`referrer`、`user_agent`、`delegation`、`uid`、`cookie`、`req_headers`(JSON)、`req_header_len`、`req_body_len` |
| 路由 | `cluster`、`sub_cluster`、`backend_info`、`backend_retry` |
| 响应 | `res_status_code`、`res_header_len`、`res_body_len`、`res_content_type`、`res_location`、`res_transfer_encoding`、`res_headers`(JSON) |
| 耗时（毫秒） | `all_time`、`read_client_time`、`cluster_serve_time`、`backend_serve_time`、`write_client_time`、`connect_backend_time`、`proxy_delay_time`、`session_offset_time` |
| API Key 标签（打平） | `level1Name`/`level1` … `level5Name`/`level5` |
| AI 可观测（标量） | `ai_target_model`、`ai_stream`、`ai_input_tokens`、`ai_output_tokens`、`ai_total_tokens`、`ai_cache_read_tokens`、`ai_cache_write_tokens`、`ai_audio_input_tokens`、`ai_audio_output_tokens`、`ai_image_count`、`ai_ttft_us`、`ai_tpot_us`、`ai_provider`、`ai_protocol`、`ai_mode`、`ai_retry_count`、`ai_cost_value`、`ai_cost_currency`、`ai_auth_reject_reason` |
| AI 可观测（JSON） | `ai_route_rule_hits`、`ai_cluster_key_names`、`ai_rate_limit_hits`、`ai_auth_reject_quota_plans`、`ai_auth_hit_quota_plans` |

## 写入语义要点

- **幂等**：唯一键 `(hostid, log_time, ai_apikey_id, ai_requested_model)`，冲突覆盖更新——log-reader 重启补读（`-b`）或重发不会产生重复行；
- **零值规则**：字符串空串写 `NULL`（下游聚合 `IFNULL/COALESCE` 归一，与 Doris 口径一致）；数值/布尔原样写入；空 JSON 值写 `NULL`；
- **标签打平**：`levelNName/levelN` 在写入时由 `ai_apikeytags` 打平，不等价于 Doris 侧由 Routine Load 表达式打平，效果一致；
- **分区分区**：表按天 RANGE 分区（`TO_DAYS(log_time)`），新分区创建与过期分区 DROP 由报表查询侧（ai-gateway-api）的分区管理 JOB 负责，本模块不参与。

## 监控指标

监控入口：`http://<host>:<port>/monitor/mod_log_mysql` 及 `/monitor/mod_log_mysql_diff`。

| 指标 | 含义 | 异常信号 |
| ---- | ---- | -------- |
| `RECEIVED_LOGS` | Update 收到的日志条数 | 与 `bfe_reader` 的 `SUM_PB_RECORD` 长期不同步 |
| `RECEIVED_REQ` | 其中 request 类型条数 | — |
| `CONVERT_FAILED` | 行组装失败条数 | 持续增长需排查字段映射 |
| `SENT_TO_MYSQL` | 成功入队条数 | 停滞 = 写入管道异常 |
| `SENT_MYSQL_CHN_FULL` | 队列满丢弃条数（背压） | 持续增长说明 MySQL 写入跟不上，调大 QueueSize/BatchSize 或排查库端 |
| `SEND_MYSQL_FAILED` | 重试耗尽丢弃批数 | >0 即需告警（库不可用/权限/锁冲突），数据缺口靠 `-b` 补读重放 |
| `WRITE_BATCH_SIZE` | 每批实际条数累计 | diff 均值观测批大小是否符合预期 |

## 排障要点

1. 启动报连接/权限错误：检查 `mod_log_mysql.conf` 的 DSN 与专用账号权限（仅需目标表 INSERT/UPDATE，无 DDL/DELETE）；
2. `SENT_TO_MYSQL` 不增长但 `RECEIVED_LOGS` 增长：写协程异常，查进程日志与 pprof；
3. `SEND_MYSQL_FAILED` 增长：先 `SHOW ENGINE INNODB STATUS` / 检查唯一键冲突外的错误（死锁、超时、磁盘），恢复后对 pb 日志 `bfe-pblog-tool` 导出比对缺口，停插件后 `-b` 补读重放；
4. 表不存在错误：部署流程漏执行 ai-gateway-api 仓库的 `db_ddl_report_mysql.sql`。

## 与 mod_kafka 的关系

两个模块共用 `reader_modules/mod_fields` 的字段抽取与类型转换逻辑（IP 转点分、`backend_info` 拼 `IP:Port`、apikeytags 打平、hostid 注入等）：mod_kafka 在其上做 `json.Marshal`，mod_log_mysql 在其上做行数组组装。`FieldMode/FieldNames` 字段裁剪配置仅属于 mod_kafka；mod_log_mysql 始终写全量 89 列。
