# mod_kafka 模块设计（Kafka 输出）

## 1. 背景与目标

### 1.1 背景

报表标准形态链路为 `log-reader → Kafka → Doris（Routine Load）→ Grafana`。Doris Routine Load 消费 JSON 消息写入明细表，因此 log-reader 需要把 pb 访问日志转换为 JSON 并可靠地写入 Kafka。`mod_kafka` 是该链路的输出模块。

### 1.2 目标

1. pb 访问日志 → JSON 的字段抽取与类型转换（含全部 AI 可观测字段）；
2. 批量异步发送：应用层攒批 + kafka-go 自身批量，失败重试与死信兜底；
3. 背压保护：下游慢时丢弃并计数，绝不影响读取主流程；
4. 字段输出集合可配置（require/default/all/customized 四档）；
5. 完整监控计数。

### 1.3 非目标

- 不做消息分区键设计（消息不设 key，Hash balancer 退化为轮询分区——Doris Routine Load 消费无需分区有序性）；
- 不重试到磁盘（无本地 spill），死信仅当配置了 DLQ topic 时生效；
- 不承载字段语义定义——字段语义以 `bfe-access-pb` 仓库 `docs/protobuf.md` 为权威。

## 2. 术语定义

| 术语 | 定义 |
|------|------|
| FieldMode | 字段输出模式：`require`（仅必需）/ `default`（默认集）/ `all`（全量）/ `customized`（必需 + 指定字段） |
| Required 字段 | 无论何种模式都输出的字段（logid/timestamp/product/hostid/client_ip/err_code/err_msg/req_*_len/proto/header_host/origin_uri/method/res_*_len/all_time 等 22 个） |
| Default 字段 | 默认字段集（62 个）成员，`FieldMode = default` 时输出 |
| DLQ | 死信 topic，发送重试耗尽的消息整批写入 |
| hostid | 实例标识，本模块经 `reader_util.GetHostId()` 生成后注入每条 JSON |

## 3. 在系统中的位置

```
PbLogReader 批次分发
   └─► mod_kafka.Update(batch)
         ├─ 过滤：仅 BfeLogType_Request
         ├─ ConvertBfeLogToJSON：按字段注册表抽取 → 单层 JSON map
         └─ KafkaProducer.Send(jsonBytes)
               └─ msgCh（容量 BatchSize*2，非阻塞）
                     └─ sendLoop：攒批（BatchSize 或 LingerMs）→ WriteMessages
                           ├─ 成功 → SENT_TO_KAFKA
                           ├─ 失败重试（kafka-go MaxAttempts）→ 整批 DLQ
                           └─ msgCh 满 → 丢弃计数 SENT_KAFKA_CHN_FULL
```

## 4. 详细设计

### 4.1 字段抽取与类型转换

**字段注册表**（`field_registry.go`）：每个字段注册五元组——名称、类型、Required 标记、Default 标记、抽取函数（`func(*BfeLog) interface{}` + 零值判定）。`Extract(fieldName, log)` 按名取值，供所有模式共用。当前注册 82 个字段（Required 22 / Default 62），字段全集与复合结构见 [output-fields.md](../modules/mod_kafka/output-fields.md)。

**类型转换**（抽取函数内完成）：

| 转换 | 规则 |
|------|------|
| IP（uint32） | 转点分十进制字符串（client_ip、bfe_ip、sock_src_ip、vip）；IPv6 直接取字符串 |
| `backend_info` | `InstanceInfo{ip, port}` 拼为 `"ip:port"` |
| `ai_apikeytags` | repeated 结构按 `taglevel` 打平为对象 `{"levelN": {"tagname","tagvalue"}}`（N=1~5） |
| `req_headers` / `res_headers` | 转 `[{"key","value"}]` 数组 |
| `ai_rate_limit_hits` / `ai_route_rule_hits` / `ai_cluster_key_names` | 转 JSON 对象数组 |
| `product` | 优先 `RequestLog.Product`，空则回退 `BfeLog.Product` 枚举名 |
| `hostid` | 非日志内容，模块注入 |

**零值行为**：抽取值为零值的字段**仍输出**（值为零值），JSON key 不省略——下游 Doris Routine Load 按 key 匹配，零值语义由表列默认值承接。

> **演进注记**：字段抽取/转换逻辑计划独立为公共包 `reader_modules/mod_fields/`，供 mod_kafka 与 mod_log_mysql 共用（mod_kafka 行为不变，见 `doc/modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md`）。

### 4.2 日志类型过滤

仅处理 `BfeLogType_Request`；会话日志（SessionLog，TLS/RTT 等）丢弃并计数。下游报表以请求为粒度。

### 4.3 KafkaProducer

`kafka_producer.go`，客户端库 `segmentio/kafka-go`（非 sarama）：

- **队列**：`msgCh` 容量 `BatchSize*2`；`Send()` 非阻塞（select + default），**队列满直接丢弃该条**并计数 `SENT_KAFKA_CHN_FULL`——背压策略是有损的，保障读取主流程永不被下游拖住；
- **攒批**：单 sendLoop goroutine，`BatchSize` 条或 `LingerMs` 定时器到期即 `WriteMessages`（10s ctx 超时）；kafka-go writer 内部还有一层批量缓冲；
- **分区**：`Balancer = Hash{}` 但消息未设 Key，Hash 对空 key 退化为轮询——随机分布到分区，符合 Doris Routine Load 并发消费模型；
- **失败处理**：`WriteMessages` 内部按 `MaxAttempts = MaxRetries` 重试；仍失败计数 `SEND_KAFKA_FAILED`，配置了 `DeadLetterTopic` 则整批写死信（10s 超时），未配置则该批丢失；
- **压缩**：可配 none/snappy/gzip/lz4/zstd（生产 zstd）；
- **关闭**：`Close()` cancel ctx → drain msgCh 残留 flush 一次 → writer.Close()。

### 4.4 配置

`conf/mod_kafka/mod_kafka.conf`（逐项说明见 [mod_kafka.conf.md](../configuration/mod_kafka/mod_kafka.conf.md)）：

| 节 | 配置项 | 默认/样例 | 说明 |
|----|--------|-----------|------|
| `[Basic]` | `DataPath` | 空 | 字段配置文件（kafka_config.data）路径，空用内置默认 |
| `[Basic]` | `OpenDebug` | false | 打印每条转换后 JSON |
| `[kafka]` | `Brokers` | 必填 | 逗号分隔 |
| `[kafka]` | `Topic` | 必填 | 目标 topic（生产 `bfe_ai_log`） |
| `[kafka]` | `DeadLetterTopic` | 空=不启用 | 死信 topic（`bfe_ai_log_dlq`） |
| `[kafka]` | `Compression` | none | 压缩算法 |
| `[kafka]` | `BatchSize` | 1000 | 应用层攒批条数 |
| `[kafka]` | `LingerMs` | 100 | 攒批等待毫秒 |
| `[kafka]` | `MaxRetries` | 3 | kafka-go MaxAttempts |

`conf/mod_kafka/kafka_config.data`：`[ConfFields] FieldMode` + 多行 `FieldNames`（customized 模式生效，未知字段告警忽略）；生产用 customized 显式列出约 61 个字段（全量基础 + 全部 AI 字段）。

### 4.5 监控指标

`mod_kafka(_diff)` 计数：

| 指标 | 含义 | 异常信号 |
|------|------|----------|
| `RECEIVED_LOGS` | Update 收到条数 | 与 `bfe_reader.SUM_PB_RECORD` 对不上 |
| `RECEIVED_REQ` | request 类型条数 | — |
| `SENT_TO_KAFKA` | 成功入队条数 | 停滞 = 管道异常 |
| `CONVERT_FAILED` | JSON 转换失败条数 | 持续增长需排查 |
| `SEND_KAFKA_FAILED` | 发送失败批数 | >0 需告警 |
| `DLQ_SENT` / `DLQ_SENT_FAILED` | 死信写入成功/失败 | DLQ 也失败 = 消息丢失 |
| `SENT_KAFKA_CHN_FULL` | 队列满丢弃条数 | 持续增长说明 Kafka 消费跟不上 |

## 5. 降级与兼容性

| 场景 | 行为 |
|------|------|
| Kafka 不可用 | 内部重试耗尽 → DLQ（若配置）；期间读取主流程不受影响，丢弃可计数 |
| 下游表结构演进（Doris 加列） | 本模块零感知——JSON 按 key 匹配，多出的 key 被忽略；新字段输出需在本模块注册 + 配置 FieldNames |
| pb 协议演进（bfe-access-pb 升级） | 升级依赖版本 + 注册新字段，二进制兼容由 proto 字段编号保证 |
| 未配置 DLQ | 重试耗尽消息丢失（仅计数），生产建议必配 |
| 老配置（无新字段） | FieldMode=default 自动跟随 Default 字段集变化 |

## 6. 关键文件索引

| 文件 | 内容 |
|------|------|
| `reader_modules/mod_kafka/mod_kafka.go` | ModuleKafka：接口实现、请求过滤、监控注册 |
| `reader_modules/mod_kafka/field_registry.go` | 字段注册表与抽取函数（82 字段） |
| `reader_modules/mod_kafka/json_converter.go` | 字段 map → JSON |
| `reader_modules/mod_kafka/kafka_data_config.go` | FieldMode/FieldNames 解析 |
| `reader_modules/mod_kafka/kafka_producer.go` | 队列、攒批、发送、DLQ、关闭 |
| `reader_modules/mod_kafka/conf_mod_kafka.go` | INI 配置解析与校验 |
| `conf/mod_kafka/` | 生产配置样例 |

关联文档：[output-fields.md](../modules/mod_kafka/output-fields.md)（字段全集）、[kafka_config.data.md](../configuration/mod_kafka/kafka_config.data.md)、[mod_log_mysql.md](./mod_log_mysql.md)（姊妹模块）。
