# mod_kafka 基础配置

## 配置简介

`mod_kafka.conf` 是 `mod_kafka` 模块的基础配置文件，用于配置 Kafka 连接参数、发送参数以及关联的数据配置文件路径。

## 配置描述

### 基础配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| Basic.DataPath | String | Kafka 字段选择数据配置文件路径 | N | 相对于 `mod_kafka.conf` 所在目录解析；不配置时使用默认字段集；参见 [FilePath](../00-common.md#2-文件路径filepath) 类型定义 | 类型为 [FilePath](../00-common.md#2-文件路径filepath) |
| Basic.OpenDebug | Boolean | 是否开启 Debug 模式 | N | 默认值 `false`；开启后会在日志中输出每条转换后的 JSON 数据 | - |

### Kafka 连接与发送配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| Kafka.Brokers | String | Kafka Broker 地址列表 | Y | 多个地址使用逗号 `,` 分隔，例如 `kafka1:9092,kafka2:9092` | 非空 |
| Kafka.Topic | String | 目标 Topic | Y | 正常发送的 Kafka Topic | 非空 |
| Kafka.DeadLetterTopic | String | 死信 Topic | N | 发送失败的消息会写入该 Topic；不配置时不启用死信队列 | - |
| Kafka.Compression | String | 消息压缩方式 | N | 默认值 `none`；参见 [KafkaCompression](../00-common.md#4-kafka-压缩算法kafkacompression) 类型定义 | 取值须为 `none`、`snappy`、`gzip`、`lz4`、`zstd` 之一 |
| Kafka.BatchSize | Integer | 批量发送大小 | N | 默认值 1000 | > 0 |
| Kafka.LingerMs | Integer | 批量发送等待时间，单位为毫秒 | N | 默认值 100 | > 0 |
| Kafka.MaxRetries | Integer | 最大重试次数 | N | 默认值 3 | > 0 |

## 配置示例

```ini
[Basic]
# path to data config file, relative to this config file's directory
DataPath = kafka_config.data
OpenDebug = false

[kafka]
# Brokers = kafka1:9092,kafka2:9092
Brokers = 172.18.1.244:9092

# target topic
Topic = bfe_ai_log

# dead letter topic (failed messages are written here)
DeadLetterTopic = bfe_ai_log_dlq

# compression: none / snappy / gzip / lz4 / zstd
Compression = zstd

# batch size
BatchSize = 100

# batch linger time in milliseconds
LingerMs = 100

# max retries
MaxRetries = 3
```

## 监控指标

`mod_kafka` 模块注册以下监控指标：

| 指标名 | 含义 |
| ------ | ---- |
| RECEIVED_LOGS | 接收到的日志总条数 |
| RECEIVED_REQ | 接收到的请求类型日志条数 |
| SENT_TO_KAFKA | 成功发送到 Kafka 的消息数 |
| CONVERT_FAILED | JSON 转换失败的日志数 |
| SEND_KAFKA_FAILED | 发送 Kafka 失败的消息数 |
| DLQ_SENT | 成功写入死信队列的消息数 |
| DLQ_SENT_FAILED | 写入死信队列失败的消息数 |
| SENT_KAFKA_CHN_FULL | Kafka 发送通道已满的次数 |
