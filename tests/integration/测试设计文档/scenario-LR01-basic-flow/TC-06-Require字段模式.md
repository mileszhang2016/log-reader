# TC-06 Require 字段模式

## 用例编号与名称

TC-06 Require 字段模式

## 所属场景

LR01 基本流程与 JSON 转换正确性

## 版本声明

- `log-reader`：当前源码版本

## 测试目的

验证 `FieldMode = require` 时，log-reader 仅输出 22 个必需字段，非必需字段不输出。

## 运行模式

单组件模式：仅启动真实 `log-reader` 进程，Mock Kafka 与日志生成均在测试代码中完成。

## 前置条件

1. 已编译 `log-reader` 可执行文件。
2. `MockKafka` 已启动并监听 `127.0.0.1` 上的随机端口。
3. 临时 `log-reader` 配置已生成并加载。
4. pb 日志文件已创建并处于空状态。

## 配置构造

- `kafka_config.data` 中 `FieldMode = require`，不配置 `FieldNames`。

## 输入数据

写入 1 条 `BfeLog` 请求日志：

| 字段 | 值 |
|------|-----|
| logid | 60001 |
| header_host | require.example.org |
| origin_uri | /v1/chat |
| ai_requested_model | require-model |

## 操作步骤

1. 启动 `MockKafka`。
2. 生成临时配置目录与日志目录。
3. 启动 `log-reader` 进程。
4. 通过 `LogGenerator` 写入 1 条 b2log 记录。
5. 等待最多 10 秒，直到 `MockKafka` 收到 1 条消息。
6. 解析 JSON，验证仅包含必需字段。

## 预期结果

- `MockKafka` 收到 1 条消息。
- 消息包含全部 22 个必需字段：
  `logid`、`timestamp`、`product`、`hostid`、`client_ip`、`err_code`、`err_msg`、`req_header_len`、`req_body_len`、`proto`、`header_host`、`origin_uri`、`method`、`res_status_code`、`res_header_len`、`res_body_len`、`all_time`、`read_client_time`、`cluster_serve_time`、`backend_serve_time`、`write_client_time`、`proxy_delay_time`。
- 非必需字段不存在，例如：`ai_requested_model`、`ai_mapped_model`、`req_num`、`session_id`、`user_agent`、`cookie`、`res_location`、`log_tag`、`bfe_ip`、`vip`。

## 清理

停止 `log-reader` 进程，关闭 `MockKafka`，删除临时目录。
