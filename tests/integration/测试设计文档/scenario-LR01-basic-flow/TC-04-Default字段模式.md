# TC-04 Default 字段模式

## 用例编号与名称

TC-04 Default 字段模式

## 所属场景

LR01 基本流程与 JSON 转换正确性

## 版本声明

- `log-reader`：当前源码版本

## 测试目的

验证 `FieldMode = default` 时，log-reader 输出默认字段集，且字段值正确。

## 运行模式

单组件模式：仅启动真实 `log-reader` 进程，Mock Kafka 与日志生成均在测试代码中完成。

## 前置条件

1. 已编译 `log-reader` 可执行文件。
2. `MockKafka` 已启动并监听 `127.0.0.1` 上的随机端口。
3. 临时 `log-reader` 配置已生成并加载。
4. pb 日志文件已创建并处于空状态。

## 配置构造

- `kafka_config.data` 中 `FieldMode = default`，不配置 `FieldNames`。

## 输入数据

写入 1 条 `BfeLog` 请求日志：

| 字段 | 值 |
|------|-----|
| logid | 40001 |
| header_host | default.example.org |
| origin_uri | /v1/chat |
| ai_requested_model | default-model |

## 操作步骤

1. 启动 `MockKafka`。
2. 生成临时配置目录与日志目录。
3. 启动 `log-reader` 进程。
4. 通过 `LogGenerator` 写入 1 条 b2log 记录。
5. 等待最多 10 秒，直到 `MockKafka` 收到 1 条消息。
6. 解析 JSON，验证默认字段集被完整输出。

## 预期结果

- `MockKafka` 收到 1 条消息。
- 消息包含默认字段集中的常用字段，例如：
  - 必需字段：`logid`、`timestamp`、`product`、`hostid`、`client_ip`、`err_code` 等；
  - 默认字段：`header_host`、`origin_uri`、`method`、`res_status_code`、`ai_requested_model`、`ai_target_model`、`ai_apikey_id`、`ai_input_tokens`、`ai_provider` 等。
- `logid = 40001`，`header_host = "default.example.org"`，`origin_uri = "/v1/chat"`，`ai_requested_model = "default-model"`。

## 清理

停止 `log-reader` 进程，关闭 `MockKafka`，删除临时目录。
