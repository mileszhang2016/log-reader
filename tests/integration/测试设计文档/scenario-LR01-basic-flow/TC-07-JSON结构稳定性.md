# TC-07 JSON 结构稳定性

## 用例编号与名称

TC-07 JSON 结构稳定性

## 所属场景

LR01 基本流程与 JSON 转换正确性

## 版本声明

- `log-reader`：当前源码版本

## 测试目的

验证同一条 Kafka 消息多次解析后，JSON 结构保持一致。

## 运行模式

单组件模式：仅启动真实 `log-reader` 进程，Mock Kafka 与日志生成均在测试代码中完成。

## 前置条件

1. 已编译 `log-reader` 可执行文件。
2. `MockKafka` 已启动并监听 `127.0.0.1` 上的随机端口。
3. 临时 `log-reader` 配置已生成并加载。
4. pb 日志文件已创建并处于空状态。

## 配置构造

- `kafka_config.data` 中 `FieldMode = customized`，配置少量字段：`logid`、`timestamp`、`product`、`ai_requested_model`。

## 输入数据

写入 1 条 `BfeLog` 请求日志：

| 字段 | 值 |
|------|-----|
| logid | 70001 |
| header_host | stable.example.org |
| origin_uri | /v1/chat |
| ai_requested_model | stable-model |

## 操作步骤

1. 启动 `MockKafka`。
2. 生成临时配置目录与日志目录。
3. 启动 `log-reader` 进程。
4. 通过 `LogGenerator` 写入 1 条 b2log 记录。
5. 等待最多 10 秒，直到 `MockKafka` 收到 1 条消息。
6. 对同一条消息进行两次 `json.Unmarshal`，比较两次解析结果。

## 预期结果

- `MockKafka` 收到 1 条消息。
- 同一条消息两次解析后的 map 表示完全一致。

## 清理

停止 `log-reader` 进程，关闭 `MockKafka`，删除临时目录。
