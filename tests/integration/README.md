# log-reader 集成测试

本目录承载 `log-reader` 的真实进程级集成测试。

与单元测试不同的是：

- 启动真实的 `log-reader` 进程；
- 不依赖真实的 BFE，而是通过测试代码直接生成 protobuf 访问日志；
- 不依赖真实的 Kafka，而是通过 `MockKafka` 模拟 Kafka Broker；
- 验证 log-reader 是否能正确读取日志、解析字段并输出到 Kafka。

## 目录结构

```text
log-reader/tests/integration/
├── README.md                                  # 本文档
├── common/                                    # 公共 harness
│   ├── mock_kafka.go                          # 模拟 Kafka Broker
│   ├── log_generator.go                       # 生成 b2log 格式日志
│   ├── process_env.go                         # 编译/启动/停止 log-reader 进程
│   ├── config_builder.go                      # 生成临时 log-reader 配置
│   └── util.go                                # 工具函数
├── implementation/                            # Go 实现代码
│   └── scenario-LR01-basic-flow/
│       ├── lr01_basic_flow_test.go
│       └── testdata/                          # 静态配置模板
└── 测试设计文档/                               # 中文测试设计文档（可选）
```

## 运行方式

在 `log-reader/` 目录下执行：

```bash
# 运行全部集成测试
go test ./tests/integration/... -v

# 运行单个场景
go test ./tests/integration/implementation/scenario-LR01-basic-flow/... -v

# 运行单个测试例
go test ./tests/integration/implementation/scenario-LR01-basic-flow/ -run TestLR01_BasicFlow -v
```

首次运行会自动编译 `log-reader` 二进制并缓存到 `log-reader/tests/integration/.integration-test-bin/`。

## 当前覆盖

| 场景 | 说明 |
|------|------|
| LR01 基本流程 | 验证 log-reader 读取 protobuf 日志、按 `conf/mod_kafka/kafka_config.data` 中开启的 60 个字段输出 JSON 到 Kafka，并逐字段（含 AI 对象数组与字符串数组）验证 JSON 内容与输入 protobuf 一致 |
| LR01 批次拆分 | 验证日志数量超过 `MaxSizePerBatch` 时，模块能正确拆分批次处理，且不丢失/不错误转换字段内容 |
| LR01 Customized 字段模式 | 验证 `customized` 模式自动包含必需字段 |
| LR01 Default 字段模式 | 验证 `default` 模式输出默认字段集 |
| LR01 All 字段模式 | 验证 `all` 模式输出所有注册字段 |
| LR01 Require 字段模式 | 验证 `require` 模式仅输出 22 个必需字段 |
| LR01 JSON 结构稳定性 | 验证同一条消息多次解析结果一致 |

## Mock 说明

### Mock 日志生成

`common.LogGenerator` 使用 `b2log.HeaderWrite` + `proto.Marshal` 将 `bfe_access_pb.BfeLog` 写入文件，格式与 BFE `mod_access_pb3` 输出的日志一致。

### Mock Kafka

`common.MockKafka` 是一个轻量级 TCP 服务器，使用 `segmentio/kafka-go/protocol` 处理以下 Kafka 请求：

- `ApiVersions`
- `Metadata`
- `Produce`

收到的 `Produce` 消息会被解析并保存，测试代码通过 `WaitForMessages` 等待并校验消息内容。

## 参考文档

- `测试设计文档/测试场景总体说明.md`
- `测试设计文档/scenario-LR01-basic-flow/场景说明.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-01-基本流程.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-02-批次拆分.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-03-Customized字段模式包含必需字段.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-04-Default字段模式.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-05-All字段模式.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-06-Require字段模式.md`
- `测试设计文档/scenario-LR01-basic-flow/TC-07-JSON结构稳定性.md`
- `../doc/configuration/config.md`
- `../doc/configuration/mod_kafka/mod_kafka.conf.md`
- `../doc/configuration/mod_kafka/kafka_config.data.md`
- `../../bfe/tests/integration/README.md`

