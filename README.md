# log-reader

log-reader 是 BFE（Beyond Front End）的 **protobuf 格式访问日志读取与分发服务**。它实时 tailing BFE 生成的二进制访问日志（`pb_access3.log`），将其解析为结构化日志记录，并按可扩展的模块化流水线分发到下游系统（目前内置 Kafka 输出模块）。

仓库同时以**第二个构建产物**形式提供命令行工具 `bfe-pblog-tool`，用于离线查看 PB 访问日志（`cat` / `tail`），方便日常排查与验证。

> BFE 是一个开源的七层负载均衡系统，详见 [bfenetworks/bfe](https://github.com/bfenetworks/bfe)。

## 功能特性

- **高性能日志读取**：基于 `bfe-access-pb` / `b2log` 二进制格式解析，支持日志轮转（log rotate）检测与自动重新定位，`-b` 可从文件头开始读取。
- **批处理分发**：读取的日志按可配置大小（`MaxSizePerBatch`）切分批次，并行投递给各绑定模块。
- **可扩展模块框架**：通过 `ReaderModule` 接口（`Init / Start / Update / Close`）即可接入新的下游模块。
- **内置 Kafka 输出模块 `mod_kafka`**：
  - 将解析结果按**可定制字段集合**转换为 JSON 后批量发送；
  - 支持压缩（none/snappy/gzip/lz4/zstd）、批量缓冲、Linger 聚合、失败重试；
  - 发送失败自动写入**死信队列（DLQ）**，并内置完整的监控计数指标；
  - 覆盖 BFE 全量字段，含 **AI Gateway 可观测字段**（`ai_provider`、`ai_input_tokens`、`ai_ttft_us` 等）。
- **内置 Web 监控**：提供 HTTP 状态接口，实时查看进程与模块运行指标。
- **运维 CLI `bfe-pblog-tool`**：`cat` 全量查看、`tail` 查看末尾 N 条并支持 `-f` 持续跟随，跨平台（Windows/Linux/macOS）。
- **完整测试体系**：单元测试 + 基于 mock Kafka broker 的端到端集成测试。

## 目录结构

```
log-reader/
├── main/                    # daemon 进程入口（log_reader）
├── cmd/bfe-pblog-tool/      # CLI 工具入口（cat / tail），与 daemon 并列的第二个构建产物
│   ├── cat/                 # cat 子命令 + PblogCat 实现
│   ├── tail/                # tail 子命令 + PblogTail 实现（含平台适配）
│   └── common/              # 共享 CLI 基础设施（RegistCmd / Cmds）
├── bfe_log_reader/          # 核心：PB 日志读取/解析 + 模块编排
├── reader_conf/             # 配置加载（config.conf、模块配置）
├── reader_module/           # 模块框架（ReaderModule 接口与注册表）
├── reader_modules/
│   └── mod_kafka/           # 内置 Kafka 输出模块
├── reader_util/             # 工具函数（hostid、信号处理）
├── conf/                    # 默认配置文件
│   ├── config.conf          # 核心配置
│   └── mod_kafka/           # mod_kafka 模块配置与字段配置
├── doc/                     # 设计/配置/字段/变更文档（中文）
├── tests/                   # 集成测试（含 mock Kafka broker 的 E2E 场景）
├── Makefile                 # 构建 / 测试 / release 目标
└── go.mod                   # Go module: github.com/rainway-ai-gateway/log-reader
```

## 环境要求

- Go **1.22+**
- 目标日志源：BFE 开启 PB 访问日志后产出的 `pb_access3.log`（[bfe-access-pb](https://github.com/bfenetworks/bfe-access-pb) 二进制格式）
- 下游：Kafka 集群（使用 `mod_kafka` 时）

## 快速开始

### 1. 获取代码并编译

```sh
git clone https://github.com/rainway-ai-gateway/log-reader.git
cd log-reader

# 编译 daemon（等价于: prepare → test → build → package）
make
```

编译产物输出到：

- daemon：`output/bin/log_reader`
- 默认配置：`output/conf/`

### 2. 运行 daemon

```sh
./log_reader -h
#   -a    自动获取 BFE 集群名
#   -b    从日志文件开头读取
#   -c    string 配置文件根目录（默认 "../conf"）
#   -d    输出 Debug 日志（默认 >= info）
#   -l    string 日志目录（默认 "../log"）
#   -s    日志同时输出到 stdout

# 以指定配置目录启动
./log_reader -c ../conf/ -l ../log -s
```

### 3. 命令行查看 PB 日志

```sh
# 编译 CLI 工具
make bfe-pblog-tool          # 产物: output/bin/bfe-pblog-tool

# 全量输出日志内容（-n 显示行号）
./output/bin/bfe-pblog-tool cat /path/to/pb_access3.log

# 查看末尾 10 条（默认）
./output/bin/bfe-pblog-tool tail /path/to/pb_access3.log

# 查看末尾 3 条
./output/bin/bfe-pblog-tool tail -n 3 /path/to/pb_access3.log

# 持续跟随新增日志（轮询间隔默认 500ms，可 --interval 调整）
./output/bin/bfe-pblog-tool tail -n 20 -f --interval 200 /path/to/pb_access3.log
```

> `bfe-pblog-tool` 原为 `bfe-access-pb` 仓库下的独立工具，现已迁移至本仓库统一维护，CLI 实现位于 `cmd/bfe-pblog-tool/cat/` 和 `cmd/bfe-pblog-tool/tail/`，复用 `bfe_log_reader` 同一套解析内核。

## 配置说明

配置基于 **INI 格式**，由 `config.conf` 与各模块目录下的模块配置组成。当前版本修改配置后需重启进程生效（暂不支持热加载）。详细说明见 [doc/configuration/config.md](doc/configuration/config.md)。

### 核心配置 `conf/config.conf`

```ini
[main]
# HTTP 监控端口（默认 8992）
httpPort=8992
# 监听地址（默认空 = 全地址监听）
# httpAddr = 127.0.0.1

# 最大使用 CPU 核数（必填）
maxCpus = 6

# 监控统计周期（秒），须能被 60 整除
monitorInterval = 60

[PbAccessLogConf]
# PB 访问日志文件路径（不配置则不读取日志）
LogFile = /home/work/bfe/log/pb_access3.log
# 启用的模块列表（当前支持 mod_kafka，可多行声明多个模块）
Modules=mod_kafka
# 每批处理的最大日志条数（默认 -1 = 不限制）
MaxSizePerBatch = 128
```

| 配置段 | 配置项 | 说明 |
| ------ | ------ | ---- |
| `[main]` | `HttpPort` / `HttpAddr` | 监控 HTTP 服务端口与监听地址 |
| `[main]` | `MaxCpus` | 最大使用 CPU 核数（必填） |
| `[main]` | `MonitorInterval` | 监控指标统计周期（秒），范围 [20,60] 且能整除 60 |
| `[PbAccessLogConf]` | `LogFile` | PB 访问日志路径；与 `Modules` 需同时配置 |
| `[PbAccessLogConf]` | `Modules` | 启用的下游模块列表 |
| `[PbAccessLogConf]` | `MaxSizePerBatch` | 每批最大日志条数 |

### 模块配置

每个模块的配置文件位于 `conf/<模块名>/<模块名>.conf`（`mod_kafka` 对应 `conf/mod_kafka/mod_kafka.conf`），详见 [mod_kafka.conf 说明](doc/configuration/mod_kafka/mod_kafka.conf.md)。

### Kafka 字段输出配置 `kafka_config.data`

通过 `FieldMode` 控制输出到 Kafka 的 JSON 字段集合：

| 模式 | 说明 |
| ---- | ---- |
| `require` | 仅输出必需字段 |
| `default` | 输出默认字段集（向后兼容） |
| `all` | 输出全部可用字段（60+） |
| `customized` | 输出 `FieldNames` 指定字段 ∪ 必需字段 |

> 无论何种模式，**必需字段始终输出**。全部字段的类型/含义说明见 [output-fields.md](doc/modules/mod_kafka/output-fields.md)。

## 模块开发

在 `reader_modules/` 下新建目录并实现 `ReaderModule` 接口：

```go
type ReaderModule interface {
    Name() string                                   // 模块名（注册标识）
    Init(conf *reader_conf.ReaderConfig, whs *web_monitor.WebHandlers, cr string) error
    Start()                                          // 启动后台协程
    Update([]*bfe_access_pb.BfeLog)                  // 消费解析后的日志批次
    Close() error                                    // 退出清理
}
```

- 注册：模块通过 `reader_module.AddModule(...)` 加入模块注册表，由 `[PbAccessLogConf] Modules` 决定是否启用。
- 监控：模块可复用 `module_state2.State` 注册自有计数指标，并注册 Web handler。

## 监控指标

进程启动后可通过 HTTP 端口（默认 `8992`）查看状态。已注册的监控项包括：

| 监控项 | 来源 | 说明 |
| ------ | ---- | ---- |
| `bfe_reader` / `bfe_reader_diff` | daemon | 进程整体状态与增量（读取次数、PB 解码错误、批次等） |
| `mod_kafka` / `mod_kafka_diff` | mod_kafka | Kafka 模块计数与增量 |

`mod_kafka` 关键计数：`RECEIVED_LOGS`、`RECEIVED_REQ`、`SENT_TO_KAFKA`、`CONVERT_FAILED`、`SEND_KAFKA_FAILED`、`DLQ_SENT`、`DLQ_SENT_FAILED`、`SENT_KAFKA_CHN_FULL`（发送缓冲满丢弃）。

> 请求地址格式由 `go-lib/web-monitor` 定义；排查时请将 `httpPort` 与监控系统采集参数对齐。

## 测试

```sh
# 单元测试 + go vet（64 位平台自动启用 -race）
make test

# 单独运行
go test ./...
```

仓库包含三层测试资产：

| 层级 | 位置 | 内容 |
| ---- | ---- | ---- |
| 单元测试 | 各包 `*_test.go` | 解析、配置加载、字段筛选、JSON 转换、批处理等 |
| 集成测试 | `tests/integration/` | 基于 mock Kafka broker 的端到端场景（LR01 basic flow），含字段模式/JSON 结构稳定性用例 |
| 手工验证 | `output/bin/bfe-pblog-tool` | 用真实测试数据 `cat` / `tail` 校验 |

## 相关文档

- [配置总览](doc/configuration/config.md) · [核心配置详解](doc/configuration/config.conf.md) · [公共类型定义](doc/configuration/00-common.md)
- [mod_kafka 模块说明](doc/modules/mod_kafka/mod_kafka.md) · [输出字段全集](doc/modules/mod_kafka/output-fields.md)
- [AI Gateway 可观测性链路打通指南](doc/howto/01AI%20Gateway%20可观测性链路打通指南.md)
- [变更记录](CHANGELOG.md)（[Keep a Changelog](https://keepachangelog.com/zh-CN/) / [SemVer](https://semver.org/lang/zh-CN/)）

## 发布

```sh
make release
```

将按 `darwin/arm64`、`linux/amd64`、`linux/arm64`、`windows/amd64` 交叉编译，打包 `log-reader` 与 `bfe-pblog-tool` 两个二进制，产出：

```
dist/log-reader_<VERSION>_<os>_<arch>.tar.gz
```

## 参与贡献

欢迎提交 Issue 与 Pull Request。建议：

1. Fork 本仓库并创建特性分支；
2. 涉及行为变更时**先补充/更新测试**（单元与集成）；
3. 通过 `make test` 后提交 PR；
4. 若涉及输出字段或 AI 可观测字段，同步更新 `doc/` 下对应文档。

## License

log-reader 基于 [Apache License 2.0](LICENSE) 开源。
