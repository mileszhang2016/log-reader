# 核心配置

## 配置简介

`config.conf` 是 log-reader 的核心配置文件，用于配置服务基础参数以及 PB 访问日志读取参数。

## 配置描述

### 服务基础配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| Main.HttpPort | Integer | HTTP 监听端口，用于监控和配置重载 | N | 默认值 8992；参见 [Port](00-common.md#1-网络端口port) 类型定义 | 类型为 [Port](00-common.md#1-网络端口port)，取值范围 [0, 65535] |
| Main.HttpAddr | String | HTTP 监听地址 | N | 默认值为空，表示监听所有地址；建议测试环境设置为 `127.0.0.1` | 参见 [ListenAddr](00-common.md#2-监听地址listenaddr) 类型定义 |
| Main.MaxCpus | Integer | 最大使用 CPU 核数 | Y | 无默认值 | > 0 |
| Main.MonitorInterval | Integer | Monitor 数据统计周期，单位为秒 | N | 默认值 20；必须能整除 60；大于 60 时会被截断为 60 | [20, 60] 且能整除 60 |

### PB 访问日志配置

| 配置项 | 类型 | 参数含义 | 必填 | 补充描述 | 合法性条件 |
| ------ | ---- | -------- | ---- | -------- | ---------- |
| PbAccessLogConf.LogFile | String | PB 访问日志文件路径 | N | 不配置时不读取日志；配置时必须同时配置 Modules；参见 [FilePath](00-common.md#2-文件路径filepath) 类型定义 | - |
| PbAccessLogConf.Modules | String[] | 启用的模块列表 | N | 不配置时不加载任何模块；配置时必须同时配置 LogFile；多个模块请增加多行 Modules 配置；格式参见 [ModuleEnable](00-common.md#3-模块启用标识moduleenable) | 必须是 log-reader 支持的模块，当前仅支持 `mod_kafka` |
| PbAccessLogConf.MaxSizePerBatch | Integer | 每批处理日志的最大元素数量 | N | 默认值 -1，表示不限制 | -1 或 > 0 |

## 配置示例

```ini
[main]
# http port for monitor and reload
httpPort=8992

# max number of CPUs to use
maxCpus = 6

# interval for get diff of proxy-state
# NOTE: this value MUST match the monitor parameters
#       of noah to generate alert correctly
monitorInterval = 60

# for bfe_access_pb
[PbAccessLogConf]
LogFile = ./../../bfe/log/pb_access3.log
Modules=mod_kafka
MaxSizePerBatch = 128
```

## 启动参数

log-reader 启动时可通过命令行参数指定配置根目录、日志目录等。

```sh
./log_reader -c ../conf/
```

常用参数说明：

| 参数 | 类型 | 默认值 | 含义 |
| ---- | ---- | ------ | ---- |
| -c string | String | `../conf` | 配置文件根目录 |
| -l string | String | `../log` | 日志文件目录 |
| -s | Bool | false | 是否在标准输出打印日志 |
| -d | Bool | false | 是否开启 Debug 日志 |
| -b | Bool | false | 是否从日志文件开头读取 |
| -a | Bool | false | 是否自动获取 BFE 集群名 |
| -h | Bool | false | 显示帮助信息 |
