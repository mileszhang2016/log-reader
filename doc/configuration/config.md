# 配置概述

## 说明

本文档是关于对 log-reader 进行配置的概述。

log-reader 用于读取 BFE 生成的 protobuf 格式访问日志，并将解析后的日志分发到下游模块（如 Kafka）。

## log-reader 配置分类

- 常规配置：在运行期间修改，需重启生效。
- 动态配置：在运行期间修改，热加载生效。

## log-reader 配置格式

- 常规配置：基于 INI 格式。
- 动态配置：目前各模块数据配置文件也采用 INI 格式。

## log-reader 配置组织

log-reader 的核心配置是 `config.conf`（`conf/config.conf`），为便于维护，配置按功能分类存放在相应目录 `conf/<feature>/`。

| 功能类别     | 文件位置 |
| ------------ | -------- |
| 服务基础配置 | `conf/config.conf` |
| PB 访问日志配置 | `conf/config.conf` 中的 `[PbAccessLogConf]` 段 |
| 扩展模块配置 | `conf/mod_<name>/` 目录 |

## log-reader 配置热加载

log-reader 当前版本暂不支持配置热加载。修改配置后，需要重启进程生效。
