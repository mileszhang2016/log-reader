# 公共类型

以下公共类型在多个配置项中复用，具体配置项的合法性条件中通过「类型名」引用即可。

## 1. 网络端口（Port）

- 类型为整数（Integer）。
- 取值范围为 0-65535（IANA 有效端口范围，RFC 793）。
- 取值为 0 时由操作系统自动分配监听端口。

## 2. 文件路径（FilePath）

- 类型为字符串（String）。
- 支持相对路径（相对于模块配置文件所在目录解析）或绝对路径（以 `/` 开头，Windows 下以盘符开头）。
- 指向的文件须在运行加载时存在且可读。

## 3. 模块启用标识（ModuleEnable）

- 类型为字符串（String）。
- 格式：`mod_name` 或 `mod_name:true` 或 `mod_name:false`。
- 仅当 `mod_name` 存在且未显式指定 `false` 时，模块才会被启用。

## 4. Kafka 压缩算法（KafkaCompression）

- 类型为字符串（String）。
- 可选值：`none`、`snappy`、`gzip`、`lz4`、`zstd`。
- 空字符串等效于 `none`。
