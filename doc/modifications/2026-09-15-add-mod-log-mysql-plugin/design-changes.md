# log-reader 新增 mod_log_mysql 插件（AI 访问日志写入 MySQL）

## 背景

当前 log-reader 只有一个输出模块 `mod_kafka`：将 BFE pb 访问日志转为 JSON 写入 Kafka，下游经 Doris Routine Load 入库、Grafana 展示。整条链路需要 Kafka、Doris、Grafana 三个外部组件，对小规模/私有化部署过重。

为支持小规模/私有化部署，新增「轻量形态」报表链路：访问日志由 log-reader 新插件 `mod_log_mysql` **直接批量写入 MySQL**，报表查询由 ai-gateway-api 提供 API、ai-gateway-web 页面展示，全程无需 Kafka/Doris/Grafana。

插件职责定位：**把 AI 访问日志推送到 MySQL**（命名取 `mod_log_mysql` 而非 `mod_mysql`，与 mod_kafka 同构——按目的地命名、log 前缀限定用途域）。插件直写数据库、不经 ai-gateway-api 中转，主要理由：① 数据面/控制面解耦——log-reader 随 BFE 部署在数据面，日志链路可用性不能依赖控制面（ai-gateway-api）的发布与负载；② 吞吐与单点——访问日志是数据面最高吞吐的数据流，经 api 中转会使其成为汇聚瓶颈；③ 与既有惯例一致——mod_kafka 直写 Kafka 不经任何服务。

## 修改目标

1. 将 mod_kafka 的「pb → 字段抽取 → 类型转换」逻辑抽取为公共包 `reader_modules/mod_fields/`，mod_kafka 行为不变（JSON 输出字节级兼容）。
2. 新增输出模块 `mod_log_mysql`：实现 `ReaderModule` 接口，批量幂等写入 MySQL 明细表 `bfe_ai_request_log`。
3. 新增插件配置样例 `conf/mod_log_mysql/mod_log_mysql.conf`。
4. 新增 web-monitor 监控 handler `mod_log_mysql`/`mod_log_mysql_diff`。
5. 补充单元测试（sqlmock）与集成测试场景 `scenario-LR03-mysql-write`。
6. 更新文档（README、doc/modules、doc/configuration）与 CHANGELOG；版本号建议 1.4.0。

## 总体设计

### 模块结构

```
reader_modules/
├── mod_fields/                    # 【新增】公共字段抽取包
│   ├── field_registry.go          # 从 mod_kafka 上移（注册表 + Extract + 字段集）
│   ├── extractors.go              # 从 mod_kafka 上移（各字段 extract 函数）
│   └── field_registry_test.go
├── mod_kafka/                     # 【修改】改用 mod_fields，JSON 输出不变
│   ├── mod_kafka.go
│   ├── conf_mod_kafka.go
│   ├── kafka_data_config.go       # FieldMode/FieldNames 配置仍属本模块
│   ├── json_converter.go          # 字段 map → JSON（逻辑不变，数据源换 mod_fields）
│   ├── kafka_producer.go
│   └── ...
└── mod_log_mysql/                 # 【新增】本方案主体
    ├── mod_log_mysql.go           # ModuleLogMysql：实现 ReaderModule 接口
    ├── conf_mod_log_mysql.go      # ConfModLogMysql 配置解析（gcfg/INI）
    ├── field_mapper.go            # 抽取值 → 表列行（固定列序）
    ├── record_writer.go           # 批量写入器：缓冲队列 + 单写协程 + 事务批插 + 重试
    └── conf/mod_log_mysql.conf    # 配置样例（DDL 归 ai-gateway-api 仓库持有，见 §5）
```

### 数据流

```
Update(batch []*BfeLog)
  → 仅处理 BfeLogType_Request（会话日志丢弃，与 mod_kafka 一致）
  → mod_fields.Extract 按注册表抽取 → field_mapper 组装行（固定列序）
    （字符串零值 → NULL；数值/布尔原样写入；JSON 列由结构化值 marshal）
  → 写入 recordCh（容量 QueueSize，满则丢弃并计数 SENT_MYSQL_CHN_FULL）
写协程（单 goroutine）：
  攒批（BatchSize 条 或 FlushIntervalMs 到期，先到先发）
  → 单事务多值 INSERT ... ON DUPLICATE KEY UPDATE（幂等覆盖，键冲突即覆盖）
  → 失败重试 MaxRetries 次（指数退避：200ms/400ms/800ms）
  → 仍失败计数 SEND_MYSQL_FAILED 并丢弃该批（v1 不落盘死信，依赖幂等键支持 -b 补读重放）
Close：停 ticker → drain 残留批 → 关闭连接池
```

### 与 Doris 链路的关系

| 项 | Doris 链路（现状） | 本插件（MySQL） |
|----|--------------------|------------------|
| 明细表 | `bfe_ai_request_log`（Doris） | `bfe_ai_request_log`（MySQL，**同名同列**） |
| 列类型差异 | `ARRAY<STRUCT>` | `JSON` 列（req_headers/res_headers/ai_route_rule_hits/ai_cluster_key_names/ai_rate_limit_hits/quota plans） |
| 标签打平 | Routine Load 内 `json_extract` | 插件写时从 apikeytags 打平（level1Name/level1 … level5Name/level5） |
| 幂等 | UNIQUE KEY 模型 | 唯一键 `(hostid, log_time, ai_apikey_id, ai_requested_model)` + `ON DUPLICATE KEY UPDATE` |
| 聚合 | Doris INSERT JOB | ai-gateway-api 侧 JOB（不属于本方案） |

同一 log-reader 进程的 `Modules` 可同时配置两个模块，但**单集群建议只启用一种落库形态**（Kafka→Doris 与 MySQL 二选一，避免两条链路各自的数据缺口窗口导致两套报表口径不一致）。

## 详细改动

### 1. 公共字段抽取包 mod_fields（重构）

**新增目录：** `log-reader/reader_modules/mod_fields/`

**移动文件（逻辑不变，仅 package 名变更）：**

| 源（mod_kafka 内） | 目标（mod_fields 内） | 说明 |
|--------------------|------------------------|------|
| `field_registry.go`（注册表、`registerField`、`Extract`、字段集计算） | `mod_fields/field_registry.go` | package `mod_fields` |
| `field_registry_test.go` | `mod_fields/field_registry_test.go` | 同步移动 |
| 各字段 extract 函数（当前内嵌在 field_registry.go 中） | 可拆分为 `mod_fields/extractors.go` | 可选拆分，纯代码组织 |

**mod_kafka 侧修改：**

- `field_registry.go` 删除，import 改为 `github.com/rainway-ai-gateway/log-reader/reader_modules/mod_fields`；
- `json_converter.go` 的 `ConvertBfeLogToJSON` 数据源由本模块注册表改为 `mod_fields.Extract`（签名不变）；
- `kafka_data_config.go` 的 FieldMode/FieldNames 解析逻辑保留在 mod_kafka（是 JSON 输出的字段裁剪配置），内部调 `mod_fields` 的字段集计算；
- **兼容性验收标准**：`FieldMode = require/default/all/customized` 四种模式下，改造前后 JSON 输出逐字节一致（LR01 集成测试 + 手工 `bfe-pblog-tool` 对拍）。

### 2. go.mod 依赖

**文件：** `log-reader/go.mod`

新增：

```go
require github.com/go-sql-driver/mysql v1.9.0
```

通过标准库 `database/sql` 访问，为后续 PostgreSQL（`lib/pq` 或 pgx 的 `stdlib` 包装）预留驱动替换点。执行 `go mod tidy`。

### 3. mod_log_mysql 插件代码

#### 3.1 `mod_log_mysql.go`

```go
package mod_log_mysql

type ModuleLogMysql struct {
    conf      *ConfModLogMysql
    state     web_monitor.State
    stateDiff web_monitor.StateDiff
    mapper    *FieldMapper
    writer    *RecordWriter
}

func NewModuleLogMysql() *ModuleLogMysql { /* 初始化 state 计数器 */ }

func (m *ModuleLogMysql) Name() string { return "mod_log_mysql" }

func (m *ModuleLogMysql) Init(conf *reader_conf.ReaderConfig,
    whs *web_monitor.WebHandlers, cr string) error {
    // 1. state.CountersInit(COUNTER_KEYS)；stateDiff.Init(&m.state, conf.Main.MonitorInterval)
    // 2. gcfg 加载 ModConfPath(cr, "mod_log_mysql") → ConfModLogMysql，Check()
    // 3. sql.Open("mysql", dsn) + Ping；SetMaxOpenConns/SetMaxIdleConns
    // 4. NewFieldMapper()；NewRecordWriter(db, conf, &m.state) 并 Start()
    // 5. web_monitor.RegisterHandlers(whs, web_monitor.WebHandleMonitor, m.monitorHandlers())
}

func (m *ModuleLogMysql) Start() {} // 写协程已在 Init 启动；Start 无操作（与 mod_kafka 一致）

func (m *ModuleLogMysql) Update(bfeLogs []*bfe_access_pb.BfeLog) {
    // 1. IncReceivedLogs(len)
    // 2. 过滤 BfeLogType_Request（其余 IncCounter RECEIVED_REQ 后丢弃）
    // 3. mapper.ToRow(log) → []interface{}；失败 IncCounter CONVERT_FAILED 跳过
    // 4. writer.Enqueue(row)：channel 满则 IncCounter SENT_MYSQL_CHN_FULL 并丢弃
}

func (m *ModuleLogMysql) Close() error {
    // writer.Close()（drain + 关闭 db）
}
```

监控 handler 与 mod_kafka 同构：`getState`/`getStateDiff` + `monitorHandlers()` 注册 `mod_log_mysql`/`mod_log_mysql_diff`。

`COUNTER_KEYS`：

```go
var COUNTER_KEYS = []string{
    "RECEIVED_LOGS",        // Update 收到的日志条数
    "RECEIVED_REQ",         // 其中 request 类型条数
    "CONVERT_FAILED",       // 行组装失败条数
    "SENT_TO_MYSQL",        // 成功入队条数
    "SENT_MYSQL_CHN_FULL",  // 队列满丢弃条数（背压）
    "SEND_MYSQL_FAILED",    // 重试耗尽丢弃批数
    "WRITE_BATCH_SIZE",     // 每批实际条数累计（diff 求均值观测批大小）
}
```

#### 3.2 `conf_mod_log_mysql.go`

gcfg/INI 解析，风格对齐 `conf_mod_kafka.go`：

```ini
[Basic]
OpenDebug = false

[mysql]
Addr = 127.0.0.1:3306
User = report
Password = ******
DBName = bfe_report
Table = bfe_ai_request_log

[Writer]
QueueSize = 2000        ; 缓冲队列容量（条）
BatchSize = 200         ; 攒批条数
FlushIntervalMs = 2000  ; 攒批超时
MaxRetries = 3          ; 写失败重试次数
MaxOpenConns = 10
MaxIdleConns = 5
```

`Check()` 校验：Addr/User/DBName/Table 非空；QueueSize/BatchSize/FlushIntervalMs/MaxRetries > 0（非法给默认值并告警，对齐 mod_kafka 风格）。

**凭据安全**：Password 仅存在于本配置文件，部署上为插件建专用最小权限 MySQL 账号——仅目标库目标表有 INSERT/UPDATE 权限（`ON DUPLICATE KEY UPDATE` 需要 UPDATE），无 DDL/DELETE/CREATE 权限；表由部署流程使用 ai-gateway-api 仓库持有的 DDL 创建（见 §5），插件不提供自动建表。

#### 3.3 `field_mapper.go`

固定列序（与 DDL 列序一致，INSERT 语句预编译占位符 `(...),(...),...` 由列数生成）。列清单与 Doris `bfe_ai_request_log` **同名同列**（89 列，见 §5 DDL），组装规则：

- 标量列：`mod_fields.Extract(fieldName, log)` 取值；**字符串零值（""）→ NULL，数值/布尔 → 原样写入**（与 Doris Routine Load 的零值语义对齐，下游聚合侧统一 IFNULL/COALESCE 归一）；
- `level1Name/level1 … level5Name/level5`：从抽取出的 apikeytags map（`{"levelN":{tagname,tagvalue}}`）打平取值，空标签 → NULL；
- JSON 列（7 个）：`req_headers`、`res_headers`、`ai_route_rule_hits`、`ai_cluster_key_names`、`ai_rate_limit_hits`、`ai_auth_reject_quota_plans`、`ai_auth_hit_quota_plans`——抽取值为结构化值，`json.Marshal` 后写入；nil/空 → NULL。

列清单以代码内常量表为唯一权威（避免 DDL 与代码漂移），单测校验列数、列序与 DDL 文件解析结果一致。

#### 3.4 `record_writer.go`

```go
type RecordWriter struct {
    db     *sql.DB
    conf   *ConfModLogMysql
    ch     chan []interface{}   // 容量 QueueSize
    stmt   string                // 预生成 INSERT ... ON DUPLICATE KEY UPDATE
    state  *web_monitor.State
    stopCh chan struct{}
    wg     sync.WaitGroup
}

func (w *RecordWriter) Start()  // 单写协程：select ch/ticker/ stopCh
func (w *RecordWriter) Enqueue(row []interface{}) bool // 非阻塞，满返回 false
func (w *RecordWriter) Close()  // 停 ticker → drain ch 残留 → 最终 flush → db.Close()
```

写批逻辑：

1. 攒批：满 `BatchSize` 或 ticker（`FlushIntervalMs`）先到即发；
2. 单事务执行 `INSERT INTO <table> (cols...) VALUES (...),(...) ON DUPLICATE KEY UPDATE col=VALUES(col),...`（全部列覆盖更新，幂等）；
3. 失败按 200ms/400ms/800ms 退避重试至 `MaxRetries`，耗尽则 `IncCounter(SEND_MYSQL_FAILED)` 丢弃该批；
4. 写协程全程 recover（对齐 mod_kafka sendLoop 与 PbLogReader 的 Update recover 约定），panic 计数后协程退出需可被监控发现（diff 计数停增 + SERVER_READY 不变，运维按 SEND_TO_MYSQL 停滞发现）。

### 4. 模块注册

**文件：** `log-reader/reader_modules/all_modules.go`

```go
import "github.com/rainway-ai-gateway/log-reader/reader_modules/mod_log_mysql"

func SetModules() {
    reader_module.AddModule(mod_kafka.NewModuleKafka())
    reader_module.AddModule(mod_log_mysql.NewModuleLogMysql()) // 新增
}
```

`config.conf` 启用（`Modules` 已支持逗号列表与 `mod_name[:true|false]` 开关，无需改解析）：

```ini
[PbAccessLogConf]
LogFile = /home/work/bfe/log/pb_access3.log
Modules = mod_log_mysql
; 或并存：Modules = mod_kafka:false, mod_log_mysql:true
```

### 5. 配置文件与建表文件归属

**新增（本仓库）：** `log-reader/conf/mod_log_mysql/mod_log_mysql.conf`（内容见 §3.2）

**明细表 DDL 不归本仓库持有**，随 ai-gateway-api 仓库发布（`db_ddl_report_mysql.sql`，与聚合表 `bfe_ai_metrics_1m` 同文件）。归属理由：

1. **schema 依赖面**：ai-gateway-api 对明细表有聚合 JOB 按列取数、分区管理 JOB 执行 `ALTER TABLE`、明细查询三处依赖，且运行时持有 DDL 权限；log-reader 仅按固定列清单 INSERT，账号按最小权限原则无 DDL 权限。
2. **惯例**：ai-gateway-api 是本代码库唯一有 MySQL DDL 管理传统的仓库（`db_ddl.sql` 体系，AGENTS.md 明确改表纪律）；两张报表表与其同发布、同演进，列变更与查询/聚合代码同 PR 落地。
3. **发布顺序**：部署时先执行 api 仓库 DDL（schema-first），再启用本插件；pb 新增字段时 api 侧是「按需暴露」的特性决策，不构成插件上线的先行阻塞。

以下为该 DDL 文件的内容（本方案评审基准，实现时以 ai-gateway-api 仓库文件为准）：

```sql
-- 要求 MySQL >= 5.7.8（JSON 列），推荐 8.0
CREATE TABLE IF NOT EXISTS bfe_ai_request_log (
    hostid                  VARCHAR(256)  NOT NULL DEFAULT '',
    log_time                DATETIME      NOT NULL,
    ai_apikey_id            VARCHAR(256)  DEFAULT NULL,
    ai_requested_model      VARCHAR(128)  DEFAULT NULL,
    logid                   BIGINT        DEFAULT NULL,
    product                 VARCHAR(64)   DEFAULT NULL,
    log_tag                 VARCHAR(64)   DEFAULT NULL,
    -- 客户端连接
    client_ip               VARCHAR(64)   DEFAULT NULL,
    client_network          VARCHAR(16)   DEFAULT NULL,
    is_trust_src_ip         TINYINT       DEFAULT NULL,
    req_num                 INT           DEFAULT NULL,
    session_id              BIGINT        DEFAULT NULL,
    bfe_ip                  VARCHAR(64)   DEFAULT NULL,
    sock_src_ip             VARCHAR(64)   DEFAULT NULL,
    vip                     VARCHAR(64)   DEFAULT NULL,
    vip6                    VARCHAR(128)  DEFAULT NULL,
    -- 错误
    err_code                VARCHAR(64)   DEFAULT NULL,
    err_msg                 VARCHAR(512)  DEFAULT NULL,
    -- 请求（长文本列用 TEXT：utf8mb4 下 VARCHAR 总长度超 65535 字节行上限，Error 1118）
    proto                   VARCHAR(16)   DEFAULT NULL,
    header_host             VARCHAR(256)  DEFAULT NULL,
    origin_uri              TEXT          DEFAULT NULL,
    final_uri               TEXT          DEFAULT NULL,
    method                  VARCHAR(16)   DEFAULT NULL,
    content_type            VARCHAR(128)  DEFAULT NULL,
    x_forward_for           TEXT          DEFAULT NULL,
    accept_language         VARCHAR(256)  DEFAULT NULL,
    authorization           TEXT          DEFAULT NULL,
    transfer_encoding       VARCHAR(64)   DEFAULT NULL,
    referrer                TEXT          DEFAULT NULL,
    user_agent              TEXT          DEFAULT NULL,
    delegation              VARCHAR(256)  DEFAULT NULL,
    uid                     VARCHAR(256)  DEFAULT NULL,
    cookie                  TEXT          DEFAULT NULL,
    req_headers             JSON          DEFAULT NULL,
    req_header_len          INT           DEFAULT NULL,
    req_body_len            INT           DEFAULT NULL,
    -- 路由
    cluster                 VARCHAR(256)  DEFAULT NULL,
    sub_cluster             VARCHAR(256)  DEFAULT NULL,
    backend_info            VARCHAR(256)  DEFAULT NULL,
    backend_retry           TINYINT       DEFAULT NULL,
    -- 响应
    res_status_code         SMALLINT      DEFAULT NULL,
    res_header_len          INT           DEFAULT NULL,
    res_body_len            INT           DEFAULT NULL,
    res_content_type        VARCHAR(128)  DEFAULT NULL,
    res_location            TEXT          DEFAULT NULL,
    res_transfer_encoding   VARCHAR(64)   DEFAULT NULL,
    res_headers             JSON          DEFAULT NULL,
    -- 耗时（毫秒）
    all_time                INT           DEFAULT NULL,
    read_client_time        INT           DEFAULT NULL,
    cluster_serve_time      INT           DEFAULT NULL,
    backend_serve_time      INT           DEFAULT NULL,
    write_client_time       INT           DEFAULT NULL,
    connect_backend_time    INT           DEFAULT NULL,
    proxy_delay_time        INT           DEFAULT NULL,
    session_offset_time     INT           DEFAULT NULL,
    -- API Key 标签（写时打平）
    level1Name              VARCHAR(128)  DEFAULT NULL,
    level1                  VARCHAR(128)  DEFAULT NULL,
    level2Name              VARCHAR(128)  DEFAULT NULL,
    level2                  VARCHAR(128)  DEFAULT NULL,
    level3Name              VARCHAR(128)  DEFAULT NULL,
    level3                  VARCHAR(128)  DEFAULT NULL,
    level4Name              VARCHAR(128)  DEFAULT NULL,
    level4                  VARCHAR(128)  DEFAULT NULL,
    level5Name              VARCHAR(128)  DEFAULT NULL,
    level5                  VARCHAR(128)  DEFAULT NULL,
    -- AI 可观测
    ai_target_model         VARCHAR(128)  DEFAULT NULL,
    ai_stream               TINYINT       DEFAULT NULL,
    ai_input_tokens         BIGINT        DEFAULT NULL,
    ai_output_tokens        BIGINT        DEFAULT NULL,
    ai_total_tokens         BIGINT        DEFAULT NULL,
    ai_cache_read_tokens    BIGINT        DEFAULT NULL,
    ai_cache_write_tokens   BIGINT        DEFAULT NULL,
    ai_audio_input_tokens   BIGINT        DEFAULT NULL,
    ai_audio_output_tokens  BIGINT        DEFAULT NULL,
    ai_image_count          BIGINT        DEFAULT NULL,
    ai_ttft_us              BIGINT        DEFAULT NULL,
    ai_tpot_us              BIGINT        DEFAULT NULL,
    ai_provider             VARCHAR(64)   DEFAULT NULL,
    ai_protocol             VARCHAR(64)   DEFAULT NULL,
    ai_mode                 VARCHAR(64)   DEFAULT NULL,
    ai_retry_count          INT           DEFAULT NULL,
    ai_cost_value           BIGINT        DEFAULT NULL,
    ai_cost_currency        VARCHAR(16)   DEFAULT NULL,
    ai_route_rule_hits      JSON          DEFAULT NULL,
    ai_cluster_key_names    JSON          DEFAULT NULL,
    ai_rate_limit_hits      JSON          DEFAULT NULL,
    ai_auth_reject_reason   VARCHAR(256)  DEFAULT NULL,
    ai_auth_reject_quota_plans JSON       DEFAULT NULL,
    ai_auth_hit_quota_plans JSON          DEFAULT NULL,
    -- 幂等键：同键冲突覆盖（log-reader 重发/-b 补读安全）
    UNIQUE KEY uk_dedup (hostid, log_time, ai_apikey_id, ai_requested_model),
    KEY idx_model_time (ai_target_model, log_time),
    KEY idx_apikey_time (ai_apikey_id, log_time),
    KEY idx_provider_time (ai_provider, log_time),
    KEY idx_host_time (hostid, log_time),
    KEY idx_status_time (res_status_code, log_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(log_time)) (
    PARTITION p_init VALUES LESS THAN (TO_DAYS('${INIT_DATE}'))
);
```

要点：

- **唯一键与 Doris UNIQUE KEY 完全一致**——`ON DUPLICATE KEY UPDATE` 覆盖写使重发/补读幂等；唯一键必须包含分区列 `log_time`（MySQL 分区表约束）；
- **唯一键中的字符串列 `ai_apikey_id`/`ai_requested_model` 允许 NULL**（LR03 集成测试发现）：零值规则把空字符串映射为 NULL，未认证请求 `ai_apikey_id` 即 NULL，`NOT NULL` 会使这类写入整批失败（Error 1048）；`hostid`/`log_time` 由 log-reader 构造保证非空，保持 `NOT NULL`。副作用：NULL 不参与唯一键去重，无 Key 日志的重发不去重（与 Doris UNIQUE KEY 对 NULL 的语义一致），可接受；
- **长文本列用 TEXT 而非 VARCHAR**（LR03 集成测试发现）：utf8mb4 下 8 个 1K~4K 长度 VARCHAR 列（origin_uri/final_uri/x_forward_for/authorization/referrer/user_agent/cookie/res_location）总长度超 MySQL 65535 字节行上限，建表即报 Error 1118；
- 唯一键长度 `(256+256+128)*4+5 ≈ 2565` 字节 < 3072 InnoDB 上限（DYNAMIC 行格式），无需前缀索引；
- **按天 RANGE 分区**：与 Doris 动态分区语义对齐；首个分区由建表 DDL 的 `p_init` 提供，新分区创建与过期分区 DROP 由报表查询侧（ai-gateway-api）的分区管理 JOB 负责，本仓库不提供。注意 MySQL 无动态分区，**分区管理 JOB 必须先于数据到达建好分区，否则对应日期的写入被直接拒绝**（Error 1526）；
- pb 中已有、Doris 表尚未纳入的新字段（`ai_image_input_tokens`、`ai_video_count`、`ai_cache_write_1h_tokens` 等）本表也不含，待 Doris 侧升级时两侧同步加列（见 §依赖与兼容性）。

### 6. 监控

- 复用现有 web-monitor 框架（`whs` 注册，`web_monitor.RegisterHandlers`），新增 handler：`mod_log_mysql`（累计值）、`mod_log_mysql_diff`（周期 diff）；
- 健康检查口径：`SENT_TO_MYSQL` 持续增长且 `SEND_MYSQL_FAILED`/`SENT_MYSQL_CHN_FULL` 为零为正常；`SERVER_READY` 沿用主进程状态。

### 7. 单元测试

新增（对齐 mod_kafka 测试风格）：

| 文件 | 用例 |
|------|------|
| `mod_log_mysql/conf_mod_log_mysql_test.go` | 默认值填充、非法值校验、必填缺失报错 |
| `mod_log_mysql/field_mapper_test.go` | 行组装的列数/列序/零值→NULL 规则；apikeytags 打平；JSON 列 marshal；与 DDL 列清单一致性校验 |
| `mod_log_mysql/record_writer_test.go` | `github.com/DATA-DOG/go-sqlmock`：攒批触发（条数/超时）、SQL 语句形状（占位符数、`ON DUPLICATE KEY UPDATE` 子句）、重试次数与退避、drain 语义、channel 满丢弃计数 |
| `mod_log_mysql/mod_log_mysql_test.go` | Update 过滤（仅 Request）、CONVERT_FAILED 路径、监控 handler 输出 |
| `mod_kafka` 回归 | 改造后全部既有单测原样通过（JSON 行为不变） |

### 8. 集成测试 scenario-LR03

新增场景 `tests/integration/implementation/scenario-LR03-mysql-write/`（结构对齐 LR01）：

- **数据准备**：复用 `common/log_generator.go` 生成含全字段的 b2log 日志文件；
- **被测进程**：真实 `log_reader` 二进制 + `Modules = mod_log_mysql` + 测试 MySQL（`testcontainers-go` 起 MySQL 8.0，镜像不可用时按环境变量 `LR_MYSQL_DSN` 指向外部实例，两者都不可用则 `t.Skip`）；表结构由 testdata 内嵌的 DDL 副本初始化（标注为 ai-gateway-api 仓库权威 DDL 的拷贝，field_mapper 单测比对列清单兜底漂移）；
- **断言**：① 表记录数与日志条数一致；② 抽样字段值正确（含 level1-5 打平列、JSON 列可反序列化）；③ **幂等重放**：同一日志文件第二次投喂（或重启进程 `-b` 重读），记录数不变、值被覆盖一致；④ 背压：灌入超过 QueueSize 的流量，`SENT_MYSQL_CHN_FULL` 计数增长且不 panic；
- **测试替身选型**：不用 SQLite 等嵌入式库替代 MySQL——核心幂等语句 `INSERT ... ON DUPLICATE KEY UPDATE` 与 DDL 的 `PARTITION BY RANGE / TO_DAYS()` 均为 MySQL 专有（SQLite 的 `ON CONFLICT ... DO UPDATE` 语法不同且不支持表分区），为测试引入方言抽象会让生产代码背负一条永不生产的代码路径；且 SQLite 动态类型（JSON 实为 TEXT、不截断超长 VARCHAR、接受非法日期）与 MySQL 严格模式差异大，易产生假阳性。无 Docker 环境的兜底顺序：go-sqlmock 单测（必跑）→ `LR_MYSQL_DSN` 外部实例 → `t.Skip`；
- **测试设计文档**：`tests/integration/测试设计文档/scenario-LR03-mysql-write/`（场景说明.md + TC 文档），更新 `tests/integration/README.md` 场景清单。

### 9. 文档更新

| 文件 | 改动 |
|------|------|
| `README.md` | 特性列表、目录结构、配置说明、监控项清单增加 mod_log_mysql |
| `readme.txt` | 精简版同步 |
| `doc/modules/mod_log_mysql/mod_log_mysql.md` | 模块说明、数据流、监控项、排障 |
| `doc/modules/mod_log_mysql/output-columns.md` | 列清单与类型（与 DDL 同源生成） |
| `doc/configuration/mod_log_mysql/mod_log_mysql.conf.md` | 配置项逐项说明 |
| `doc/configuration/config.conf.md` | `Modules` 多模块语法示例 |
| `doc/howto/03 MySQL 报表链路部署指南.md` | 可选：轻量形态端到端部署步骤（含最小权限账号 SQL 样例） |
| `CHANGELOG.md` | 新增 1.4.0 条目 |

### 10. 版本与发布

- `VERSION`：1.3.0 → **1.4.0**（新增功能，向后兼容）；
- `make release` 产物自动包含新插件的默认 conf（检查 `Makefile` 的打包清单是否通配 `conf/`，否则补充）；
- `make license-check`：新文件需带 BFE 风格 Apache License 头（沿用现有文件头模板）。

## 代码变更说明

- 【新增】`log-reader/reader_modules/mod_fields/`（field_registry.go / extractors.go / field_registry_test.go，自 mod_kafka 上移）
- 【修改】`log-reader/reader_modules/mod_kafka/field_registry.go`（删除，改用 mod_fields）
- 【修改】`log-reader/reader_modules/mod_kafka/json_converter.go`、`kafka_data_config.go`（数据源改 mod_fields，行为不变）
- 【修改】`log-reader/reader_modules/mod_kafka/field_registry_test.go`（迁移/适配）
- 【新增】`log-reader/reader_modules/mod_log_mysql/`（mod_log_mysql.go、conf_mod_log_mysql.go、field_mapper.go、record_writer.go + 单测）
- 【新增】`log-reader/reader_modules/mod_log_mysql/conf/mod_log_mysql.conf`（DDL 不在本仓库：明细表随 ai-gateway-api 仓库 `db_ddl_report_mysql.sql` 发布，见 §5）
- 【修改】`log-reader/reader_modules/all_modules.go`（注册新模块）
- 【修改】`log-reader/go.mod` / `go.sum`（go-sql-driver/mysql）
- 【新增】`log-reader/tests/integration/implementation/scenario-LR03-mysql-write/`（用例 + testdata）
- 【新增】`log-reader/tests/integration/测试设计文档/scenario-LR03-mysql-write/`
- 【修改】`log-reader/tests/integration/README.md`、`tests/integration/common/log_generator.go`（如 LR01 也需覆盖新字段）
- 【修改】`README.md`、`readme.txt`、`CHANGELOG.md`、`VERSION`
- 【新增】`doc/modules/mod_log_mysql/*`、`doc/configuration/mod_log_mysql/*`、`doc/howto/03 MySQL 报表链路部署指南.md`（可选）
- 【修改】`doc/configuration/config.conf.md`
- 【新增】`doc/modifications/2026-09-15-add-mod-log-mysql-plugin/design-changes.md`（本文档）

## 验证步骤

```bash
cd log-reader

# 1. 静态检查与单测（全绿，mod_kafka 回归含在内）
make check
go test ./reader_modules/... -race

# 2. 集成测试（LR01 回归 + LR03 新场景）
go test ./tests/integration/implementation/scenario-LR01-basic-flow/... -v
LR_MYSQL_DSN="report:****@tcp(127.0.0.1:3306)/bfe_report" \
  go test ./tests/integration/implementation/scenario-LR03-mysql-write/... -v

# 3. 手工端到端（开发机）
#    3.1 从 ai-gateway-api 仓库取 db_ddl_report_mysql.sql 执行（替换 ${INIT_DATE}）：mysql bfe_report < db_ddl_report_mysql.sql
#    3.2 配置 conf/config.conf Modules = mod_log_mysql 与 mod_log_mysql.conf
#    3.3 用 bfe-pblog-tool 造/确认 pb_access3.log 有数据，启动 ./log_reader -c ../conf -l ../log -s -b
#    3.4 观察 http://127.0.0.1:8992/monitor/mod_log_mysql：SENT_TO_MYSQL 增长，FAILED/CHN_FULL 为 0
#    3.5 SELECT 比对明细表记录数与 pb 日志条数；重复启动（-b）验证记录数不变（幂等）

# 4. 全量构建
make
```

## 影响范围

| 维度 | 影响 |
|------|------|
| 存量部署 | 零影响：新插件默认不启用（`Modules` 不含即不加载）；mod_kafka 重构后 JSON 输出字节级不变（LR01 回归保障） |
| 配置 | 新增 `conf/mod_log_mysql/` 目录（仅 mod_log_mysql.conf，DDL 归 ai-gateway-api 仓库）；`config.conf` 的 `Modules` 语法不变 |
| 依赖 | 新增 `go-sql-driver/mysql`；bfe-access-pb 版本不变 |
| 性能 | 插件仅在启用时消耗资源；写路径异步（channel + 单协程），对日志读取主流程的影响与 mod_kafka 同量级 |
| 下游 | 无：不改动 Kafka JSON 格式；MySQL 明细表供 ai-gateway-api 报表 API 消费（另案实现） |

## 依赖与兼容性

- **MySQL**：>= 5.7.8（JSON 列依赖），推荐 8.0；分区表要求唯一键包含分区列（已满足）。
- **bfe-access-pb**：沿用当前 v0.3.5，不升级。
- **与 Doris 明细表的列对齐**：本表与 `ai-gateway-observability` 仓库的 Doris DDL 同名同列；任一侧加列（如 pb 新增 `ai_image_input_tokens`/`ai_video_count`/`ai_cache_write_1h_tokens`）时需两侧同步，由 `ai-gateway-observability` 的 `skills/gen-req-log-api` 流程驱动。
- **回滚**：插件纯增量；停用 = 配置移除 `mod_log_mysql` 并重启；已写入 MySQL 的数据不受回滚影响（独立 report 库）。
- **已知限制**：log-reader 无持久化位点（重启丢窗口，`-b` 补读依赖本表幂等键）；写失败无死信（监控 `SEND_MYSQL_FAILED` 告警 + 补读重放兜底）。

---

*文档生成日期：2026-09-15*
