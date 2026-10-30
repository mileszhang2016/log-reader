# TC-08 缺价信号字段写入与 NULL 规则

## 用例编号与名称

TC-08 缺价信号字段写入与 NULL 规则（`TestLR03_PriceMissColumns`）

## 所属场景

LR03 MySQL 写入场景（scenario-LR03-mysql-write）

## 版本声明

- `log-reader`：当前源码版本（`mod_log_mysql` 101 列，`bfe-access-pb` v0.3.17 字段 763/766）
- 表结构：ai-gateway-api 权威 `db_ddl_report_mysql.sql` 明细表 101 列版内嵌副本（基准提交 `508e552`，含 2026-10-30 加列）

## 测试目的

验证缺价信号两列（`ai_price_miss`、`ai_price_warn`，bfenetworks/bfe#1479）的端到端写入：

1. 命中缺价回退的请求（`fallback`）与因缺价被拒绝的请求（`reject`，非 2xx）都把信号落到明细行上；
2. 未发生缺价的请求写入 **NULL**，而不是 `0` / `''`——报表侧不能把"未发生"读成"发生了但形态未知"；
3. `ai_price_miss` 显式 `false` 写 `0`，与 NULL 可区分。

## 运行模式

单组件模式：真实 `log-reader` 进程 + 真实 MySQL。

## 前置条件

同 TC-01。

## 配置构造

同 TC-01。

## 输入数据

- **Part 1（回退）**：`common.MakeRequestLog(83001, ...)`，追加 `ai_price_miss=true`、`ai_price_warn=fallback`（非 chat 接口按该模型 chat 价计）。
- **Part 2（拒绝）**：`common.MakeRequestLog(83002, ...)`，追加 `ai_price_miss=true`、`ai_price_warn=reject`、`res_status_code=503`（bfe 在转发前拒绝，行仍会写出）。
- **Part 3（正常定价）**：`common.MakeRequestLog(83003, ...)`，两字段均未设置。
- **Part 4（显式 false）**：`common.MakeRequestLog(83004, ...)`，仅设 `ai_price_miss=false`，`ai_price_warn` 未设置。

## 操作步骤

1. 准备环境并启动 `log-reader`。
2. 依次写入 Part 1~4 的四条日志。
3. 轮询等待表行数 ≥ 4（超时 20s）。
4. 按 `logid = 83001 / 83002 / 83003 / 83004` 分别查询并断言两列取值。

## 预期结果

- **Part 1**：`ai_price_miss = 1`、`ai_price_warn = 'fallback'`。
- **Part 2**：`res_status_code = 503`、`ai_price_miss = 1`、`ai_price_warn = 'reject'`（非 2xx 行同样带信号，禁止用"仅 2xx 行才有信号"的口径统计）。
- **Part 3**：`ai_price_miss IS NULL`、`ai_price_warn IS NULL`（mapper `kindOptionalBoolTiny` / `kindOptionalStr` 按 proto 指针判存在，未设置 → NULL）。
- **Part 4**：`ai_price_miss = 0`（显式 false，非 NULL）、`ai_price_warn IS NULL`。

## 清理

停止 `log-reader` 进程，关闭数据库连接，DROP 测试专用数据库，删除临时目录。