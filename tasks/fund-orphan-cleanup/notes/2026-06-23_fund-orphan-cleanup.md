# 2026-06-23 基金持仓 orphan 旧码清理记录

## 背景

用户反馈 ETF 项目里“基金持仓查询”基本查不出结果，怀疑大量基金代码已经失效但系统里仍然保留旧码。

排查确认：问题不是前端查询函数整体失效，而是数据库里存在一批历史残留的 orphan 基金代码：

- `vw_fund_portfolio` 中仍有这些基金代码的持仓记录
- 但当前 `vw_fund_basic` / Tushare `fund_basic` 已不再承认这些代码
- 结果是查询链路里会继续暴露错误旧码，误导用户

## 关键排查结论

### 1. 查询函数本身可用

在本地和 VPS 都验证过：

- `018993.OF`（中欧数字经济混合-A）可正常查询持仓
- `018994.OF`（中欧数字经济混合-C）可正常查询持仓
- `159994.SZ`（银华中证5G通信主题ETF）可正常查询持仓

说明问题不在 `query_fund_preference_snapshot()` 本身。

### 2. VPS 库里存在大量 orphan 基金代码

清理前在线上库实测：

- orphan 基金代码数：`1032`
- orphan 持仓记录数：`16759`

典型 orphan 代码示例：

- `002411.OF`
- `001261.OF`
- `001158.OF`
- `001717.OF`
- `001157.OF`
- `007491.OF`

### 3. VPS 真实 Tushare fund_basic 已不返回这些旧码

使用 VPS 上真实 `TUSHARE_TOKEN` 直接调用 `fetch_fund_basic()` 后确认：

- 当前上游能返回：`017968.OF`、`018993.OF`、`018994.OF`、`159994.SZ`
- 当前上游不返回：`001717.OF`、`001158.OF`、`002411.OF`、`007491.OF`

这说明 orphan 旧码不是当前同步脚本刚生成的，而是历史持仓残留；如果不清理，它们会一直污染查询结果。

## 执行操作

### 1. 先备份 orphan 数据

VPS 路径：

- `/opt/etf-app/tasks/fund-orphan-cleanup/outputs/orphan_fund_portfolio_rows_20260623.tsv`
- `/opt/etf-app/tasks/fund-orphan-cleanup/outputs/orphan_fund_portfolio_summary_20260623.tsv`

备份规模：

- 明细文件：`16759` 行
- 汇总文件：`1032` 行（按 orphan 基金代码汇总）

### 2. 删除 orphan 基金持仓

删除条件：

- `ts_fund_portfolio.ts_code` 在 `vw_fund_basic.fund_code` 中不存在

执行结果：

- 删除前 orphan 行数：`16759`
- 实际删除：`16759`
- 删除后 orphan 行数：`0`

### 3. 重建基金热股聚合

在 VPS 上调用：

- `rebuild_hot_stock_aggregate(engine)`

重建结果：

- 聚合写入：`10538` 条
- 最新报告期：`2026-03-31`

## 清理后验证

### orphan 已归零

VPS 最终核对：

- `orphan_code_count = 0`
- `orphan_row_count = 0`

### 持仓查询行为符合预期

清理后验证：

- `001717.OF` → 查询结果 `0`
- `007491.OF` → 查询结果 `0`
- `018993.OF` → 查询结果 `3`
- `018994.OF` → 查询结果 `3`
- `159994.SZ` → 查询结果 `3`

结论：错误旧码已从数据库层面消失；现行有效基金代码查询正常。

## 清理后数据规模

- `vw_fund_portfolio = 136778`
- `agg_fund_holding_stock_quarterly = 10538`

## 同步链路代码侧补强

除数据库清理外，本轮也对 `src/fund_hot_stocks.py` / `app.py` 做了补强，减少以后再次积累同类脏数据：

- 搜索候选不再只依赖 `vw_fund_basic`
- 同步/查缺任务不再完全忽略 `vw_fund_portfolio` 中基础表缺失的代码
- 前端对直接输入失效代码的场景有更明确提示

注意：本次真正解决“错的不再存在”的关键动作仍然是 **VPS 数据库清理**，不是映射。

## 后续建议

1. 后续若再出现基金持仓查询异常，优先检查：
   - `vw_fund_portfolio` 是否重新出现 orphan 基金代码
   - `fund_basic` 与 `fund_portfolio` 的集合是否再次脱节
2. 若夜跑未来又重新引入 orphan 代码，应继续追踪：
   - 上游 `fund_portfolio` 是否仍会返回旧 ts_code
   - 当前 `fund_basic` 的市场/状态过滤是否覆盖完整
3. 可考虑补一个定期巡检 SQL：
   - 统计 orphan 基金代码数
   - 非 0 时主动报警
