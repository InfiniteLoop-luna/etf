# 资金流向数据接入与炒股辅助功能方案

## 背景

基于对 Tushare `doc_id=342`（资金流向数据）文档及现有 ETF 项目代码架构的分析，围绕 **个股资金流向** 构建从数据拉取、入库、到页面展示的完整流水线，融入现有 Streamlit + PostgreSQL 体系。

---

## 数据源清单（资金流向系列）

Tushare 在 `doc_id=342` 下提供 8 个子接口，主要分析以下**高价值接口**：

| 接口名 | Tushare函数 | 数据维度 | 权限要求 |
|---|---|---|---|
| 个股资金流向 | `moneyflow` | 每股每日 超/大/中/小单的净额和占比 | 2000积分 |
| 个股资金流向（THS）| `moneyflow_ths` | 同花顺口径 超/大/中/小单净流入 | 5000积分 |
| 个股资金流向（DC）| `moneyflow_dc` | 东方财富口径 超/大/中/小单净流入 | 2000积分 |
| 行业资金流向（THS）| `moneyflow_ind_ths` | 行业每日总流向 | 5000积分 |
| 板块资金流向（DC）| `moneyflow_dc_sector` | 板块总流向 | 2000积分 |
| 大盘资金流向（DC）| `moneyflow_dc_index` | 大盘整体流向 | 2000积分 |
| 沪深港通资金流向 | `moneyflow_hsgt` | 北向/南向资金 | 100积分 |

> [!IMPORTANT]
> 需先确认你的 Tushare 账户积分是否满足以上接口要求。`moneyflow`(2000分) 和 `moneyflow_hsgt`(100分) 门槛最低，建议优先接入。

---

## 数据字段说明（个股资金流向 `moneyflow`）

| 字段 | 类型 | 含义 | 炒股意义 |
|---|---|---|---|
| `ts_code` | str | 股票代码 | 标识 |
| `trade_date` | str | 交易日期 | 时间轴 |
| `buy_sm_vol/buy_sm_amount` | int/float | 小单买入（量/额） | 散户情绪 |
| `sell_sm_vol/sell_sm_amount` | int/float | 小单卖出 | 散户出逃 |
| `buy_md_vol/buy_md_amount` | int/float | 中单买入 | 中等资金动向 |
| `sell_md_vol/sell_md_amount` | int/float | 中单卖出 | — |
| `buy_lg_vol/buy_lg_amount` | int/float | 大单买入 | 机构布局 |
| `sell_lg_vol/sell_lg_amount` | int/float | 大单卖出 | 机构撤离 |
| `buy_elg_vol/buy_elg_amount` | int/float | 超大单买入 | 主力资金流入 |
| `sell_elg_vol/sell_elg_amount` | int/float | 超大单卖出 | 主力资金流出 |
| `net_mf_vol` | int | 主力净流入量 | ⭐核心指标 |
| `net_mf_amount` | float | 主力净流入额（万元） | ⭐核心指标 |

---

## 炒股信号设计

### 信号1：主力资金持续净流入（强度信号）
- **逻辑**：连续 N 天（如3/5/10天）主力净流入 > 0，且净流入额呈增大趋势
- **意义**：主力在持续建仓，可能酝酿上涨

### 信号2：超大单净流入 + 大单净流入同步为正
- **逻辑**：`buy_elg_amount - sell_elg_amount > 0` AND `buy_lg_amount - sell_lg_amount > 0`
- **意义**：机构和主力同向买入，信号强烈

### 信号3：散户卖出+主力买入（分歧博弈）
- **逻辑**：小单净流出 + 主力净流入同时满足
- **意义**：主力利用散户抛售压力吸筹，经典吸筹形态

### 信号4：行业资金轮动
- **逻辑**：对比行业资金流向，找出连续两日净流入Top 3的行业
- **意义**：捕捉热点板块轮动机会

### 信号5：北向资金追踪（沪深港通）
- **逻辑**：`moneyflow_hsgt` 北向净买入额连续正值
- **意义**：外资偏好，长期价值参考

---

## 技术架构方案

```
  Tushare API
      |
      v
[数据拉取脚本] src/moneyflow_fetcher.py
  - 首次拉取：2025-01-01 至今（全量历史）
  - 每日增量：GitHub Actions 定时触发（每天17:00后）
  - 接口限制处理：每次请求500条，分批遍历日期
      |
      v
[PostgreSQL 数据库]（已有 67.216.207.73）
  表：ts_moneyflow           -- 个股资金流向（主力核心）
  表：ts_moneyflow_hsgt      -- 沪深港通资金流向（北向）
  表：ts_moneyflow_ind_ths   -- 行业资金流向（可选）
  视图：vw_money_signals     -- 预计算信号视图
      |
      v
[Streamlit 新标签页] "资金流向"
  - 每日主力净流入排行榜（Top 20）
  - 资金流向趋势图（按个股查询）
  - 连续净流入选股策略
  - 大盘/北向资金概览
  - 行业资金热力图
```

---

## 拟新增文件

### 1. [NEW] `src/moneyflow_fetcher.py`
负责从 Tushare 拉取资金流向数据并入库 PostgreSQL

- 参照 `sync_tushare_security_data.py` 的 JSONB landing 表模式
- 支持初始全量历史拉取（2025-01-01起）
- 支持增量更新（只取最新未拉取的日期）
- 按日期批量拉取（每次一天），控制 API 频率

### 2. [MODIFY] `src/sync_tushare_security_data.py`
- 在 `DATASET_TABLES` 和 `NORMALIZED_VIEW_SPECS` 中添加 `moneyflow` 和 `moneyflow_hsgt` 数据集定义

### 3. [MODIFY] `app.py`
- 新增"📈 资金流向"标签页
- 子页面：主力净流入榜 / 个股资金流图 / 北向资金 / 选股策略

### 4. [MODIFY] `.github/workflows/update-data.yml`
- 新增资金流向数据更新 step，每个交易日 17:00 后触发

---

## 数据库表设计

### `ts_moneyflow`（沿用现有 JSONB landing 表模式）
```sql
CREATE TABLE IF NOT EXISTS ts_moneyflow (
    business_key TEXT PRIMARY KEY,   -- ts_code + trade_date
    dataset_name VARCHAR(64),
    ts_code VARCHAR(20),
    trade_date DATE,
    record_hash VARCHAR(64),
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
-- 索引
CREATE INDEX ON ts_moneyflow(trade_date);
CREATE INDEX ON ts_moneyflow(ts_code);
```

### `vw_moneyflow`（规范化视图）
展开 JSONB 字段为可查询的结构化列，便于 SQL 分析

---

## 实施步骤

- [ ] **Step 1** — 在数据库建表（复用 `ensure_landing_table` 机制）
- [ ] **Step 2** — 编写 `moneyflow_fetcher.py`（拉取 + 入库 + 增量检测）
- [ ] **Step 3** — 手动执行全量拉取（2025-01-01 至今）
- [ ] **Step 4** — 创建规范化视图和信号视图
- [ ] **Step 5** — 在 `app.py` 添加"资金流向"标签页 + 可视化
- [ ] **Step 6** — 配置 GitHub Actions 定时任务

---

## 开放问题

> [!IMPORTANT]
> **Q1：你的 Tushare 积分是多少？**
> - `moneyflow`：需要 **2000积分**
> - `moneyflow_hsgt`：需要 **100积分**（最低门槛）
> - `moneyflow_ind_ths`（行业）：需要 **5000积分**
>
> 这决定我们能接入哪几个接口。

> [!NOTE]
> **Q2：重点关注个股还是板块/大盘？**
> - 如果做个股选股：重点接 `moneyflow` + `moneyflow_hsgt`
> - 如果做板块轮动：需要 `moneyflow_ind_ths` 或 `moneyflow_dc_sector`

> [!NOTE]
> **Q3：是否需要同时保留 THS 和 DC 两个口径？**
> - THS（同花顺）和 DC（东方财富）的数据分类标准略有差异
> - 接一个即可，建议先接 `moneyflow`（标准口径）

---

## 预期效果

完成后，在 Streamlit 中新增"资金流向"页面，可以：

1. 看到**今日主力资金净流入 Top 20** 个股排行
2. 点击个股查看**历年资金流入趋势图和买卖力量对比**
3. 一键筛选**连续 N 天主力净流入**的个股（选股策略）
4. 监控**北向资金**每日动向（沪深港通）
5. 每天 GitHub Actions 自动更新数据，无需手动干预
