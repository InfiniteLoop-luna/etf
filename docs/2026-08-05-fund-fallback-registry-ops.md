# 2026-08-05 - 基金 fallback registry 机制运维说明

## 背景

生产上发现部分基金（典型案例：`007491.OF` / `南方信息创新混合C`）在 Tushare `fund_basic` 接口中查不到，但在以下辅助接口中可以查到：

- `fund_nav`
- `fund_share`
- `fund_manager`
- `fund_portfolio`

旧逻辑过度依赖 `fund_basic -> ts_fund_basic -> vw_fund_basic` 建立基金主档，导致这类基金：

1. 不能进入基金基础库
2. 不能被 `fund_portfolio` 动态同步发现
3. 页面搜索基金代码时查不到

## 已上线机制

### 1. 新增辅助注册表

新增表：`fund_registry_aux`

用途：
- 存放 `fund_basic` 缺失、但可由其他辅助接口确认存在的基金
- 保存辅助发现得到的最小主档与补充元数据

主要字段：
- `fund_code`
- `name`
- `management`
- `fund_type`
- `invest_type`
- `status`
- `market`
- `latest_nav_date`
- `latest_share_date`
- `latest_portfolio_period`
- `discovered_sources`
- `payload`
- `updated_at`

### 2. 新增统一基金视图

新增视图：`vw_fund_registry`

统一合并三类来源：

1. `vw_fund_basic`
2. `vw_fund_portfolio` 中已有但未出现在 `vw_fund_basic` 的基金
3. `fund_registry_aux`

后续基金搜索、元信息查询、持仓同步目标集合，统一应以 `vw_fund_registry` 为主，而不是只依赖 `vw_fund_basic`。

### 3. 搜索时自动辅助发现

`search_funds()` 已加入自动补发现逻辑：

- 若用户输入像基金代码（如 `007491` / `007491.OF`）
- 且 `vw_fund_registry` 首次查不到
- 则自动调用辅助发现：
  - `fund_nav`
  - `fund_share`
  - `fund_manager`
  - `fund_portfolio`
- 若确认基金存在，则写入 `fund_registry_aux`
- 再回查 `vw_fund_registry`

### 4. 正式名称补全

辅助发现过程中，新增 AkShare 名称补全：

- 通过 `ak.fund_name_em()` 按裸码匹配（如 `007491`）
- 获取正式基金简称（如 `南方信息创新混合C`）
- 写入 `fund_registry_aux.name`

### 5. 占位名称刷新

已增加刷新规则：

如果基金已经存在于 `vw_fund_registry`，但名称仍为占位值（例如：
- `007491.OF`
- `007491`
- 空值
）

则允许再次执行辅助发现，并把正式名称覆盖回去。

## 已确认的线上真实结果

`007491.OF` 当前线上已验证：

- 可以通过辅助发现进入 `fund_registry_aux`
- 可以通过 `vw_fund_registry` 被搜索到
- 名称已升级为：`南方信息创新混合C`

## 当前代码层结论

### 当前真正落库的 Tushare 基金表

生产 PostgreSQL 中已存在：
- `ts_fund_basic`
- `ts_fund_portfolio`

当前不存在：
- `ts_fund_nav`
- `ts_fund_share`
- `ts_fund_manager`

因此当前 fallback registry 的角色不是“把所有辅助接口都完整入库”，而是：

> 先把 `fund_basic` 缺失基金建立成可检索、可继续同步的主索引入口。

## 关键运维认知

### 1. 不是所有基金都能从 `fund_basic` 发现

像 `007491.OF` 这种基金：
- `fund_basic` 没有
- 但辅助接口有数据

所以不能再假设 `fund_basic` 是完整基金 universe。

### 2. 不能只靠 `vw_fund_basic` 驱动后续同步

因为如果基金主档从未建立：
- `fund_portfolio` 动态同步就可能永远发现不到它

### 3. fallback registry 是“基金发现层”，不是完整替代基础库

它解决的是：
- 查不到基金
- 主档缺失
- 后续同步无法纳入

而不是一次性替代所有标准基金基础表字段。

## 后续建议

### 建议 1：后续查询尽量统一读 `vw_fund_registry`

任何“先按基金代码/基金名称找基金”的业务，应优先依赖：
- `vw_fund_registry`

### 建议 2：必要时可增加批量辅助发现任务

目前已上线的是：
- 用户首次搜索时自动发现

后续可考虑增加：
- 对自选基金列表批量预热补发现
- 对常用基金代码清单批量补发现

### 建议 3：如果以后要扩展更多字段，再考虑独立 enrichment 流程

例如：
- 更稳定的基金正式名称
- 基金公司正式名称
- 基金类型更精细分类
- 经理列表/历史经理

这些可作为第二阶段 enhancement，不影响当前 registry 机制有效性。

## 本次相关提交

- `79f040a` `feat: add fallback fund registry discovery`
- `7ca6a87` `fix: sanitize fallback fund registry payloads`
- `f1f236d` `feat: enrich fallback fund names with akshare`
- `a83289a` `fix: upgrade placeholder fallback fund names`
- `af50933` `fix: refresh placeholder fallback funds`
