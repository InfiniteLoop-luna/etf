# 宏观经济 Tab 设计与实现方案

**日期**: 2026-04-14
**状态**: 实施中

## 1. 目标

在现有 Streamlit 看板中新增一个 `宏观经济` Tab，集中展示影响 ETF 与权益资产判断的核心宏观变量，首期覆盖以下 6 组 Tushare 宏观接口：

- `cn_gdp`: GDP 总量与同比
- `cn_cpi`: CPI 全国当月值、同比、环比
- `cn_ppi`: PPI 同比、环比、累计同比
- `cn_m`: M0/M1/M2 与同比、环比
- `shibor`: 隔夜、1周、1月、3月、1年利率
- `shibor_lpr`: 1年、5年 LPR

## 2. 设计思路

### 2.1 页面结构

顶层新增 `宏观经济` Tab，内部拆成 4 个子区域：

- `总览`: 6 张指标卡，展示最新值、最新日期、较上一期变化
- `增长与通胀`: GDP、CPI、PPI 趋势图
- `流动性与利率`: M2、Shibor、LPR 趋势图
- `原始明细`: 展示当前筛选窗口下的底表数据

这样做的原因：

- 复用现有 `宽基指数ETF` 页的时间筛选和指标卡交互模式
- 将低频季度/月度数据与高频日度利率数据放在不同区域，减少视觉噪音
- 先给结论，再给趋势，再给明细，符合投研阅读顺序

### 2.2 数据接入方式

沿用现有 `sync_tushare_security_data.py` 的通用落地模式：

1. 从 Tushare 拉取原始 DataFrame
2. 统一补充 `trade_date/end_date/period`
3. 写入 PostgreSQL landing table
4. 自动生成标准化 view
5. 在 `etf_stats.py` 中封装查询函数
6. 在 `app.py` 中通过缓存函数调用并渲染

### 2.3 为什么不用实时直连 Tushare

- 现有项目已经形成了 “同步脚本 -> DB -> Streamlit” 的稳定链路
- 页面打开时不依赖 Tushare 网络状态，体验更稳定
- 宏观数据更新频率低，适合预同步
- 后续容易扩展更多宏观口径或计算衍生指标

## 3. 数据模型

### 3.1 落地表

新增以下 landing table：

- `ts_macro_cn_gdp`
- `ts_macro_cn_cpi`
- `ts_macro_cn_ppi`
- `ts_macro_cn_m`
- `ts_macro_shibor`
- `ts_macro_shibor_lpr`

### 3.2 标准化视图

新增以下 view：

- `vw_ts_macro_cn_gdp`
- `vw_ts_macro_cn_cpi`
- `vw_ts_macro_cn_ppi`
- `vw_ts_macro_cn_m`
- `vw_ts_macro_shibor`
- `vw_ts_macro_shibor_lpr`

### 3.3 统一日期口径

- GDP 季频：将 `quarter` 转为季度末日期
- CPI/PPI/M2 月频：将 `month` 转为月末日期
- Shibor/LPR 日频：直接使用 `date`

统一后，前端可以直接按 `trade_date` 画图，无需分别处理季度/月度/日度。

## 4. 页面展示方案

### 4.1 总览指标卡

推荐展示：

- GDP 当季同比
- CPI 全国同比
- PPI 全部工业品同比
- M2 同比
- Shibor 1Y
- LPR 5Y

指标卡显示：

- 最新值
- 最新发布日期
- 较上一期变化

### 4.2 增长与通胀

- GDP 累计值折线
- GDP 同比折线
- CPI/PPI 同比双线对比

### 4.3 流动性与利率

- M2 余额折线
- M2 同比折线
- Shibor 多期限曲线
- LPR 1Y/5Y 双线

### 4.4 原始明细

支持在当前时间窗口下切换数据集查看底层记录，便于核数。

## 5. 实施步骤

1. 扩展同步脚本，支持 6 个宏观数据集
2. 扩展标准化视图定义
3. 在查询层新增宏观数据读取函数
4. 在 `app.py` 中增加缓存入口与新 Tab
5. 执行同步命令验证可落库
6. 启动 Streamlit 检查页面展示

## 6. 已知限制

- 当前首版未纳入 PMI、社融等指标，因为未在本轮中找到稳定可复用的 Tushare直连接口
- 季频、月频、日频混合展示时，最新日期不一定一致，页面会分别标注最新发布日期
- 若数据库尚未同步宏观表，页面会提示先执行同步脚本

## 7. 后续扩展

- 增加 PMI、社融、外汇储备等前瞻指标
- 增加 “风险偏好” 派生面板，例如 `CPI-PPI`、`Shibor-LPR` 利差
- 支持将宏观状态与宽基 ETF 流入流出放在同一页联动分析
