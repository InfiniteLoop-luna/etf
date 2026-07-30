# 2026-07-30 游资 nightly 硬化

## 背景
- 用户反馈：资金板块的游资数据停留在 2026-07-24。
- 用户明确要求不要只临时补数，而是根据日志与脚本行为彻底修复 recurring 问题。

## 已确认根因
### 1. 不是页面缓存
- `src.hotmoney_monitor.get_hotmoney_latest_detail_date()` 直接读取 `ts_hm_detail` 最大日期。
- 手工补跑前，游资数据停留是 DB 明细表未推进，不是前端展示问题。

### 2. nightly 对 `hm_detail` 的错误处理过于“静默”
- `scripts/etf-data-update.sh` 中：
  - `python update_hotmoney.py --datasets hm_detail ...`
  - 失败后仅打印 warning，然后继续整条 nightly
- 结果：整个 nightly 表面 `SUCCESS`，但游资明细可能实际没更新。

### 3. `update_hotmoney.py` / `src.hotmoney_sync.py` 之前缺少“限频即显式失败”的出口
- 旧行为：`hm_detail` 遇到 rate limit 时，在内部记 warning/提前停止，但上层仍可能以“正常完成”收尾。
- 这会让问题长期潜伏，不容易从总任务状态里看出来。

### 4. 默认 nightly 参数过于保守
- 旧默认：`--detail-batch-days 1`
- 如果某天因为限频/断连漏掉，后续一次 nightly 只能追一天，补齐速度很慢。
- 这正是“落后几天后一直追不回来”的结构性原因。

## 本轮已做修复
### A. 手工补数验证
手工执行：
- `update_hotmoney.py --datasets hm_detail --detail-batch-days 10 --detail-sleep 1 --detail-lookback-days 0 --detail-max-days 10`
结果：
- `20260727` 写入 276 行
- `20260728` 写入 297 行
- `20260729` 写入 275 行
- 总计 848 行
- 最新日期推进到 `20260729`

### B. `src/hotmoney_sync.py` 硬化
- 新增 `ZoneInfo('Asia/Shanghai')`
- 新增 `beijing_today_ymd()`，避免“今天”跟随服务器本地时区
- `sync_hm_detail()` 返回结构化结果，而不是仅返回写入行数：
  - `rows_written`
  - `start_date`
  - `requested_end_date`
  - `hard_end_date`
  - `processed_days`
  - `rate_limited`
  - `last_success_trade_date`
- 这样上层可以明确知道本轮是不是被限频截断。

### C. `update_hotmoney.py` 硬化
- 如果 `hm_detail` 结果中 `rate_limited=True`
- 则显式输出 warning 并 `SystemExit(2)`
- 这样 nightly 日志里会出现明确失败，而不是“静默成功”

### D. nightly 参数加固（`scripts/etf-data-update.sh`）
将游资明细任务从：
- `batch-days=1`
改为可配置、默认更积极：
- `ETF_HM_DETAIL_BATCH_DAYS:-10`
- `ETF_HM_DETAIL_SLEEP_SECONDS:-35`
- `ETF_HM_DETAIL_LOOKBACK_DAYS:-0`
- `ETF_HM_DETAIL_MAX_DAYS:-10`

目的：
- 即使某次漏了，也能在后续 nightly 中自动追最近 10 天窗口
- 不再只能“一天一天慢慢追”

## 当前结果
- `get_hotmoney_latest_detail_date()` 当前已是：`20260729`
- `get_hotmoney_sync_meta()` 当前 `latest_trade_date` 也是：`20260729`
- 说明线上游资明细 DB 已从 `20260724` 补到 `20260729`

## 预期改进
后续再出现以下情况时，会更容易被发现和恢复：
1. **限频**：不再伪装成完全成功
2. **漏跑几天**：nightly 默认会一次追更大窗口，而不是只追 1 天
3. **时区偏差**：游资“今天”按北京时间

## 仍可继续加强的点
- 增加一个专门的 `ensure_recent_hotmoney_data.sh`
  - 对照“北京时间最近交易日”检查 `ts_hm_detail` 最大日期
  - 若落后则二次补跑
- 再往上加 cron / alert（例如连续 2 次 rate limit 就提醒）
