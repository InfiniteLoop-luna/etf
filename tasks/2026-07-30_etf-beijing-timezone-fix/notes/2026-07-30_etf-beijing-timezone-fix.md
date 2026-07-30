# 2026-07-30 ETF 北京时区修正

## 背景
- 用户明确要求：任何时间判断都按北京时间（Asia/Shanghai）。
- 本次 ETF 板块数据停留在 2026-07-28 的排查中，已确认一部分链路仍受服务器本地时区（America/New_York）影响。

## 已确认问题
1. `src/fetch_etf_share_size.py`
   - `resolve_fetch_range()` 使用 `datetime.now().strftime('%Y%m%d')`
   - `verify_only + full` 分支也使用服务器本地 `datetime.now()`
   - 这会让 ETF 份额抓取的“今天”跟随服务器时区，而不是北京时间。

2. `scripts/ensure_recent_etf_share_data.sh`
   - 虽然 `TODAY_SH` / `YESTERDAY_SH` 已按 `TZ=Asia/Shanghai` 计算
   - 但判断条件是“最新数据是否追到 **今天**”
   - 对 20:00 北京时间的 nightly 来说，更稳的目标应是至少追到 **北京时间昨天**，避免把尚未稳定可取的“今天”当成必须目标。

## 已完成修改（本地工作区）
### `src/fetch_etf_share_size.py`
- 新增 `ZoneInfo('Asia/Shanghai')`
- 新增 `beijing_now()`
- 将以下日期判断改为北京时间：
  - `resolve_fetch_range()` 的 `today`
  - `verify_only + full` 分支的 `end_date`

### `scripts/ensure_recent_etf_share_data.sh`
- 新增 `TARGET_SH=$YESTERDAY_SH`
- 将补跑判断从“追到 today”改为“追到 target（北京时间昨天）”
- 日志文案同步改为 `Beijing target date`

## 本轮已做的线上补救
- 已在服务器手动补跑 2026-07-29 的 ETF 份额抓取与分类聚合
- 结果：
  - ETF 份额明细补写入 1612 条
  - 分类聚合 21 行
  - 宽基聚合 13 行
  - 行业聚合 373 行
- 这样线上 ETF 板块数据已从 2026-07-28 补到 2026-07-29

## 后续建议
- 将上述本地修正同步到 VPS `/opt/etf-app`
- 重新执行一次最小验证：
  1. `python src/fetch_etf_share_size.py --start-date 20260729 --end-date 20260729 --skip-verify`
  2. `bash scripts/ensure_recent_etf_share_data.sh`
  3. 核对 `etf_share_size` 与三张聚合表最大日期
- 如无异常，再按正常 deploy 流程提交/推送/部署
