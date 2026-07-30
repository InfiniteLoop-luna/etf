# 2026-07-30 资金链 freshness summary

## 目标
把资金相关多条 nightly 数据链的“最新交易日”统一可见化，减少某条链静默落后但总任务仍显示成功的问题。

## 新增内容
### `scripts/funding_freshness_summary.py`
- 统一按北京时间（Asia/Shanghai）生成 summary
- 当前检查的数据项：
  - `etf_share_size`
  - `etf_category_agg`
  - `moneyflow`
  - `hotmoney_detail`
  - `lhb`
  - `limitup`
  - `margin_detail`
- 输出：
  - stdout JSON
  - `data/funding_freshness_summary.json`
- 返回码：
  - 全部达到目标日期（北京时间昨天）→ `0`
  - 任一项落后 → `2`

### 接入 nightly
在 `scripts/etf-data-update.sh` 中新增：
- `funding freshness summary`
- 若 summary 返回非 0，则记录 warning，明确指出还有 stale data

## 本轮验证
### 第一次 summary
发现：
- `margin_detail` 停在 `20260728`
- 其他资金链均已到 `20260729`

这证明 summary 已经开始替系统主动暴露“下一个静默落后点”。

### 补跑融资融券后
执行：
- `update_margin.py --datasets margin,margin_detail --lookback-days 2`
结果：
- `margin`: 9 行
- `margin_detail`: 13252 行
- 最新日期推进到 `20260729`

### 第二次 summary
结果：
- `all_ok = true`
- 所有检查项都达到目标日期 `20260729`

## 当前收益
- 不再只靠打开页面时人工发现哪个板块停了
- nightly 结束时能统一看到多条资金链最新日期
- 这套 summary 还能继续扩展到更多数据源
