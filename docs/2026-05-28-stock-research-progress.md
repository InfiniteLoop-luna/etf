# 2026-05-28 自选股个股深度研究落地记录

本文记录 2026-05-28 围绕“把个股深度研究集成到 ETF 项目”的主要工作，方便后续继续迭代和排查线上行为。

## 背景

目标不是简单搬运 `InfiniteLoop-luna/stock-analysis`，而是把个股深度研究能力嵌入当前 ETF 项目的自选股体系：

- 个股深度研究只针对自选股。
- 报告生成时间较长，必须走后台定时任务，不阻塞 Streamlit 页面。
- 报告必须接入 LLM 深度分析，否则只有数据堆叠，研究价值不足。
- LLM 使用 DeepSeek OpenAI-compatible API，配置放在环境变量或 `.env`，不进 Git。
- 参考已有“主力出货深度分析”的缓存/后台刷新模式。

## 第一阶段：自选股深度研究后台缓存

第一阶段完成了自选股级别的个股研究报告闭环：

- 新增 `src/stock_research_fact_pack.py`：从本地数据库构建个股研究 FactPack。
- 新增 `src/stock_research_llm_analysis.py`：封装 DeepSeek 配置、调用、JSON 结果规整、Markdown 渲染。
- 新增 `src/stock_research_analyzer.py`：组合 FactPack、LLM、Markdown 报告。
- 新增 `src/stock_research_report_store.py`：持久化报告、状态、刷新锁。
- 新增 `src/watchlist_stock_research_refresh.py`：按自选股增量刷新报告。
- 新增 `scripts/update_watchlist_stock_research_reports.py`：后台脚本入口。
- 更新 `scripts/etf-data-update.sh`：夜间任务中追加个股深度研究刷新。
- 更新 `app.py`：自选管理页展示个股研究报告状态和入口。

报告表核心数据：

- `ts_stock_research_reports`
- `ts_stock_research_report_status`
- `ts_stock_research_refresh_locks`

第一阶段提交：

- `44c0088 feat: add watchlist stock research reports`
- `22f4a23 fix: avoid unexpected kwargs when generating stock research bundle`
- `ec4f112 chore: switch DeepSeek model defaults to deepseek-v4-flash`

## 第二阶段第一刀：可视化 HTML 报告

第一刀目标是让报告从 Markdown 升级为可视化 HTML，提升可读性和传播性。

主要改动：

- 新增 `src/stock_research_html_renderer.py`
- `ts_stock_research_reports` 新增 `report_html`
- 后台生成报告时同时保存 Markdown 和 HTML
- 自选股页面弹窗优先展示 HTML，并支持下载 HTML
- 旧缓存如果已有 `fact_pack_json` 和 `llm_result_json`，可以补渲染 HTML，不重复调用 LLM

HTML 报告包含：

- 顶部结论、风险等级、置信度、质地评分
- K 线走势
- 公司与估值快照
- 核心投资命题
- 估值与赔率
- 位置与节奏
- 关键证据、主要风险、后续跟踪清单
- Step 0-8 分析框架
- 财务与数据质量

提交：

- `b1d176f feat: add stock research html reports`

## DeepSeek 模型配置调整

曾短暂使用 `deepseek-chat` 作为默认模型；后续根据项目判断，统一改为 `deepseek-v4-flash`。

当前约定：

```bash
DISTRIBUTION_LLM_MODEL=deepseek-v4-flash
STOCK_RESEARCH_LLM_MODEL=deepseek-v4-flash
```

配置位置：

- 本地：`D:\sourcecode\etf\.env`
- VPS：`/opt/etf-app/.env`
- `.env` 已被 `.gitignore` 忽略，不提交 API key

提交：

- `2f7b5e7 chore: use deepseek v4 pro model`

## 第二阶段第二刀：FactPack v2 + AkShare 补充证据

第二刀目标是把报告输入从“本地行情/财务底稿”升级为“本地底稿 + 补充证据层”。

主要改动：

- 新增 `src/stock_research_akshare_enrichment.py`
- `stock_research_fact_pack.py` schema 升级为 `stock-research-fact-pack-v2`
- FactPack 新增 `supplemental`
- LLM prompt 明确要求只在补充数据块 `status=ok` 时引用该块证据
- HTML 报告新增“补充证据层”
- 后台启用 AkShare 后，会自动刷新旧 v1 或未启用补充证据的 v2 缓存

当前 `supplemental` 数据块：

- `business_composition`：主营构成
- `news`：近期新闻
- `research_reports`：机构研报
- `money_flow`：资金流
- `lhb`：龙虎榜
- `industry_peer_hint`：行业成分参考

每个数据块统一结构：

```python
{
    "name": "...",
    "source": "...",
    "status": "ok | empty | failed | disabled | missing",
    "updated_at": "...",
    "row_count": 0,
    "items": [],
    "error": None,
    "meta": {}
}
```

AkShare 失败不会中断报告生成，只会记录为 `failed`，并进入数据质量说明。

VPS 当前已启用：

```bash
STOCK_RESEARCH_ENABLE_AKSHARE=true
STOCK_RESEARCH_NEWS_LIMIT=8
STOCK_RESEARCH_REPORT_LIMIT=6
STOCK_RESEARCH_MONEY_FLOW_LIMIT=8
STOCK_RESEARCH_LHB_LIMIT=6
```

提交：

- `416c751 feat: enrich stock research fact packs`

## 验证记录

本地验证：

- 编译检查通过：
  - `app.py`
  - `src/stock_research_akshare_enrichment.py`
  - `src/stock_research_fact_pack.py`
  - `src/stock_research_html_renderer.py`
  - `src/watchlist_stock_research_refresh.py`
  - `scripts/update_watchlist_stock_research_reports.py`
- 单测通过：`32 passed`

真实 AkShare smoke 结果：

- 主营构成：可返回
- 机构研报：可返回
- 龙虎榜：可返回
- 新闻：出现过上游正则错误，已降级为 `failed`
- 资金流：出现过上游连接中断，已降级为 `failed`

VPS 验证：

- 代码已拉到 `/opt/etf-app`
- 当前线上提交：`416c751`
- `etf-streamlit` 服务状态：`active`
- 远端编译检查通过
- 远端配置读取确认：
  - `stock_research_model=deepseek-v4-flash`
  - `akshare_enabled=True`
- 远端无 `pytest`，用 `unittest` 跑新增测试通过

## 当前线上行为

页面行为：

- 自选管理页只展示缓存状态，不实时生成报告。
- 点击“个股深度研究”后，若有 HTML 报告，弹窗展示可视化报告。
- 支持下载 HTML。
- 若报告尚未生成，按钮保持不可用，等待后台定时任务。

后台行为：

- 夜间任务执行 `scripts/update_watchlist_stock_research_reports.py`
- 仅处理自选股并集
- 用刷新锁避免并发重复跑
- 若缓存已经是最新且完整，则跳过
- 若启用 AkShare 且旧缓存没有补充证据，会重新生成

## 注意事项

- 不要把 DeepSeek API key 提交到 Git。
- `.env` 是线上/本地私有配置，已在 `.gitignore`。
- AkShare 接口稳定性不完全可控，所有补充块必须允许 `failed/empty`。
- 页面不要直接调用 AkShare 或 LLM，避免 Streamlit 阻塞。
- HTML 是确定性模板渲染，LLM 只输出结构化分析结果，不让 LLM 直接写 HTML。

## 下一步建议

下一步优先做线上验收和可控回填：

1. 给 `scripts/update_watchlist_stock_research_reports.py` 增加 `--limit`、`--only-code`、`--force` 参数。
2. 在 VPS 上先回填 1-3 只自选股，检查真实报告质量。
3. 查看 LLM 是否真的引用了 `supplemental` 证据。
4. 记录 AkShare 各数据块成功率，决定是否需要替换不稳定接口。
5. 自选股页面增加更细状态：`已生成 / AkShare部分失败 / LLM失败 / 数据缺口`。

多轮 LLM 深度分析建议放在后续阶段。先把证据层、调度和质量观测做稳，再扩大模型调用复杂度。
