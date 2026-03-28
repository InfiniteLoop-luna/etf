# ETF份额变动可视化系统

基于Streamlit的Web可视化应用，用于展示ETF基金份额变动情况。

## 功能特性

- 📊 **自动数据加载**: 从Excel文件自动解析双行表头和多个section
- 🎯 **智能指标选择**: 自动检测所有可用指标
- 🔄 **智能显示模式**: 总市值显示所有ETF的总和，其他指标显示选中的个体ETF
- 📈 **交互式图表**: 鼠标悬停查看详情、双击重置缩放、拖拽缩放特定区域、保存为PNG图片
- 📊 **统计分析**: 显示期初值、期末值、涨跌幅、最大值、最小值
- 🔍 **灵活筛选**: 按指标类型、ETF名称、日期范围筛选

## 安装依赖

```bash
pip install -r requirements.txt
```

## 运行应用

```bash
streamlit run app.py
```

应用将在浏览器中自动打开，默认地址: http://localhost:8501

## 使用说明

1. **选择指标**: 在侧边栏选择要查看的指标类型
2. **选择ETF**: 如果是总市值则自动显示汇总，其他指标可多选ETF
3. **选择日期范围**: 使用滑块选择时间段
4. **查看结果**: 主区域显示交互式折线图和统计信息表格

## 技术架构

- **Streamlit**: Web应用框架
- **Pandas**: 数据处理
- **Plotly**: 交互式图表
- **openpyxl**: Excel文件读取

## 本次数据修复与巡检

### 已处理的问题

- 修复了 `etf_share_size` 历史数据中因 Tushare `etf_share_size` 接口单次返回上限导致的周期性截断问题。
- 已回补 2024-11-18 之后发现的异常交易日，并重跑对应的 `etf_category_daily_agg` 聚合数据。
- 已确认数据库明细与 Tushare 单日结果重新对齐，历史周期性异常已清除。

### 代码侧重点

- `src/fetch_etf_share_size.py`
  - 默认按单日批次抓取，避免 7 天批次触发接口截断。
  - 抓取完成后自动调用分类聚合，并把日期范围传递给聚合脚本。
  - 增加自动巡检：按交易日对比 Tushare、`etf_share_size`、`etf_category_daily_agg` 三方汇总。
  - 增加 `--verify-only` 和 `--skip-verify` 参数，支持单独巡检或跳过巡检。
  - 数据写入改为 `ON CONFLICT DO UPDATE`，便于重跑时刷新错误历史数据。
- `src/aggregate_etf_categories.py`
  - 支持按日期范围增量聚合，便于异常日期回补后只重算受影响区间。

### 常用命令

```bash
python src/fetch_etf_share_size.py
python src/fetch_etf_share_size.py --start-date 20260326 --end-date 20260326
python src/fetch_etf_share_size.py --verify-only --start-date 20260326 --end-date 20260326
python src/aggregate_etf_categories.py --start-date 20260326 --end-date 20260326
```

## 性能优化

- 数据缓存5分钟，避免重复读取Excel
- 自动刷新机制

## 故障排除

- **找不到Excel文件**: 确保文件在运行目录下
- **没有数据显示**: 检查筛选条件和日期范围
- **图表显示异常**: 刷新页面或清除缓存

