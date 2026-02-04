# ETF份额变动可视化系统设计文档

**日期**: 2026-02-04
**状态**: 已验证

## 1. 概述

开发一个基于Web的ETF份额变动可视化系统，使用Python 3、Pandas、Streamlit和Plotly技术栈，从现有Excel文件读取数据并提供交互式可视化分析。

## 2. 技术栈

- **Python 3**: 核心开发语言
- **Pandas**: 数据处理和转换
- **Streamlit**: Web应用框架
- **Plotly**: 交互式图表库
- **openpyxl**: Excel文件读取

## 3. 数据源

### 3.1 Excel文件结构

- **文件名**: `主要ETF基金份额变动情况.xlsx`
- **表头结构**: 双行表头
  - 第1行: 包含"代码"、"名称"及重复的指标名（如：总市值：亿元）
  - 第2行: 具体的日期（YYYY-MM-DD格式）
- **数据内容**: 从第3行开始，每行是一个ETF的具体数值
- **特殊行**: "总市值"section包含所有ETF的总和行

### 3.2 Section结构

Excel文件包含多个section，每个section有：
- Section标题行（如"总市值：亿元"）
- 日期行
- 数据行（多个ETF）

## 4. 系统架构

### 4.1 文件结构

```
etf/
├── app.py                          # 主Streamlit应用
├── src/
│   ├── data_loader.py             # Excel解析和数据转换
│   ├── excel_manager.py           # (现有文件，复用section检测逻辑)
│   └── ...                        # (其他现有文件)
├── 主要ETF基金份额变动情况.xlsx    # 数据源
├── docs/
│   └── plans/                     # 设计文档目录
└── requirements.txt               # 依赖项
```

### 4.2 核心组件

#### 4.2.1 Data Loader (`src/data_loader.py`)

**职责**:
- 解析Excel文件的双行表头结构
- 自动检测所有section（复用`DynamicExcelManager`的section检测逻辑）
- 将宽表转换为长表格式
- 处理特殊的"总市值总和"聚合行
- 缓存转换后的数据

**输出数据格式**:
```python
columns = ['code', 'name', 'date', 'metric_type', 'value', 'is_aggregate']
# 示例:
# 'SH510300', '沪深300ETF', '2026-01-15', '总市值：亿元', 1234.56, False
# 'ALL', '所有ETF', '2026-01-15', '总市值：亿元', 9999.99, True
```

**缓存策略**:
```python
@st.cache_data(ttl=300)  # 缓存5分钟
def load_etf_data(file_path: str) -> pd.DataFrame:
    # ... 解析逻辑
    return df
```

#### 4.2.2 Streamlit App (`app.py`)

**职责**:
- 提供Web界面
- 处理用户交互
- 调用数据加载和可视化功能
- 显示统计信息

## 5. 功能需求

### 5.1 自动数据清洗

- 自动处理双行表头
- 将宽表转换为长表（字段：代码, 名称, 日期, 指标类型, 数值, 是否聚合）
- 处理缺失值（删除value为空的行）

### 5.2 Web交互界面

#### 5.2.1 侧边栏

**1. 指标选择器**
- 类型: 下拉选择框
- 选项: 自动检测Excel中的所有section
- 示例: ['总市值：亿元', '基金单位市值', '份额变动', ...]

**2. ETF多选器（智能行为）**
- 当选择聚合指标（如"总市值"）时:
  - 隐藏或禁用ETF选择器
  - 显示提示信息："📊 当前显示所有ETF的总和"
- 当选择非聚合指标时:
  - 显示ETF多选框
  - 支持通过名称搜索
  - 默认选择前3个ETF

**3. 日期范围滑块**
- 类型: 日期范围滑块
- 范围: 根据数据自动确定最小和最大日期
- 默认: 显示全部日期范围
- 格式: YYYY-MM-DD

#### 5.2.2 主区域

**布局**: 3:1 两列布局

**左列（图表区）**:
- Plotly交互式折线图
- 全宽显示

**右列（统计区）**:
- 统计信息表格

### 5.3 图表功能

#### 5.3.1 Plotly折线图特性

- **交互功能**:
  - 鼠标悬停查看详细数值
  - 双击重置缩放
  - 拖拽缩放特定区域
  - 内置保存为PNG功能

- **显示模式**:
  - 聚合模式: 单条粗线显示所有ETF总和
  - 个体模式: 多条线显示选中的各个ETF

- **样式配置**:
  - 线条 + 标记点
  - 统一的悬停模式（x轴对齐）
  - 水平图例（位于图表上方）
  - 高度: 600px
  - 响应式宽度

#### 5.3.2 图表实现

```python
import plotly.graph_objects as go

def create_line_chart(filtered_df, metric_name, is_aggregate):
    fig = go.Figure()

    if is_aggregate:
        # 聚合数据的单条线
        agg_data = filtered_df[filtered_df['is_aggregate'] == True]
        fig.add_trace(go.Scatter(
            x=agg_data['date'],
            y=agg_data['value'],
            mode='lines+markers',
            name='所有ETF总和',
            line=dict(width=3),
            hovertemplate='<b>%{x}</b><br>%{y:.2f}<extra></extra>'
        ))
    else:
        # 个体ETF的多条线
        for etf_name in selected_etfs:
            etf_data = filtered_df[filtered_df['name'] == etf_name]
            fig.add_trace(go.Scatter(
                x=etf_data['date'],
                y=etf_data['value'],
                mode='lines+markers',
                name=etf_name,
                hovertemplate=f'<b>{etf_name}</b><br>%{{x}}<br>%{{y:.4f}}<extra></extra>'
            ))

    fig.update_layout(
        title=f'{metric_name} 变动趋势',
        xaxis_title='日期',
        yaxis_title=metric_name,
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        height=600
    )

    fig.update_xaxes(rangeslider_visible=False)

    return fig
```

### 5.4 统计分析

#### 5.4.1 统计指标

对每个选中的ETF（或聚合数据），在所选日期范围内计算：

- **期初值**: 日期范围内第一个数据点的值
- **期末值**: 日期范围内最后一个数据点的值
- **涨跌幅**: `((期末值 - 期初值) / 期初值) * 100%`
- **最大值**: 日期范围内的最大值
- **最小值**: 日期范围内的最小值

#### 5.4.2 显示格式

- 表格形式显示
- 列: ETF名称, 期初值, 期末值, 涨跌幅, 最大值, 最小值
- 数值格式:
  - 亿元单位: 保留2位小数
  - 元单位: 保留4位小数
  - 涨跌幅: 保留2位小数，带正负号

#### 5.4.3 实现代码

```python
def calculate_statistics(filtered_df, selected_etfs, date_range):
    """计算每个选中ETF的基本统计信息"""
    stats_list = []

    for etf_name in selected_etfs:
        etf_data = filtered_df[
            (filtered_df['name'] == etf_name) &
            (filtered_df['date'] >= date_range[0]) &
            (filtered_df['date'] <= date_range[1])
        ].sort_values('date')

        if len(etf_data) == 0:
            continue

        start_value = etf_data.iloc[0]['value']
        end_value = etf_data.iloc[-1]['value']
        max_value = etf_data['value'].max()
        min_value = etf_data['value'].min()
        change_pct = ((end_value - start_value) / start_value * 100) if start_value != 0 else 0

        stats_list.append({
            'ETF名称': etf_name,
            '期初值': f'{start_value:.4f}',
            '期末值': f'{end_value:.4f}',
            '涨跌幅': f'{change_pct:+.2f}%',
            '最大值': f'{max_value:.4f}',
            '最小值': f'{min_value:.4f}'
        })

    return pd.DataFrame(stats_list)
```

## 6. 错误处理和边界情况

### 6.1 错误处理

```python
# 1. 文件未找到
try:
    df = load_etf_data('主要ETF基金份额变动情况.xlsx')
except FileNotFoundError:
    st.error("❌ 未找到Excel文件：主要ETF基金份额变动情况.xlsx")
    st.stop()

# 2. 筛选后数据为空
if len(filtered_df) == 0:
    st.warning("⚠️ 所选条件下没有数据，请调整筛选条件")
    st.stop()

# 3. 数据中的缺失值
df = df.dropna(subset=['value'])  # 删除value为空的行

# 4. 无效的日期范围
if date_range[0] > date_range[1]:
    st.error("❌ 开始日期不能晚于结束日期")
    st.stop()

# 5. 未选择ETF（非聚合指标）
if not is_aggregate and len(selected_etfs) == 0:
    st.info("ℹ️ 请至少选择一个ETF")
    st.stop()
```

### 6.2 边界情况

- **Excel结构变化**: 优雅处理section的增加/删除
- **单个数据点**: 仅显示标记点（无连线）
- **超大日期范围**: 使用Plotly的内置优化
- **非交易日**: 按原样显示（折线图中出现间隙）

### 6.3 性能考虑

- 缓存解析后的数据（5分钟TTL）
- 默认ETF选择限制为3-5个，避免图表过于拥挤
- 利用Plotly对大数据集的内置优化

## 7. 数据流

```
Excel文件
  ↓
解析表头（检测section）
  ↓
转换为长表格式
  ↓
缓存（5分钟）
  ↓
根据用户选择筛选
  ↓
可视化（图表 + 统计）
```

## 8. UI布局示例

```
┌─────────────────────────────────────────────────────────────┐
│  ETF基金份额变动可视化系统                                    │
├──────────────┬──────────────────────────────────────────────┤
│ 数据筛选     │                                              │
│              │                                              │
│ 选择指标:    │         Plotly 折线图                        │
│ [总市值▼]    │         (交互式，可缩放)                      │
│              │                                              │
│ 📊 当前显示  │                                              │
│ 所有ETF总和  │                                              │
│              │                                              │
│ 选择日期范围: │                                              │
│ [========]   │                                              │
│              │                                              │
│              ├──────────────────────────────────────────────┤
│              │  统计信息                                    │
│              │  ┌────────────────────────────────────────┐ │
│              │  │ ETF名称 │ 期初值 │ 期末值 │ 涨跌幅 │...│ │
│              │  ├────────────────────────────────────────┤ │
│              │  │ ...     │ ...    │ ...    │ ...    │...│ │
│              │  └────────────────────────────────────────┘ │
└──────────────┴──────────────────────────────────────────────┘
```

## 9. 依赖项

```txt
streamlit>=1.30.0
pandas>=2.0.0
plotly>=5.18.0
openpyxl>=3.1.0
```

## 10. 实现优先级

1. **Phase 1 - 核心功能**:
   - 数据加载和转换（`data_loader.py`）
   - 基本UI布局（侧边栏 + 主区域）
   - 简单折线图显示

2. **Phase 2 - 交互功能**:
   - 指标选择器
   - ETF多选器（智能行为）
   - 日期范围筛选

3. **Phase 3 - 统计和优化**:
   - 统计信息计算和显示
   - 错误处理
   - 性能优化（缓存）

## 11. 测试要点

- Excel文件解析正确性
- 双行表头处理
- 聚合数据识别
- 日期范围筛选
- 统计计算准确性
- 边界情况处理
- 性能（大数据集）

## 12. 未来扩展

- 支持多个Excel文件
- 导出图表和统计数据
- 自定义图表样式
- 数据对比功能
- 移动端适配
