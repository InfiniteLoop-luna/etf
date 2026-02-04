# ETF份额变动可视化系统 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a web-based ETF share change visualization system using Streamlit, Pandas, and Plotly that reads from the existing Excel file and provides interactive charts with statistics.

**Architecture:** Single Streamlit app with a data loader module that parses the Excel file's double-header structure, transforms it to long format, and caches the result. The UI has smart behavior: shows aggregate sum for "总市值" and individual ETF lines for other metrics.

**Tech Stack:** Python 3, Streamlit, Pandas, Plotly, openpyxl

---

## Task 1: Update requirements.txt with new dependencies

**Files:**
- Modify: `requirements.txt`

**Step 1: Add Streamlit and Plotly dependencies**

Add these lines to requirements.txt:
```
streamlit>=1.30.0
plotly>=5.18.0
```

**Step 2: Install new dependencies**

Run: `pip install -r requirements.txt`
Expected: Streamlit and Plotly installed successfully

**Step 3: Commit**

```bash
git add requirements.txt
git commit -m "feat: add Streamlit and Plotly dependencies for visualization"
```

---

## Task 2: Create data loader module with Excel parsing

**Files:**
- Create: `src/data_loader.py`
- Test: Manual verification (will add proper tests later)

**Step 1: Create data_loader.py with basic structure**

Create `src/data_loader.py`:
```python
"""Data loader for ETF visualization - parses Excel and transforms to long format"""

import pandas as pd
import openpyxl
from typing import Dict, List, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def load_etf_data(file_path: str) -> pd.DataFrame:
    """
    Load and transform ETF data from Excel file.

    Args:
        file_path: Path to Excel file

    Returns:
        DataFrame with columns: code, name, date, metric_type, value, is_aggregate
    """
    wb = openpyxl.load_workbook(file_path)
    ws = wb.active

    # Detect sections and parse data
    sections = _detect_sections(ws)
    data_rows = []

    for section_name, section_info in sections.items():
        rows = _parse_section(ws, section_name, section_info)
        data_rows.extend(rows)

    df = pd.DataFrame(data_rows, columns=['code', 'name', 'date', 'metric_type', 'value', 'is_aggregate'])

    # Clean data
    df = df.dropna(subset=['value'])
    df['date'] = pd.to_datetime(df['date'])
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna(subset=['value'])

    logger.info(f"Loaded {len(df)} data points from {len(sections)} sections")

    return df
```

**Step 2: Implement section detection**

Add to `src/data_loader.py`:
```python
def _detect_sections(ws) -> Dict[str, Dict]:
    """
    Detect all sections in the Excel file.

    Returns:
        Dict mapping section name to {header_row, date_row, data_start, data_end}
    """
    sections = {}
    CODE_COL = 1
    NAME_COL = 2

    # Keywords that identify section headers
    keywords = ['市值', '份额', '变动', '申赎', '比例', '涨跌幅']

    current_section = None

    for row_idx in range(1, ws.max_row + 1):
        code_cell = ws.cell(row_idx, CODE_COL).value
        name_cell = ws.cell(row_idx, NAME_COL).value

        # Check if this is a section header
        is_header = (code_cell is None and
                    name_cell and
                    isinstance(name_cell, str) and
                    any(kw in name_cell for kw in keywords))

        if is_header:
            # Save previous section
            if current_section:
                current_section['data_end'] = row_idx - 1
                sections[current_section['name']] = current_section

            # Start new section
            current_section = {
                'name': name_cell,
                'header_row': row_idx,
                'date_row': row_idx + 1,
                'data_start': row_idx + 2,
                'data_end': None
            }

    # Save last section
    if current_section:
        current_section['data_end'] = ws.max_row
        sections[current_section['name']] = current_section

    return sections
```

**Step 3: Implement section parsing**

Add to `src/data_loader.py`:
```python
def _parse_section(ws, section_name: str, section_info: Dict) -> List[Tuple]:
    """
    Parse a single section and return data rows.

    Returns:
        List of tuples: (code, name, date, metric_type, value, is_aggregate)
    """
    rows = []
    CODE_COL = 1
    NAME_COL = 2
    DATA_START_COL = 3

    date_row = section_info['date_row']
    data_start = section_info['data_start']
    data_end = section_info['data_end']

    # Get all dates from the date row
    dates = []
    for col in range(DATA_START_COL, ws.max_column + 1):
        date_val = ws.cell(date_row, col).value
        if date_val:
            if isinstance(date_val, datetime):
                dates.append((col, date_val.strftime("%Y-%m-%d")))
            elif isinstance(date_val, str):
                # Normalize date format
                normalized = date_val.replace('/', '-')
                dates.append((col, normalized))

    # Parse data rows
    for row in range(data_start, data_end + 1):
        code = ws.cell(row, CODE_COL).value
        name = ws.cell(row, NAME_COL).value

        if not code or not name:
            continue

        # Check if this is an aggregate row (总计, 合计, etc.)
        is_aggregate = isinstance(name, str) and any(kw in name for kw in ['总计', '合计', '总和'])

        # For aggregate rows, use special code
        if is_aggregate:
            code = 'ALL'

        # Get values for each date
        for col, date_str in dates:
            value = ws.cell(row, col).value
            if value is not None:
                rows.append((code, name, date_str, section_name, value, is_aggregate))

    return rows
```

**Step 4: Test data loader manually**

Create a test script `test_loader.py`:
```python
from src.data_loader import load_etf_data

df = load_etf_data('主要ETF基金份额变动情况.xlsx')
print(f"Loaded {len(df)} rows")
print(f"Columns: {df.columns.tolist()}")
print(f"Metrics: {df['metric_type'].unique()}")
print(f"Date range: {df['date'].min()} to {df['date'].max()}")
print(f"\nSample data:")
print(df.head(10))
print(f"\nAggregate data:")
print(df[df['is_aggregate'] == True].head())
```

Run: `python test_loader.py`
Expected: Should print data summary without errors

**Step 5: Commit**

```bash
git add src/data_loader.py test_loader.py
git commit -m "feat: add Excel data loader with section detection and parsing"
```

---

## Task 3: Create basic Streamlit app structure

**Files:**
- Create: `app.py`

**Step 1: Create basic app.py with layout**

Create `app.py`:
```python
"""ETF份额变动可视化系统 - Main Streamlit App"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.data_loader import load_etf_data
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="ETF份额变动可视化",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("📊 ETF基金份额变动可视化系统")

# Load data with caching
@st.cache_data(ttl=300)
def get_data():
    """Load and cache ETF data for 5 minutes"""
    try:
        return load_etf_data('主要ETF基金份额变动情况.xlsx')
    except FileNotFoundError:
        st.error("❌ 未找到Excel文件：主要ETF基金份额变动情况.xlsx")
        st.stop()
    except Exception as e:
        st.error(f"❌ 加载数据失败: {e}")
        st.stop()

# Load data
df = get_data()

# Sidebar
with st.sidebar:
    st.header("🔍 数据筛选")

    # Metric selector
    all_metrics = sorted(df['metric_type'].unique())
    selected_metric = st.selectbox(
        "选择指标",
        options=all_metrics,
        index=0
    )

    # Check if this is an aggregate metric
    metric_data = df[df['metric_type'] == selected_metric]
    has_aggregate = metric_data['is_aggregate'].any()

    # ETF selector (smart behavior)
    if has_aggregate and '总市值' in selected_metric:
        st.info("📊 当前显示所有ETF的总和")
        selected_etfs = None
    else:
        # Get non-aggregate ETFs
        non_agg_etfs = metric_data[metric_data['is_aggregate'] == False]['name'].unique()
        selected_etfs = st.multiselect(
            "选择ETF",
            options=sorted(non_agg_etfs),
            default=sorted(non_agg_etfs)[:3] if len(non_agg_etfs) > 0 else [],
            help="支持搜索ETF名称"
        )

    # Date range slider
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    date_range = st.slider(
        "选择日期范围",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY-MM-DD"
    )

# Main area - placeholder for now
st.write("Chart and statistics will go here")
st.write(f"Selected metric: {selected_metric}")
st.write(f"Date range: {date_range[0]} to {date_range[1]}")
if selected_etfs:
    st.write(f"Selected ETFs: {len(selected_etfs)}")
```

**Step 2: Test the basic app**

Run: `streamlit run app.py`
Expected: App opens in browser, shows sidebar with controls, no errors

**Step 3: Commit**

```bash
git add app.py
git commit -m "feat: add basic Streamlit app structure with sidebar controls"
```

---
