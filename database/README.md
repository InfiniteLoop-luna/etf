# ETF数据库方案完整指南

## 📋 目录
1. [数据库设计说明](#数据库设计说明)
2. [本地开发指南](#本地开发指南)
3. [Streamlit Cloud部署指南](#streamlit-cloud部署指南)
4. [数据导入说明](#数据导入说明)
5. [常见问题](#常见问题)

---

## 数据库设计说明

### 1. 表结构设计

#### 为什么选择长表（纵向）结构？

**长表结构的优势：**
- ✅ **更适合时间序列数据**：每条记录代表一个时间点的一个指标值
- ✅ **查询灵活**：可以轻松筛选特定日期范围、特定ETF、特定指标
- ✅ **易于扩展**：添加新指标不需要修改表结构
- ✅ **适合可视化**：Plotly等可视化库更容易处理长格式数据
- ✅ **索引高效**：可以针对常用查询模式创建复合索引

**宽表结构的劣势：**
- ❌ 每个日期需要一列，添加新日期需要修改表结构
- ❌ 查询特定日期范围需要复杂的列选择
- ❌ 不适合动态增长的时间序列数据

### 2. 核心表结构

#### etf_info（ETF基本信息表）
```sql
- code: ETF代码（主键）
- name: ETF名称
- created_at: 创建时间
- updated_at: 更新时间
```

#### etf_timeseries（时间序列数据表）
```sql
- id: 自增主键
- code: ETF代码（外键）
- date: 日期
- metric_type: 指标类型（总市值、份额、变动等）
- value: 数值
- is_aggregate: 是否为汇总数据
- created_at: 创建时间
- updated_at: 更新时间
- UNIQUE(code, date, metric_type): 唯一约束
```

### 3. 索引策略

```sql
-- 复合索引：最常用的查询模式
CREATE INDEX idx_etf_timeseries_code_date_metric
    ON etf_timeseries(code, date, metric_type);

-- 日期索引：用于日期范围查询
CREATE INDEX idx_etf_timeseries_date
    ON etf_timeseries(date);

-- 指标类型索引：用于按指标筛选
CREATE INDEX idx_etf_timeseries_metric
    ON etf_timeseries(metric_type);

-- 汇总数据索引：快速查询汇总行
CREATE INDEX idx_etf_timeseries_aggregate
    ON etf_timeseries(is_aggregate, date, metric_type);
```

---

## 本地开发指南

### 方案1：使用SQLite（推荐用于本地开发）

#### 步骤1：创建数据库并导入数据

```bash
# 进入database目录
cd database

# 运行导入脚本
python import_data.py
```

这将：
1. 创建 `etf_data.db` SQLite数据库文件
2. 创建所有表和索引
3. 从Excel导入所有历史数据
4. 显示导入统计信息

#### 步骤2：配置Streamlit应用使用数据库

创建或修改 `.streamlit/config.toml`：

```toml
[server]
port = 8501

[theme]
primaryColor = "#FF4B4B"
```

设置环境变量：

```bash
# Windows
set DATA_SOURCE=database
set DB_TYPE=sqlite
set DB_PATH=etf_data.db

# Linux/Mac
export DATA_SOURCE=database
export DB_TYPE=sqlite
export DB_PATH=etf_data.db
```

#### 步骤3：运行应用

```bash
# 使用数据库版本
streamlit run app_with_db.py

# 或继续使用Excel版本
streamlit run app.py
```

### 方案2：使用PostgreSQL（推荐用于生产环境）

#### 步骤1：安装PostgreSQL

```bash
# 安装psycopg2
pip install psycopg2-binary
```

#### 步骤2：创建数据库

```sql
CREATE DATABASE etf_data;
```

#### 步骤3：导入数据

修改 `database/import_data.py` 的主函数：

```python
if __name__ == '__main__':
    # PostgreSQL连接字符串
    connection_string = "postgresql://username:password@localhost:5432/etf_data"

    excel_file = '../主要ETF基金份额变动情况.xlsx'
    stats = import_to_postgresql(excel_file, connection_string)

    print(f"\n导入完成:")
    print(f"  新增: {stats['inserted']} 条")
    print(f"  更新: {stats['updated']} 条")
    print(f"  失败: {stats['failed']} 条")
```

---

## Streamlit Cloud部署指南

### ⚠️ 重要：Streamlit Cloud的限制

**Streamlit Cloud是无状态的（ephemeral）：**
- 容器会定期重启
- 本地文件系统不持久化
- SQLite数据库文件会在重启后丢失

### 推荐方案

#### 方案A：继续使用Excel文件（最简单）✅

**优点：**
- 无需额外配置
- 部署简单
- 适合数据更新不频繁的场景

**步骤：**
1. 将Excel文件提交到Git仓库
2. 使用现有的 `app.py`
3. 部署到Streamlit Cloud

**适用场景：**
- 数据每天或每周手动更新
- 数据量不大（< 10MB）
- 不需要实时数据更新

#### 方案B：使用外部PostgreSQL数据库（推荐用于生产）✅

**优点：**
- 数据持久化
- 支持大数据量
- 可以实时更新数据
- 多个应用可以共享数据

**步骤：**

1. **创建PostgreSQL数据库**

   推荐使用以下服务之一：
   - [Supabase](https://supabase.com/)（免费套餐，推荐）
   - [ElephantSQL](https://www.elephantsql.com/)（免费套餐）
   - [Neon](https://neon.tech/)（免费套餐）
   - [Railway](https://railway.app/)

2. **在Streamlit Cloud配置Secrets**

   在Streamlit Cloud的应用设置中，添加 `.streamlit/secrets.toml`：

   ```toml
   # PostgreSQL连接信息
   DATABASE_URL = "postgresql://username:password@host:5432/database"
   ```

3. **配置环境变量**

   在Streamlit Cloud的应用设置中，添加环境变量：
   ```
   DATA_SOURCE=database
   DB_TYPE=postgresql
   ```

4. **添加依赖**

   在 `requirements.txt` 中添加：
   ```
   psycopg2-binary
   ```

5. **部署应用**

   使用 `app_with_db.py` 作为主文件

6. **导入初始数据**

   在本地运行一次导入脚本：
   ```python
   from database.import_data import import_to_postgresql

   connection_string = "你的PostgreSQL连接字符串"
   excel_file = '主要ETF基金份额变动情况.xlsx'

   stats = import_to_postgresql(excel_file, connection_string)
   ```

#### 方案C：使用Streamlit的文件上传功能

**优点：**
- 用户可以上传最新的Excel文件
- 无需数据库

**实现：**
```python
uploaded_file = st.file_uploader("上传ETF数据Excel文件", type=['xlsx'])
if uploaded_file:
    df = load_etf_data(uploaded_file)
```

---

## 数据导入说明

### 自动处理的功能

1. **日期格式转换**
   - 自动识别Excel中的datetime对象
   - 支持 `2026/02/02` 和 `2026-02-02` 格式
   - 统一转换为 `YYYY-MM-DD` 格式

2. **Upsert操作（插入或更新）**
   - SQLite: 使用 `INSERT OR REPLACE`
   - PostgreSQL: 使用 `ON CONFLICT DO UPDATE`
   - 根据 `(code, date, metric_type)` 判断是否重复

3. **日志输出**
   ```
   2026-02-06 10:00:00 - INFO - 开始从Excel导入数据
   2026-02-06 10:00:01 - INFO - 从Excel加载了 5000 条记录
   2026-02-06 10:00:02 - INFO - 导入/更新了 50 个ETF基本信息
   2026-02-06 10:00:05 - INFO - 数据导入完成
   2026-02-06 10:00:05 - INFO -   - 新增记录: 4500
   2026-02-06 10:00:05 - INFO -   - 更新记录: 500
   2026-02-06 10:00:05 - INFO -   - 失败记录: 0
   ```

### 定期更新数据

#### 方法1：手动运行导入脚本

```bash
cd database
python import_data.py
```

#### 方法2：创建定时任务

**Windows（任务计划程序）：**
```batch
# 创建批处理文件 update_data.bat
cd d:\Code\etf\database
python import_data.py
```

**Linux/Mac（cron）：**
```bash
# 编辑crontab
crontab -e

# 每天早上8点运行
0 8 * * * cd /path/to/etf/database && python import_data.py
```

#### 方法3：使用GitHub Actions自动更新

创建 `.github/workflows/update_data.yml`：

```yaml
name: Update ETF Data

on:
  schedule:
    - cron: '0 0 * * *'  # 每天UTC 0点运行
  workflow_dispatch:  # 允许手动触发

jobs:
  update:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install psycopg2-binary

      - name: Import data to database
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
        run: |
          cd database
          python import_data.py
```

---

## 常见问题

### Q1: 为什么选择长表而不是宽表？

**A:** 长表结构更适合时间序列数据：
- 查询灵活（可以轻松筛选日期范围）
- 易于扩展（添加新日期不需要修改表结构）
- 适合可视化（Plotly等库更容易处理）
- 索引高效（可以创建复合索引）

### Q2: Streamlit Cloud能用SQLite吗？

**A:** 不推荐。Streamlit Cloud是无状态的，容器重启后SQLite文件会丢失。建议：
- 继续使用Excel文件（简单场景）
- 使用外部PostgreSQL数据库（生产场景）

### Q3: 如何在本地测试数据库版本？

**A:**
```bash
# 1. 导入数据
cd database
python import_data.py

# 2. 设置环境变量
export DATA_SOURCE=database
export DB_TYPE=sqlite

# 3. 运行应用
streamlit run app_with_db.py
```

### Q4: 数据更新频率建议？

**A:** 根据数据源特点：
- **Excel方案**：每天或每周手动更新
- **数据库方案**：可以实时更新，建议每天自动导入

### Q5: 如何迁移现有数据？

**A:** 使用提供的导入脚本：
```python
from database.import_data import import_to_sqlite, import_to_postgresql

# SQLite
import_to_sqlite('主要ETF基金份额变动情况.xlsx', 'etf_data.db')

# PostgreSQL
import_to_postgresql('主要ETF基金份额变动情况.xlsx', connection_string)
```

### Q6: 性能优化建议？

**A:**
1. 确保创建了所有索引
2. 使用Streamlit的 `@st.cache_data` 缓存查询结果
3. 对于大数据量，考虑分页查询
4. PostgreSQL性能优于SQLite

---

## 📊 性能对比

| 方案 | 查询速度 | 数据持久化 | 部署难度 | 适用场景 |
|------|---------|-----------|---------|---------|
| Excel | ⭐⭐⭐ | ✅ | ⭐⭐⭐⭐⭐ | 小数据量，低频更新 |
| SQLite | ⭐⭐⭐⭐ | ⚠️ 本地 | ⭐⭐⭐⭐ | 本地开发 |
| PostgreSQL | ⭐⭐⭐⭐⭐ | ✅ | ⭐⭐⭐ | 生产环境，大数据量 |

---

## 🎯 推荐方案总结

### 本地开发
✅ **使用SQLite** - 简单快速，无需额外配置

### Streamlit Cloud部署
✅ **方案1（推荐）**：继续使用Excel - 简单可靠
✅ **方案2（进阶）**：使用Supabase PostgreSQL - 专业可扩展

### 生产环境
✅ **使用PostgreSQL** - 性能最佳，功能完整

---

## 📝 下一步

1. **本地测试**：运行导入脚本，测试数据库功能
2. **选择方案**：根据需求选择Excel或数据库方案
3. **部署应用**：按照指南部署到Streamlit Cloud
4. **设置更新**：配置定期数据更新机制

如有问题，请查看日志输出或联系技术支持。
