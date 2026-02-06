# Supabase申请和配置详细教程

## 📝 第一步：注册Supabase账号

### 1. 访问官网
打开浏览器，访问：https://supabase.com/

### 2. 注册账号
点击右上角的 **"Start your project"** 或 **"Sign Up"**

### 3. 选择登录方式（推荐GitHub）

**方式A：使用GitHub账号（推荐）**
- 点击 "Continue with GitHub"
- 授权Supabase访问你的GitHub账号
- 自动完成注册

**方式B：使用邮箱注册**
- 输入邮箱地址
- 设置密码
- 验证邮箱

---

## 🗄️ 第二步：创建数据库项目

### 1. 创建新项目

注册完成后，会自动跳转到控制台，点击 **"New Project"**

### 2. 填写项目信息

```
Organization: 选择你的组织（通常是你的用户名）
Project Name: etf-database
Database Password: 设置一个强密码（重要！请记住）
Region: 选择 Northeast Asia (Tokyo) 或 Southeast Asia (Singapore)
Pricing Plan: Free（免费套餐）
```

**重要提示：**
- 数据库密码一定要记住！后面会用到
- 建议使用密码管理器保存
- 区域选择离中国近的（东京或新加坡）

### 3. 创建项目

点击 **"Create new project"**，等待2-3分钟完成初始化

---

## 🔗 第三步：获取数据库连接字符串

### 1. 进入项目设置

项目创建完成后，点击左侧菜单的 **"Settings"**（齿轮图标）

### 2. 找到数据库设置

点击 **"Database"** 标签

### 3. 复制连接字符串

在 **"Connection string"** 部分：
- 选择 **"URI"** 格式
- 点击复制按钮

连接字符串格式如下：
```
postgresql://postgres.[project-ref]:[YOUR-PASSWORD]@aws-0-[region].pooler.supabase.com:6543/postgres
```

**重要：** 将 `[YOUR-PASSWORD]` 替换为你在第二步设置的密码

### 4. 保存连接字符串

将完整的连接字符串保存到安全的地方，格式应该类似：
```
postgresql://postgres.abcdefghijklmnop:MyStrongPassword123@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres
```

---

## 📊 第四步：创建数据库表结构

### 1. 打开SQL Editor

在Supabase控制台，点击左侧菜单的 **"SQL Editor"**

### 2. 创建新查询

点击 **"New query"** 按钮

### 3. 复制表结构SQL

打开你本地的 `database/schema.sql` 文件，复制全部内容

或者直接复制以下内容：

```sql
-- ETF数据库表结构设计

-- 1. ETF基本信息表
CREATE TABLE IF NOT EXISTS etf_info (
    code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 指标类型表
CREATE TABLE IF NOT EXISTS metric_types (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. ETF时间序列数据表
CREATE TABLE IF NOT EXISTS etf_timeseries (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value NUMERIC(20, 4) NOT NULL,
    is_aggregate BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_etf_date_metric UNIQUE (code, date, metric_type),
    FOREIGN KEY (code) REFERENCES etf_info(code) ON DELETE CASCADE
);

-- 4. 创建索引
CREATE INDEX IF NOT EXISTS idx_etf_timeseries_code_date_metric
    ON etf_timeseries(code, date, metric_type);

CREATE INDEX IF NOT EXISTS idx_etf_timeseries_date
    ON etf_timeseries(date);

CREATE INDEX IF NOT EXISTS idx_etf_timeseries_metric
    ON etf_timeseries(metric_type);

CREATE INDEX IF NOT EXISTS idx_etf_timeseries_aggregate
    ON etf_timeseries(is_aggregate, date, metric_type);

-- 5. 创建触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_etf_info_updated_at BEFORE UPDATE ON etf_info
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_etf_timeseries_updated_at BEFORE UPDATE ON etf_timeseries
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

### 4. 执行SQL

粘贴SQL后，点击右下角的 **"Run"** 按钮

### 5. 验证创建成功

如果看到 **"Success. No rows returned"** 消息，说明表结构创建成功！

你可以在左侧菜单点击 **"Table Editor"** 查看创建的表：
- etf_info
- metric_types
- etf_timeseries

---

## ✅ 第五步：验证数据库连接

### 在本地测试连接

打开命令行，运行以下命令（替换为你的连接字符串）：

```bash
# Windows
set DATABASE_URL=postgresql://postgres.xxx:password@aws-0-xxx.pooler.supabase.com:6543/postgres

# 测试连接
python -c "import psycopg2; conn = psycopg2.connect('你的连接字符串'); print('连接成功！'); conn.close()"
```

如果看到 "连接成功！"，说明配置正确！

---

## 🔐 第六步：配置GitHub Secrets

### 1. 打开GitHub仓库设置

访问：https://github.com/InfiniteLoop-luna/etf/settings/secrets/actions

### 2. 添加DATABASE_URL

- 点击 **"New repository secret"**
- Name: `DATABASE_URL`
- Secret: 粘贴你的完整连接字符串
- 点击 **"Add secret"**

### 3. 验证TUSHARE_TOKEN

确认 `TUSHARE_TOKEN` secret已存在（用于获取ETF数据）

---

## 📱 第七步：配置Streamlit Cloud

### 1. 登录Streamlit Cloud

访问：https://share.streamlit.io/

### 2. 找到你的应用

在应用列表中找到你的ETF应用

### 3. 添加Secrets

- 点击应用右侧的 **"⋮"** 菜单
- 选择 **"Settings"**
- 点击 **"Secrets"** 标签
- 添加以下内容：

```toml
DATABASE_URL = "postgresql://postgres.xxx:password@aws-0-xxx.pooler.supabase.com:6543/postgres"
```

**注意：** 替换为你的实际连接字符串

### 4. 设置环境变量

在 **"Environment variables"** 标签中添加：

```
DATA_SOURCE=database
DB_TYPE=postgresql
```

### 5. 保存并重启

- 点击 **"Save"**
- 点击 **"Reboot app"**
- 等待应用重启完成

---

## 🎯 完成！现在你可以：

### 1. 初始化数据库

在本地运行：
```bash
set DATABASE_URL=你的连接字符串
python scripts/update_database.py
```

### 2. 启用自动更新

推送代码到GitHub：
```bash
git add .
git commit -m "feat: add database auto-update"
git push
```

### 3. 手动触发测试

访问：https://github.com/InfiniteLoop-luna/etf/actions
- 选择 "Update ETF Database Daily"
- 点击 "Run workflow"

---

## 📊 Supabase控制台功能

### 常用功能

1. **Table Editor** - 查看和编辑数据
2. **SQL Editor** - 运行SQL查询
3. **Database** - 查看连接信息和统计
4. **Logs** - 查看数据库日志
5. **Settings** - 项目设置

### 监控数据

在SQL Editor中运行：

```sql
-- 查看ETF数量
SELECT COUNT(*) FROM etf_info;

-- 查看数据记录数
SELECT COUNT(*) FROM etf_timeseries;

-- 查看最新数据日期
SELECT MAX(date) FROM etf_timeseries;

-- 查看数据库大小
SELECT pg_size_pretty(pg_database_size('postgres'));
```

---

## 💡 常见问题

### Q1: 忘记数据库密码怎么办？

A: 在Supabase控制台：
1. Settings → Database
2. 点击 "Reset database password"
3. 设置新密码
4. 更新所有使用该密码的地方（GitHub Secrets、Streamlit Secrets）

### Q2: 连接字符串在哪里找？

A: Settings → Database → Connection string → URI

### Q3: 免费套餐够用吗？

A: 完全够用！
- 你的数据只需约2MB
- 免费套餐提供500MB
- 可以使用数年

### Q4: 数据会丢失吗？

A: 不会！
- Supabase自动备份
- 数据永久保存
- 99.9%可用性保证

### Q5: 如何查看使用情况？

A: 在Supabase控制台：
- Settings → Usage
- 查看存储、带宽等使用情况

---

## 🎉 总结

完成以上步骤后，你将拥有：

✅ 一个云端PostgreSQL数据库（Supabase）
✅ 自动备份和高可用性
✅ GitHub Actions可以自动更新数据
✅ Streamlit Cloud可以实时读取数据
✅ 完全免费（免费套餐）

**预计总时间：** 10-15分钟

祝你使用愉快！🚀
