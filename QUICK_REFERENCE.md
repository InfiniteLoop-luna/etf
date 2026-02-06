# 🚀 GitHub Actions自动更新数据库 - 快速参考

## 📋 5步完成设置（15分钟）

### 1️⃣ 创建Supabase数据库（5分钟）
```
1. 访问 https://supabase.com/ 注册
2. 创建新项目 "etf-database"
3. 复制连接字符串（Settings → Database → URI）
4. 在SQL Editor中运行 database/schema.sql
```

### 2️⃣ 配置GitHub Secrets（2分钟）
```
1. 访问 https://github.com/InfiniteLoop-luna/etf/settings/secrets/actions
2. 添加 DATABASE_URL = 你的连接字符串
3. 确认 TUSHARE_TOKEN 已存在
```

### 3️⃣ 初始化数据库（5分钟）
```bash
set DATABASE_URL=你的连接字符串
python scripts/update_database.py
python scripts/verify_database.py
```

### 4️⃣ 启用GitHub Actions（1分钟）
```bash
git add .
git commit -m "feat: add automatic database update"
git push
```
然后在GitHub Actions页面手动触发测试

### 5️⃣ 配置Streamlit Cloud（2分钟）
```
1. 在Streamlit Cloud添加Secrets:
   DATABASE_URL = "你的连接字符串"

2. 添加环境变量:
   DATA_SOURCE=database
   DB_TYPE=postgresql

3. 重启应用
```

---

## 🔑 关键文件

| 文件 | 用途 |
|------|------|
| `.github/workflows/update-database.yml` | GitHub Actions工作流 |
| `scripts/update_database.py` | 数据库更新脚本 |
| `scripts/verify_database.py` | 数据验证脚本 |
| `database/schema.sql` | 数据库表结构 |
| `app_with_db.py` | 支持数据库的应用 |

---

## ⏰ 自动更新时间

**当前设置**: 每天北京时间18:00（UTC 10:00）

**修改时间**: 编辑 `.github/workflows/update-database.yml`
```yaml
# 北京时间20:00
- cron: '0 12 * * *'

# 北京时间早上8:00
- cron: '0 0 * * *'

# 仅工作日18:00
- cron: '0 10 * * 1-5'
```

---

## ✅ 验证清单

### GitHub Actions
- [ ] Workflow运行成功（绿色✓）
- [ ] 日志显示 "Database update completed"
- [ ] 无错误信息

### Supabase数据库
```sql
SELECT COUNT(*) FROM etf_info;          -- 应该是13
SELECT COUNT(*) FROM etf_timeseries;    -- 应该是35000+
SELECT MAX(date) FROM etf_timeseries;   -- 应该是最近日期
```

### Streamlit应用
- [ ] 应用正常加载
- [ ] 数据显示正常
- [ ] 图表可交互
- [ ] 包含最新数据

---

## 🔧 常见问题速查

| 问题 | 解决方案 |
|------|---------|
| Actions失败 | 检查DATABASE_URL是否正确设置 |
| 连接被拒绝 | 检查Supabase项目是否暂停 |
| 认证失败 | 检查连接字符串中的密码 |
| 数据未更新 | 手动触发workflow测试 |
| 应用加载失败 | 检查Streamlit Secrets配置 |

---

## 📊 监控命令

### 检查最新数据
```sql
SELECT MAX(date), MAX(updated_at)
FROM etf_timeseries;
```

### 检查今天更新的记录
```sql
SELECT COUNT(*)
FROM etf_timeseries
WHERE updated_at >= CURRENT_DATE;
```

### 检查数据库大小
```sql
SELECT pg_size_pretty(pg_database_size('postgres'));
```

---

## 🎯 手动触发更新

1. 访问 https://github.com/InfiniteLoop-luna/etf/actions
2. 选择 "Update ETF Database Daily"
3. 点击 "Run workflow"
4. 等待2-3分钟完成

---

## 💡 重要提示

### ✅ 做这些
- 定期检查GitHub Actions运行状态
- 每周验证数据完整性
- 保护好DATABASE_URL（不要泄露）
- 定期更新数据库密码

### ❌ 不要做这些
- 不要将DATABASE_URL提交到代码
- 不要在日志中打印连接字符串
- 不要在免费套餐下存储超过500MB数据
- 不要忘记监控Supabase使用情况

---

## 📞 快速链接

- **GitHub仓库**: https://github.com/InfiniteLoop-luna/etf
- **GitHub Actions**: https://github.com/InfiniteLoop-luna/etf/actions
- **Supabase控制台**: https://app.supabase.com/
- **Streamlit Cloud**: https://share.streamlit.io/
- **完整文档**: [GITHUB_ACTIONS_SETUP.md](GITHUB_ACTIONS_SETUP.md)

---

## 🎉 完成后你将拥有

- ✅ 每天自动更新的数据库
- ✅ 持久化的数据存储
- ✅ 实时的数据展示
- ✅ 完整的监控日志
- ✅ 零维护成本（自动化）

**总设置时间**: 15分钟
**每周维护**: 5分钟

祝你使用愉快！🚀
