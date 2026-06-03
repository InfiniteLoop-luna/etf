# ETF 数据可视化系统 - 部署指南

## 概述

本系统包含两个主要组件：
1. **数据更新程序** (main.py) - 每天自动从 AkShare 获取 ETF 数据并更新 Excel 文件
2. **可视化应用** (app.py) - Streamlit Web 应用，展示 ETF 数据的交互式图表和统计信息

## 部署架构

```
GitHub Repository
    ↓
GitHub Actions (每天自动运行 main.py)
    ↓
更新 Excel 文件并提交
    ↓
VPS 上的 Streamlit 服务 / GitHub Actions 持续更新
    ↓
用户通过浏览器访问
```

## 部署步骤

### 0. 配置 Secrets（重要！）

**在推送代码前，必须先配置 Tushare Token：**

#### GitHub Secrets 配置

1. 访问 `https://github.com/InfiniteLoop-luna/etf/settings/secrets/actions`
2. 点击 "New repository secret"
3. Name: `TUSHARE_TOKEN`
4. Value: 你的 Tushare API token
5. 点击 "Add secret"

#### 本地配置

1. 复制配置模板：

   ```bash
   cp config.yaml.template config.yaml
   ```

2. 编辑 config.yaml，填入你的 Tushare token
3. config.yaml 已在 .gitignore 中，不会被提交到 GitHub

### 1. 推送代码到 GitHub

```bash
cd D:\sourcecode\etf
git push origin main
```

### 2. VPS 部署（当前生产路径）

当前线上部署路径为 VPS：

- Host: `bw-kind-hats`
- App dir: `/opt/etf-app`
- Service: `etf-streamlit`
- Site: `https://wealthspark.club/`

标准部署流程：

```bash
ssh bw-kind-hats "cd /opt/etf-app && git fetch origin && git pull --ff-only"
ssh bw-kind-hats "cd /opt/etf-app && /opt/etf-app/.venv/bin/pip install -r requirements.txt"
ssh bw-kind-hats "systemctl restart etf-streamlit"
ssh bw-kind-hats "curl -fsS http://127.0.0.1:8501/_stcore/health && echo"
ssh bw-kind-hats "curl -I -k -sS https://wealthspark.club/ | sed -n '1,8p'"
```

### 3. `mootdx` 部署说明

本项目已集成：

- 股票 `1min` 分时优先级：`mootdx -> Tushare -> 数据库缓存/空结果`

部署注意事项：

1. 线上环境需执行 `pip install -r requirements.txt` 以安装 `mootdx`
2. 若 `mootdx` 暂时不可用，页面不会整体报错，股票分时会自动回退到 `Tushare`
3. 北交所/非沪深分钟数据当前不会强行走 `mootdx minutes()`，会优雅降级

### 4. LLM API Key 部署说明

生产 VPS 不要把 API key 写进 Git。当前脚本会优先读取 `/opt/etf-app/.env`，也支持 `.streamlit/secrets.toml`。推荐在 VPS 上创建本机 `.env`：

```bash
ssh bw-kind-hats
cd /opt/etf-app
umask 077
cat > .env <<'EOF'
DISTRIBUTION_LLM_ENABLED=true
DISTRIBUTION_LLM_API_KEY=your_deepseek_api_key
DISTRIBUTION_LLM_BASE_URL=https://api.deepseek.com
DISTRIBUTION_LLM_MODEL=deepseek-v4-flash
DISTRIBUTION_LLM_TIMEOUT_SECONDS=60
DISTRIBUTION_LLM_TEMPERATURE=0.2
DISTRIBUTION_LLM_MAX_TOKENS=3200

STOCK_RESEARCH_LLM_ENABLED=true
STOCK_RESEARCH_LLM_API_KEY=your_deepseek_api_key
STOCK_RESEARCH_LLM_BASE_URL=https://api.deepseek.com
STOCK_RESEARCH_LLM_MODEL=deepseek-v4-flash
STOCK_RESEARCH_LLM_TIMEOUT_SECONDS=90
STOCK_RESEARCH_LLM_TEMPERATURE=0.2
STOCK_RESEARCH_LLM_MAX_TOKENS=3200

STOCK_RESEARCH_ENABLE_AKSHARE=true
STOCK_RESEARCH_NEWS_LIMIT=8
STOCK_RESEARCH_REPORT_LIMIT=6
STOCK_RESEARCH_MONEY_FLOW_LIMIT=8
STOCK_RESEARCH_LHB_LIMIT=6
EOF
chmod 600 .env
```

配置后重启 Streamlit，并可手动跑一次后台报告任务：

```bash
systemctl restart etf-streamlit
cd /opt/etf-app
TZ=Asia/Shanghai PYTHONPATH=/opt/etf-app /opt/etf-app/.venv/bin/python scripts/update_watchlist_stock_research_reports.py
```

夜间定时任务 `scripts/etf-data-update.sh` 会自动刷新主力出货报告和个股深度研究报告。

### 5. 可选：Streamlit Community Cloud

1. **访问** [share.streamlit.io](https://share.streamlit.io)

2. **登录** 使用 GitHub 账号

3. **创建新应用**
   - 点击 "New app"
   - Repository: `InfiniteLoop-luna/etf`
   - Branch: `main`
   - Main file path: `app.py`

4. **配置 Secrets（重要！）**

   在部署前或部署后，必须配置 Tushare Token：

   - 点击应用设置（Settings）
   - 找到 "Secrets" 部分
   - 点击 "Edit Secrets"
   - 添加以下内容：

   ```toml
   TUSHARE_TOKEN = "your_tushare_token_here"
   ```

   - 点击 "Save"

5. **部署**
   - 点击 "Deploy"
   - 等待部署完成（约 2-3 分钟）
   - 获得公开访问 URL: `https://your-app-name.streamlit.app`

### 4. 验证部署

1. **检查 GitHub Actions**
   - 访问 Actions 页面
   - 手动触发一次工作流测试
   - 确认数据更新成功

2. **检查 Streamlit 应用**
   - 访问部署的 URL
   - 验证所有 7 个指标都显示在下拉菜单中
   - 测试图表交互功能

## 自动更新流程

1. **每天 18:00** GitHub Actions 自动运行
2. **检查交易日** 如果不是交易日，跳过更新
3. **获取数据** 从 AkShare / Tushare 获取最新数据
4. **更新数据文件/数据库**
5. **提交推送** 自动提交更改到 GitHub
6. **VPS 拉取新代码并提供页面服务**
7. **用户刷新页面即可看到最新数据**

## 手动更新

如果需要手动更新数据：

### 方法 1: GitHub Actions 手动触发
1. 访问 `https://github.com/InfiniteLoop-luna/etf/actions`
2. 选择 "Update ETF Data Daily" 工作流
3. 点击 "Run workflow"
4. 选择分支并运行

### 方法 2: 本地运行并推送
```bash
cd "d:\Code\etf"
python main.py
git add "主要ETF基金份额变动情况.xlsx"
git commit -m "chore: manual data update"
git push origin main
```

## 成本说明

- **GitHub Actions**: 免费（公开仓库每月 2000 分钟）
- **Streamlit Community Cloud**: 免费
  - 1 GB RAM
  - 1 CPU
  - 无限流量
  - 公开访问

## 故障排查

### GitHub Actions 失败
- 检查 Actions 日志查看错误信息
- 常见问题：AkShare API 限流、网络问题
- 解决方案：等待下次自动运行或手动重试

### Streamlit 应用错误
- 检查 Streamlit Cloud 日志
- 常见问题：Excel 文件格式、依赖版本
- 解决方案：查看日志，修复代码后推送

### 数据未更新
- 检查是否为交易日
- 检查 GitHub Actions 是否成功运行
- 检查 Excel 文件是否有新的提交

## 监控和维护

1. **定期检查** GitHub Actions 运行状态
2. **关注** Streamlit 应用性能和错误
3. **更新依赖** 定期更新 requirements.txt 中的包版本
4. **备份数据** Excel 文件在 Git 中有完整历史记录

## 技术栈

- **数据源**: AkShare (中国金融数据接口)
- **数据处理**: Pandas, openpyxl
- **可视化**: Streamlit, Plotly
- **自动化**: GitHub Actions
- **部署**: Streamlit Community Cloud
