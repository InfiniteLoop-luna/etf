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
Streamlit Cloud (自动重新部署)
    ↓
用户通过浏览器访问
```

## 部署步骤

### 1. 推送代码到 GitHub

```bash
cd "d:\Code\etf\.worktrees\etf-visualization"

# 如果遇到代理问题，先清除代理设置
git config --global --unset http.proxy
git config --global --unset https.proxy

# 推送分支
git push -u origin feature/etf-visualization

# 合并到主分支
cd "d:\Code\etf"
git checkout main
git merge feature/etf-visualization
git push origin main
```

### 2. 配置 GitHub Actions

GitHub Actions 工作流已经创建在 `.github/workflows/update-data.yml`

**工作流说明：**
- **触发时间**: 每天北京时间 18:00 (UTC 10:00)
- **执行内容**: 运行 `python main.py` 更新数据
- **自动提交**: 如果数据有变化，自动提交并推送到 GitHub
- **手动触发**: 可以在 GitHub Actions 页面手动运行

**首次启用：**
1. 推送代码到 GitHub 后，Actions 会自动启用
2. 访问 `https://github.com/InfiniteLoop-luna/etf/actions`
3. 查看工作流运行状态

### 3. 部署到 Streamlit Community Cloud

1. **访问** [share.streamlit.io](https://share.streamlit.io)

2. **登录** 使用 GitHub 账号

3. **创建新应用**
   - 点击 "New app"
   - Repository: `InfiniteLoop-luna/etf`
   - Branch: `main`
   - Main file path: `app.py`

4. **部署**
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
3. **获取数据** 从 AkShare 获取最新 ETF 数据
4. **更新 Excel** 更新 `主要ETF基金份额变动情况.xlsx`
5. **提交推送** 自动提交更改到 GitHub
6. **触发部署** Streamlit Cloud 检测到文件变化，自动重新部署
7. **用户访问** 用户刷新页面即可看到最新数据

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
