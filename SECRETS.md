# Secrets 管理说明

## 概述

本项目使用 Tushare API 获取 ETF 数据，需要配置 API Token。为了安全起见，Token 不应该提交到 GitHub 仓库中。

## 配置方式

### 1. 本地开发

复制配置模板并填入你的 Token：

```bash
cp config.yaml.template config.yaml
```

编辑 `config.yaml`，在 `tushare.token` 字段填入你的 Token：

```yaml
data_sources:
  tushare:
    enabled: true
    priority: 1
    token: "your_tushare_token_here"  # 填入你的 Token
    timeout: 10
```

**注意：** `config.yaml` 已在 `.gitignore` 中，不会被提交到 GitHub。

### 2. GitHub Actions

在 GitHub 仓库中配置 Secret：

1. 访问 `https://github.com/你的用户名/etf/settings/secrets/actions`
2. 点击 "New repository secret"
3. Name: `TUSHARE_TOKEN`
4. Value: 你的 Tushare API token
5. 点击 "Add secret"

GitHub Actions 会自动从环境变量读取 Token。

### 3. Streamlit Cloud

在 Streamlit Cloud 应用设置中配置：

1. 打开你的应用
2. 点击右上角的设置图标
3. 选择 "Secrets"
4. 添加以下内容：

```toml
TUSHARE_TOKEN = "your_tushare_token_here"
```

5. 点击 "Save"
6. 应用会自动重启并使用新的 Token

## Token 优先级

程序会按以下优先级读取 Token：

1. 环境变量 `TUSHARE_TOKEN`（优先级最高）
2. `config.yaml` 中的 `tushare.token` 字段

这样可以确保：
- 本地开发使用 config.yaml
- GitHub Actions 使用 GitHub Secrets
- Streamlit Cloud 使用 Streamlit Secrets

## 安全提示

- ❌ **不要** 将 Token 提交到 GitHub
- ❌ **不要** 在公开的地方分享 Token
- ✅ **使用** 环境变量或 Secrets 管理工具
- ✅ **定期** 更换 Token
- ✅ **检查** `.gitignore` 确保 `config.yaml` 被忽略

## 获取 Tushare Token

如果你还没有 Tushare Token：

1. 访问 [Tushare 官网](https://tushare.pro/)
2. 注册账号
3. 在个人中心获取 API Token
4. 按照上述方式配置到项目中
