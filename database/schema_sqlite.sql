-- SQLite版本的表结构（用于本地开发和测试）
-- SQLite不支持某些PostgreSQL特性，需要简化

-- 1. ETF基本信息表
CREATE TABLE IF NOT EXISTS etf_info (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 指标类型表
CREATE TABLE IF NOT EXISTS metric_types (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. ETF时间序列数据表（核心表 - 长表结构）
CREATE TABLE IF NOT EXISTS etf_timeseries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,
    date DATE NOT NULL,
    metric_type TEXT NOT NULL,
    value REAL NOT NULL,
    is_aggregate INTEGER DEFAULT 0,  -- SQLite使用0/1表示布尔值
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 唯一约束
    UNIQUE (code, date, metric_type),

    -- 外键约束
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

-- 5. 创建触发器自动更新updated_at字段
CREATE TRIGGER IF NOT EXISTS update_etf_info_updated_at
AFTER UPDATE ON etf_info
FOR EACH ROW
BEGIN
    UPDATE etf_info SET updated_at = CURRENT_TIMESTAMP WHERE code = NEW.code;
END;

CREATE TRIGGER IF NOT EXISTS update_etf_timeseries_updated_at
AFTER UPDATE ON etf_timeseries
FOR EACH ROW
BEGIN
    UPDATE etf_timeseries SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
