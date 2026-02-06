-- ETF数据库表结构设计
-- 推荐使用长表（纵向）结构，更适合时间序列数据的查询和可视化

-- 1. ETF基本信息表
CREATE TABLE IF NOT EXISTS etf_info (
    code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 指标类型表（用于规范化metric_type）
CREATE TABLE IF NOT EXISTS metric_types (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. ETF时间序列数据表（核心表 - 长表结构）
CREATE TABLE IF NOT EXISTS etf_timeseries (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value NUMERIC(20, 4) NOT NULL,
    is_aggregate BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- 唯一约束：同一ETF、同一日期、同一指标只能有一条记录
    CONSTRAINT unique_etf_date_metric UNIQUE (code, date, metric_type),

    -- 外键约束
    FOREIGN KEY (code) REFERENCES etf_info(code) ON DELETE CASCADE
);

-- 4. 创建索引以提高查询效率
-- 复合索引：按代码、日期、指标类型查询（最常用的查询模式）
CREATE INDEX IF NOT EXISTS idx_etf_timeseries_code_date_metric
    ON etf_timeseries(code, date, metric_type);

-- 单列索引：按日期查询（用于日期范围筛选）
CREATE INDEX IF NOT EXISTS idx_etf_timeseries_date
    ON etf_timeseries(date);

-- 单列索引：按指标类型查询
CREATE INDEX IF NOT EXISTS idx_etf_timeseries_metric
    ON etf_timeseries(metric_type);

-- 索引：查询汇总数据
CREATE INDEX IF NOT EXISTS idx_etf_timeseries_aggregate
    ON etf_timeseries(is_aggregate, date, metric_type);

-- 5. 创建更新时间戳的触发器（PostgreSQL）
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
