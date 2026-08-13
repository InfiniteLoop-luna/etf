from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.sync_tushare_security_data import build_db_url
from src.user_preference_store import normalize_username


TABLE_NAME = "app_user_nasdaq_a_share_mapping"


@dataclass(frozen=True)
class AShareMapping:
    sector: str
    ts_code: str
    name: str
    theme: str
    reason: str
    source: str = "system"


DEFAULT_A_SHARE_MAPPINGS: tuple[AShareMapping, ...] = (
    AShareMapping("半导体", "002371.SZ", "北方华创", "半导体设备", "国产半导体设备平台龙头"),
    AShareMapping("半导体", "688012.SH", "中微公司", "半导体设备", "刻蚀与薄膜设备核心公司"),
    AShareMapping("半导体", "603986.SH", "兆易创新", "存储与芯片设计", "存储、MCU与模拟芯片代表"),
    AShareMapping("半导体", "603501.SH", "豪威集团", "芯片设计", "CIS与平台型芯片公司"),
    AShareMapping("半导体", "688256.SH", "寒武纪", "AI芯片", "国产AI算力芯片核心标的"),
    AShareMapping("AI与云计算", "000977.SZ", "浪潮信息", "AI服务器", "国内AI服务器与算力基础设施代表"),
    AShareMapping("AI与云计算", "601138.SH", "工业富联", "AI服务器", "AI服务器制造与云基础设施链"),
    AShareMapping("AI与云计算", "603019.SH", "中科曙光", "算力基础设施", "高性能计算与算力中心代表"),
    AShareMapping("AI与云计算", "688041.SH", "海光信息", "算力芯片", "国产CPU与DCU核心公司"),
    AShareMapping("AI与云计算", "000938.SZ", "紫光股份", "云与网络", "企业网络、云计算基础设施代表"),
    AShareMapping("软件与网络安全", "300454.SZ", "深信服", "网络安全", "企业安全与云计算平台公司"),
    AShareMapping("软件与网络安全", "002439.SZ", "启明星辰", "网络安全", "综合网络安全产品与服务龙头"),
    AShareMapping("软件与网络安全", "688111.SH", "金山办公", "应用软件", "国产办公软件核心平台"),
    AShareMapping("互联网平台", "300418.SZ", "昆仑万维", "互联网平台", "AI应用与海外互联网平台映射"),
    AShareMapping("互联网平台", "002517.SZ", "恺英网络", "数字内容", "线上娱乐与数字内容代表"),
    AShareMapping("消费电子与硬件", "002475.SZ", "立讯精密", "苹果产业链", "消费电子精密制造核心龙头"),
    AShareMapping("消费电子与硬件", "002241.SZ", "歌尔股份", "消费电子", "声学、XR及智能硬件代表"),
    AShareMapping("消费电子与硬件", "300433.SZ", "蓝思科技", "消费电子", "消费电子结构件与整机制造"),
    AShareMapping("新能源汽车", "300750.SZ", "宁德时代", "动力电池", "全球动力电池核心龙头"),
    AShareMapping("新能源汽车", "002594.SZ", "比亚迪", "整车与电池", "新能源汽车整车与电池平台"),
    AShareMapping("新能源汽车", "002920.SZ", "德赛西威", "智能驾驶", "智能座舱与智能驾驶核心供应商"),
    AShareMapping("生物科技", "603259.SH", "药明康德", "CXO", "创新药研发服务产业链代表"),
    AShareMapping("生物科技", "300759.SZ", "康龙化成", "CXO", "药物研发与生产服务平台"),
    AShareMapping("医疗科技", "300760.SZ", "迈瑞医疗", "医疗器械", "医疗器械平台型龙头"),
    AShareMapping("医疗科技", "688271.SH", "联影医疗", "高端影像", "国产高端医学影像设备龙头"),
    AShareMapping("金融科技与加密", "300033.SZ", "同花顺", "金融IT", "互联网金融信息服务龙头"),
    AShareMapping("金融科技与加密", "300059.SZ", "东方财富", "互联网券商", "互联网财富管理与证券平台"),
    AShareMapping("通信与数据中心", "300308.SZ", "中际旭创", "光模块", "高速光模块核心龙头"),
    AShareMapping("通信与数据中心", "300502.SZ", "新易盛", "光模块", "高速光模块头部公司"),
    AShareMapping("通信与数据中心", "300394.SZ", "天孚通信", "光器件", "光通信无源器件平台公司"),
    AShareMapping("通信与数据中心", "002837.SZ", "英维克", "液冷与温控", "数据中心温控与液冷代表"),
)


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_mapping_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        username VARCHAR(64) NOT NULL,
        sector VARCHAR(64) NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        name VARCHAR(120) NOT NULL,
        theme VARCHAR(120) NOT NULL DEFAULT '',
        reason TEXT NOT NULL DEFAULT '',
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (username, sector, ts_code)
    );
    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_username_sector
        ON {TABLE_NAME} (username, sector, updated_at DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def list_default_mappings(sector: str) -> pd.DataFrame:
    rows = [asdict(item) for item in DEFAULT_A_SHARE_MAPPINGS if item.sector == sector]
    return pd.DataFrame(rows, columns=["sector", "ts_code", "name", "theme", "reason", "source"])


def list_user_mappings(username: str, sector: str, engine: Engine | None = None) -> pd.DataFrame:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return pd.DataFrame(columns=["sector", "ts_code", "name", "theme", "reason", "source"])
    actual_engine = engine or get_engine()
    ensure_mapping_table(actual_engine)
    query = text(
        f"""
        SELECT sector, ts_code, name, theme, reason, 'user' AS source
        FROM {TABLE_NAME}
        WHERE username = :username AND sector = :sector
        ORDER BY updated_at DESC, ts_code
        """
    )
    return pd.read_sql(query, actual_engine, params={"username": normalized_username, "sector": str(sector or "").strip()})


def list_combined_mappings(username: str, sector: str, engine: Engine | None = None) -> pd.DataFrame:
    default_df = list_default_mappings(sector)
    user_df = list_user_mappings(username, sector, engine=engine) if normalize_username(username) else pd.DataFrame()
    if user_df.empty:
        return default_df
    combined = pd.concat([user_df, default_df], ignore_index=True)
    return combined.drop_duplicates(subset=["ts_code"], keep="first").reset_index(drop=True)


def add_user_mapping(
    username: str,
    sector: str,
    ts_code: str,
    name: str,
    theme: str = "",
    reason: str = "",
    engine: Engine | None = None,
) -> bool:
    normalized_username = normalize_username(username)
    normalized_sector = str(sector or "").strip()[:64]
    normalized_code = str(ts_code or "").strip().upper()[:20]
    normalized_name = str(name or "").strip()[:120]
    if not normalized_username or not normalized_sector or not normalized_code or not normalized_name:
        return False
    actual_engine = engine or get_engine()
    ensure_mapping_table(actual_engine)
    sql = text(
        f"""
        INSERT INTO {TABLE_NAME} (username, sector, ts_code, name, theme, reason)
        VALUES (:username, :sector, :ts_code, :name, :theme, :reason)
        ON CONFLICT (username, sector, ts_code)
        DO UPDATE SET name=EXCLUDED.name, theme=EXCLUDED.theme,
                      reason=EXCLUDED.reason, updated_at=CURRENT_TIMESTAMP
        """
    )
    with actual_engine.begin() as conn:
        conn.execute(sql, {"username": normalized_username, "sector": normalized_sector, "ts_code": normalized_code, "name": normalized_name, "theme": str(theme or "").strip()[:120], "reason": str(reason or "").strip()[:500]})
    return True


def remove_user_mapping(username: str, sector: str, ts_code: str, engine: Engine | None = None) -> int:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return 0
    actual_engine = engine or get_engine()
    ensure_mapping_table(actual_engine)
    with actual_engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {TABLE_NAME} WHERE username=:username AND sector=:sector AND ts_code=:ts_code"),
            {"username": normalized_username, "sector": str(sector or "").strip(), "ts_code": str(ts_code or "").strip().upper()},
        )
    return int(result.rowcount or 0)
