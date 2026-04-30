from __future__ import annotations

import json
import hashlib
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

TREND_RECO_RUNS_TABLE = "trend_reco_runs"
TREND_RECO_ITEMS_TABLE = "trend_reco_items"
LATEST_RECO_FILENAME = "latest_trend_recommendations.json"

logger = logging.getLogger(__name__)


def build_db_url():
    """Reuse the project's existing DB URL conventions when possible."""
    try:
        from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

        return _sync_build_db_url()
    except Exception:
        pass

    try:
        from src.hotmoney_sync import build_db_url as _hotmoney_build_db_url

        return _hotmoney_build_db_url()
    except Exception:
        pass

    direct_url = os.getenv("ETF_PG_URL") or os.getenv("DATABASE_URL")
    if direct_url:
        return direct_url

    password = os.getenv("ETF_PG_PASSWORD") or os.getenv("PGPASSWORD")
    if not password:
        raise RuntimeError("未配置数据库密码，请设置 ETF_PG_PASSWORD 或 PGPASSWORD")

    from sqlalchemy.engine import URL

    return URL.create(
        "postgresql+psycopg2",
        username=os.getenv("ETF_PG_USER", "postgres"),
        password=password,
        host=os.getenv("ETF_PG_HOST", "67.216.207.73"),
        port=int(os.getenv("ETF_PG_PORT", "5432")),
        database=os.getenv("ETF_PG_DATABASE", "postgres"),
        query={"sslmode": os.getenv("ETF_PG_SSLMODE", "disable")},
    )


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def compute_record_hash(payload: dict) -> str:
    content = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:64]


def normalize_trade_date(value) -> date:
    if value is None:
        raise ValueError("payload.trade_date 不能为空")
    text_value = str(value).strip()
    if not text_value:
        raise ValueError("payload.trade_date 不能为空字符串")

    candidates = [text_value, text_value.replace("-", "")]
    for candidate in candidates:
        try:
            if len(candidate) == 8 and candidate.isdigit():
                return datetime.strptime(candidate, "%Y%m%d").date()
            if len(text_value) == 10 and text_value.count("-") == 2:
                return datetime.strptime(text_value, "%Y-%m-%d").date()
        except ValueError:
            continue

    raise ValueError(f"无法解析 trade_date: {value}")


def normalize_timestamp(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    text_value = str(value).strip()
    if not text_value:
        return None

    iso_candidate = text_value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y%m%d %H:%M:%S"):
        try:
            return datetime.strptime(text_value, fmt)
        except ValueError:
            continue
    return None


def to_int(value) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def to_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def ensure_storage_objects(engine: Engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TREND_RECO_RUNS_TABLE} (
        trade_date DATE PRIMARY KEY,
        generated_at TIMESTAMPTZ,
        universe_size INTEGER NOT NULL DEFAULT 0,
        source_file TEXT,
        payload JSONB NOT NULL,
        record_hash VARCHAR(64) NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_{TREND_RECO_RUNS_TABLE}_generated_at
        ON {TREND_RECO_RUNS_TABLE}(generated_at DESC);

    CREATE TABLE IF NOT EXISTS {TREND_RECO_ITEMS_TABLE} (
        trade_date DATE NOT NULL,
        reco_type VARCHAR(16) NOT NULL,
        rank_no INTEGER NOT NULL,
        ts_code VARCHAR(20) NOT NULL,
        name TEXT,
        industry TEXT,
        close NUMERIC(18, 4),
        trend_score NUMERIC(18, 6),
        risk_score NUMERIC(18, 6),
        prob_up_5d NUMERIC(18, 6),
        prob_up_20d NUMERIC(18, 6),
        reason TEXT,
        source_file TEXT,
        payload JSONB NOT NULL,
        record_hash VARCHAR(64) NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (trade_date, reco_type, rank_no)
    );
    CREATE INDEX IF NOT EXISTS idx_{TREND_RECO_ITEMS_TABLE}_ts_code
        ON {TREND_RECO_ITEMS_TABLE}(ts_code);
    CREATE INDEX IF NOT EXISTS idx_{TREND_RECO_ITEMS_TABLE}_trade_date
        ON {TREND_RECO_ITEMS_TABLE}(trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{TREND_RECO_ITEMS_TABLE}_type_date
        ON {TREND_RECO_ITEMS_TABLE}(reco_type, trade_date DESC);
    CREATE INDEX IF NOT EXISTS idx_{TREND_RECO_ITEMS_TABLE}_industry
        ON {TREND_RECO_ITEMS_TABLE}(industry);
    """

    with engine.begin() as conn:
        for statement in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(statement))



def build_item_records(payload: dict, source_file: str | os.PathLike | None = None) -> list[dict]:
    trade_date = normalize_trade_date(payload.get("trade_date"))
    source_file_text = str(source_file) if source_file else None

    rows: list[dict] = []
    groups = [
        ("uptrend", payload.get("top_uptrend") or []),
        ("avoid", payload.get("top_avoid") or []),
    ]
    for reco_type, items in groups:
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                logger.warning("skip invalid %s item at %s: not a dict", reco_type, idx)
                continue
            ts_code = str(item.get("ts_code") or "").strip()
            if not ts_code:
                logger.warning("skip invalid %s item at %s: ts_code missing", reco_type, idx)
                continue
            rank_no = to_int(item.get("rank")) or idx
            item_payload = dict(item)
            rows.append(
                {
                    "trade_date": trade_date,
                    "reco_type": reco_type,
                    "rank_no": rank_no,
                    "ts_code": ts_code,
                    "name": str(item.get("name") or "").strip() or None,
                    "industry": str(item.get("industry") or "").strip() or None,
                    "close": to_float(item.get("close")),
                    "trend_score": to_float(item.get("trend_score")),
                    "risk_score": to_float(item.get("risk_score")),
                    "prob_up_5d": to_float(item.get("prob_up_5d")),
                    "prob_up_20d": to_float(item.get("prob_up_20d")),
                    "reason": str(item.get("reason") or "").strip() or None,
                    "source_file": source_file_text,
                    "payload_json": json.dumps(item_payload, ensure_ascii=False, sort_keys=True, default=str),
                    "record_hash": compute_record_hash(item_payload),
                }
            )
    return rows



def upsert_trend_reco_payload(engine: Engine, payload: dict, source_file: str | os.PathLike | None = None) -> dict:
    ensure_storage_objects(engine)

    trade_date = normalize_trade_date(payload.get("trade_date"))
    generated_at = normalize_timestamp(payload.get("generated_at"))
    universe_size = to_int(payload.get("universe_size")) or 0
    source_file_text = str(source_file) if source_file else None
    run_payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    run_record_hash = compute_record_hash(payload)
    item_rows = build_item_records(payload, source_file=source_file_text)

    run_sql = text(
        f"""
        INSERT INTO {TREND_RECO_RUNS_TABLE} (
            trade_date, generated_at, universe_size, source_file, payload, record_hash
        ) VALUES (
            :trade_date, :generated_at, :universe_size, :source_file, CAST(:payload_json AS JSONB), :record_hash
        )
        ON CONFLICT (trade_date) DO UPDATE SET
            generated_at = EXCLUDED.generated_at,
            universe_size = EXCLUDED.universe_size,
            source_file = EXCLUDED.source_file,
            payload = EXCLUDED.payload,
            record_hash = EXCLUDED.record_hash,
            ingested_at = NOW();
        """
    )

    item_sql = text(
        f"""
        INSERT INTO {TREND_RECO_ITEMS_TABLE} (
            trade_date, reco_type, rank_no, ts_code, name, industry, close,
            trend_score, risk_score, prob_up_5d, prob_up_20d, reason,
            source_file, payload, record_hash
        ) VALUES (
            :trade_date, :reco_type, :rank_no, :ts_code, :name, :industry, :close,
            :trend_score, :risk_score, :prob_up_5d, :prob_up_20d, :reason,
            :source_file, CAST(:payload_json AS JSONB), :record_hash
        )
        ON CONFLICT (trade_date, reco_type, rank_no) DO UPDATE SET
            ts_code = EXCLUDED.ts_code,
            name = EXCLUDED.name,
            industry = EXCLUDED.industry,
            close = EXCLUDED.close,
            trend_score = EXCLUDED.trend_score,
            risk_score = EXCLUDED.risk_score,
            prob_up_5d = EXCLUDED.prob_up_5d,
            prob_up_20d = EXCLUDED.prob_up_20d,
            reason = EXCLUDED.reason,
            source_file = EXCLUDED.source_file,
            payload = EXCLUDED.payload,
            record_hash = EXCLUDED.record_hash,
            ingested_at = NOW();
        """
    )

    with engine.begin() as conn:
        conn.execute(
            run_sql,
            {
                "trade_date": trade_date,
                "generated_at": generated_at,
                "universe_size": universe_size,
                "source_file": source_file_text,
                "payload_json": run_payload_json,
                "record_hash": run_record_hash,
            },
        )

        conn.execute(
            text(f"DELETE FROM {TREND_RECO_ITEMS_TABLE} WHERE trade_date = :trade_date"),
            {"trade_date": trade_date},
        )
        if item_rows:
            conn.execute(item_sql, item_rows)

    return {
        "trade_date": trade_date.isoformat(),
        "run_written": 1,
        "item_rows": len(item_rows),
        "source_file": source_file_text,
    }



def load_payload_from_file(file_path: str | os.PathLike) -> dict:
    path = Path(file_path)
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)



def upsert_trend_reco_file(engine: Engine, file_path: str | os.PathLike) -> dict:
    path = Path(file_path)
    payload = load_payload_from_file(path)
    return upsert_trend_reco_payload(engine, payload, source_file=str(path))



def iter_reco_files(base_dir: str | os.PathLike, include_latest: bool = False) -> list[Path]:
    base_path = Path(base_dir)
    if not base_path.exists():
        return []

    files = sorted(base_path.glob("*_trend_recommendations.json"))
    if not include_latest:
        files = [p for p in files if p.name != LATEST_RECO_FILENAME]
    return files



def summarize_payload(payload: dict) -> dict:
    return {
        "trade_date": str(payload.get("trade_date") or ""),
        "generated_at": str(payload.get("generated_at") or ""),
        "universe_size": to_int(payload.get("universe_size")) or 0,
        "top_uptrend": len(payload.get("top_uptrend") or []),
        "top_avoid": len(payload.get("top_avoid") or []),
    }



def backfill_directory(
    engine: Engine,
    base_dir: str | os.PathLike,
    include_latest: bool = False,
    limit: Optional[int] = None,
) -> dict:
    files = iter_reco_files(base_dir, include_latest=include_latest)
    if limit and limit > 0:
        files = files[:limit]

    processed = 0
    item_rows = 0
    trade_dates: list[str] = []
    for file_path in files:
        result = upsert_trend_reco_file(engine, file_path)
        processed += 1
        item_rows += int(result.get("item_rows") or 0)
        trade_dates.append(str(result.get("trade_date") or ""))

    return {
        "files": processed,
        "item_rows": item_rows,
        "trade_dates": trade_dates,
    }



def _coerce_payload_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}



def fetch_trend_reco_payload(engine: Engine, trade_date: str | date | None = None) -> dict:
    sql = f"""
    SELECT trade_date, generated_at, universe_size, source_file, payload
    FROM {TREND_RECO_RUNS_TABLE}
    """
    params = {}
    if trade_date:
        params["trade_date"] = normalize_trade_date(trade_date)
        sql += " WHERE trade_date = :trade_date"
    sql += " ORDER BY trade_date DESC LIMIT 1"

    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql), params).mappings().first()
    except Exception as exc:
        logger.warning("fetch_trend_reco_payload failed: %s", exc)
        return {}

    if not row:
        return {}

    payload = _coerce_payload_dict(row.get("payload"))
    db_trade_date = row.get("trade_date")
    if db_trade_date and not payload.get("trade_date"):
        payload["trade_date"] = db_trade_date.isoformat() if hasattr(db_trade_date, "isoformat") else str(db_trade_date)

    db_generated_at = row.get("generated_at")
    if db_generated_at and not payload.get("generated_at"):
        if hasattr(db_generated_at, "strftime"):
            payload["generated_at"] = db_generated_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            payload["generated_at"] = str(db_generated_at)

    if payload.get("universe_size") in (None, ""):
        payload["universe_size"] = to_int(row.get("universe_size")) or 0

    source_file = str(row.get("source_file") or "").strip()
    if source_file and not payload.get("source_file"):
        payload["source_file"] = source_file

    return payload



def list_trend_reco_runs(engine: Engine, limit: Optional[int] = None) -> list[dict]:
    sql = f"""
    SELECT trade_date, generated_at, universe_size, source_file
    FROM {TREND_RECO_RUNS_TABLE}
    ORDER BY trade_date DESC
    """
    params = {}
    if limit and int(limit) > 0:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).mappings().all()
    except Exception as exc:
        logger.warning("list_trend_reco_runs failed: %s", exc)
        return []

    results = []
    for row in rows:
        trade_date_value = row.get("trade_date")
        generated_at_value = row.get("generated_at")
        results.append(
            {
                "trade_date": trade_date_value.isoformat() if hasattr(trade_date_value, "isoformat") else str(trade_date_value or ""),
                "generated_at": generated_at_value.strftime("%Y-%m-%d %H:%M:%S") if hasattr(generated_at_value, "strftime") else str(generated_at_value or ""),
                "universe_size": to_int(row.get("universe_size")) or 0,
                "source_file": str(row.get("source_file") or ""),
            }
        )
    return results
