from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import timedelta
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .models import normalize_timestamp

PRICE_DAILY_VIEW = "vw_ts_stock_daily"
STOCK_BASIC_VIEW = "vw_ts_stock_basic"


def build_db_url():
    try:
        from src.sync_tushare_security_data import build_db_url as _sync_build_db_url

        return _sync_build_db_url()
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


def ensure_storage_objects(engine: Engine) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS em_author_posts (
            post_id BIGINT PRIMARY KEY,
            author_uid TEXT,
            post_publish_time TEXT,
            post_last_time TEXT,
            post_title TEXT,
            post_content TEXT,
            post_guba_code TEXT,
            post_guba_name TEXT,
            post_type INTEGER,
            post_pic_url_json TEXT,
            raw_payload_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_author_replies (
            reply_id BIGINT PRIMARY KEY,
            post_id BIGINT NOT NULL,
            reply_time TEXT,
            reply_text TEXT,
            reply_is_author INTEGER NOT NULL DEFAULT 0,
            raw_payload_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_author_post_images (
            post_id BIGINT NOT NULL,
            image_index INTEGER NOT NULL,
            image_url TEXT,
            ocr_status TEXT,
            ocr_text TEXT,
            ocr_provider TEXT,
            PRIMARY KEY (post_id, image_index)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_stock_mentions (
            mention_id TEXT PRIMARY KEY,
            author_uid TEXT,
            post_id BIGINT,
            reply_id BIGINT,
            ts_code TEXT,
            symbol TEXT,
            security_name TEXT,
            mention_time TEXT,
            source_type TEXT,
            direction TEXT,
            confidence_score REAL,
            target_text TEXT,
            risk_text TEXT,
            reason_text TEXT,
            rule_version TEXT,
            evidence_payload_json TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_stock_cycles (
            cycle_id TEXT PRIMARY KEY,
            author_uid TEXT,
            ts_code TEXT NOT NULL,
            cycle_open_time TEXT,
            cycle_close_time TEXT,
            cycle_status TEXT,
            open_mention_id TEXT,
            close_mention_id TEXT,
            close_reason TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_cycle_events (
            cycle_id TEXT NOT NULL,
            mention_id TEXT NOT NULL,
            event_sequence INTEGER NOT NULL,
            PRIMARY KEY (cycle_id, mention_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_cycle_scores (
            cycle_id TEXT PRIMARY KEY,
            ts_code TEXT NOT NULL,
            total_return REAL,
            max_drawdown REAL,
            hold_days INTEGER,
            exit_quality_2d INTEGER
        )
        """,
    ]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))


def replace_author_activity(engine: Engine, author_uid: str, posts: list[dict[str, Any]], image_records_by_post: dict[int, list[dict[str, Any]]] | None = None) -> None:
    ensure_storage_objects(engine)
    image_map = image_records_by_post or {}
    with engine.begin() as conn:
        for post in posts:
            conn.execute(text("DELETE FROM em_author_posts WHERE post_id = :post_id"), {"post_id": post.get("post_id")})
            conn.execute(text("DELETE FROM em_author_replies WHERE post_id = :post_id"), {"post_id": post.get("post_id")})
            conn.execute(text("DELETE FROM em_author_post_images WHERE post_id = :post_id"), {"post_id": post.get("post_id")})
            conn.execute(
                text(
                    """
                    INSERT INTO em_author_posts (
                        post_id, author_uid, post_publish_time, post_last_time, post_title, post_content,
                        post_guba_code, post_guba_name, post_type, post_pic_url_json, raw_payload_json
                    ) VALUES (
                        :post_id, :author_uid, :post_publish_time, :post_last_time, :post_title, :post_content,
                        :post_guba_code, :post_guba_name, :post_type, :post_pic_url_json, :raw_payload_json
                    )
                    """
                ),
                {
                    "post_id": post.get("post_id"),
                    "author_uid": author_uid,
                    "post_publish_time": post.get("post_publish_time"),
                    "post_last_time": post.get("post_last_time"),
                    "post_title": post.get("post_title"),
                    "post_content": post.get("post_content"),
                    "post_guba_code": (post.get("post_guba") or {}).get("stockbar_code"),
                    "post_guba_name": (post.get("post_guba") or {}).get("stockbar_name"),
                    "post_type": post.get("post_type"),
                    "post_pic_url_json": json.dumps(post.get("post_pic_url") or [], ensure_ascii=False),
                    "raw_payload_json": json.dumps(post.get("raw_payload") or post, ensure_ascii=False),
                },
            )
            for reply in post.get("reply_list") or []:
                conn.execute(
                    text(
                        """
                        INSERT INTO em_author_replies (
                            reply_id, post_id, reply_time, reply_text, reply_is_author, raw_payload_json
                        ) VALUES (
                            :reply_id, :post_id, :reply_time, :reply_text, :reply_is_author, :raw_payload_json
                        )
                        """
                    ),
                    {
                        "reply_id": reply.get("reply_id"),
                        "post_id": post.get("post_id"),
                        "reply_time": reply.get("reply_time"),
                        "reply_text": reply.get("reply_text"),
                        "reply_is_author": 1 if reply.get("reply_is_author") else 0,
                        "raw_payload_json": json.dumps(reply, ensure_ascii=False),
                    },
                )
            for image_record in image_map.get(int(post.get("post_id") or 0), []):
                conn.execute(
                    text(
                        """
                        INSERT INTO em_author_post_images (
                            post_id, image_index, image_url, ocr_status, ocr_text, ocr_provider
                        ) VALUES (
                            :post_id, :image_index, :image_url, :ocr_status, :ocr_text, :ocr_provider
                        )
                        """
                    ),
                    {
                        "post_id": post.get("post_id"),
                        "image_index": image_record.get("image_index"),
                        "image_url": image_record.get("image_url"),
                        "ocr_status": image_record.get("ocr_status"),
                        "ocr_text": image_record.get("ocr_text"),
                        "ocr_provider": image_record.get("ocr_provider"),
                    },
                )


def replace_all_mentions(engine: Engine, mentions: list[dict[str, Any]]) -> None:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM em_stock_mentions"))
        for mention in mentions:
            conn.execute(
                text(
                    """
                    INSERT INTO em_stock_mentions (
                        mention_id, author_uid, post_id, reply_id, ts_code, symbol, security_name,
                        mention_time, source_type, direction, confidence_score, target_text, risk_text,
                        reason_text, rule_version, evidence_payload_json
                    ) VALUES (
                        :mention_id, :author_uid, :post_id, :reply_id, :ts_code, :symbol, :security_name,
                        :mention_time, :source_type, :direction, :confidence_score, :target_text, :risk_text,
                        :reason_text, :rule_version, :evidence_payload_json
                    )
                    """
                ),
                {
                    **mention,
                    "evidence_payload_json": json.dumps(mention.get("evidence_payload") or {}, ensure_ascii=False),
                },
            )


def load_all_mentions(engine: Engine) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT mention_id, author_uid, post_id, reply_id, ts_code, symbol, security_name, mention_time,
                       source_type, direction, confidence_score, target_text, risk_text, reason_text, rule_version
                FROM em_stock_mentions
                ORDER BY ts_code ASC, mention_time ASC
                """
            )
        ).mappings()
        return [dict(row) for row in rows]


def replace_all_cycles(engine: Engine, cycles: list[dict[str, Any]], scores: list[dict[str, Any]]) -> None:
    ensure_storage_objects(engine)
    score_by_cycle = {item["cycle_id"]: item for item in scores}
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM em_cycle_events"))
        conn.execute(text("DELETE FROM em_cycle_scores"))
        conn.execute(text("DELETE FROM em_stock_cycles"))
        for cycle in cycles:
            conn.execute(
                text(
                    """
                    INSERT INTO em_stock_cycles (
                        cycle_id, author_uid, ts_code, cycle_open_time, cycle_close_time, cycle_status,
                        open_mention_id, close_mention_id, close_reason
                    ) VALUES (
                        :cycle_id, :author_uid, :ts_code, :cycle_open_time, :cycle_close_time, :cycle_status,
                        :open_mention_id, :close_mention_id, :close_reason
                    )
                    """
                ),
                {
                    "cycle_id": cycle.get("cycle_id"),
                    "author_uid": cycle.get("author_uid"),
                    "ts_code": cycle.get("ts_code"),
                    "cycle_open_time": cycle.get("cycle_open_time"),
                    "cycle_close_time": cycle.get("cycle_close_time"),
                    "cycle_status": cycle.get("cycle_status"),
                    "open_mention_id": cycle.get("open_mention_id"),
                    "close_mention_id": cycle.get("close_mention_id"),
                    "close_reason": cycle.get("close_reason"),
                },
            )
            for index, mention_id in enumerate(cycle.get("mention_ids") or [], start=1):
                conn.execute(
                    text(
                        """
                        INSERT INTO em_cycle_events (cycle_id, mention_id, event_sequence)
                        VALUES (:cycle_id, :mention_id, :event_sequence)
                        """
                    ),
                    {
                        "cycle_id": cycle.get("cycle_id"),
                        "mention_id": mention_id,
                        "event_sequence": index,
                    },
                )
            score = score_by_cycle.get(cycle.get("cycle_id"))
            if score:
                conn.execute(
                    text(
                        """
                        INSERT INTO em_cycle_scores (
                            cycle_id, ts_code, total_return, max_drawdown, hold_days, exit_quality_2d
                        ) VALUES (
                            :cycle_id, :ts_code, :total_return, :max_drawdown, :hold_days, :exit_quality_2d
                        )
                        """
                    ),
                    {
                        "cycle_id": score.get("cycle_id"),
                        "ts_code": score.get("ts_code"),
                        "total_return": score.get("total_return"),
                        "max_drawdown": score.get("max_drawdown"),
                        "hold_days": score.get("hold_days"),
                        "exit_quality_2d": None if score.get("exit_quality_2d") is None else (1 if score.get("exit_quality_2d") else 0),
                    },
                )


def list_cycles_with_scores(engine: Engine) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                WITH event_stats AS (
                    SELECT cycle_id, COUNT(*) AS event_count
                    FROM em_cycle_events
                    GROUP BY cycle_id
                ),
                latest_event_keys AS (
                    SELECT cycle_id, MAX(event_sequence) AS max_event_sequence
                    FROM em_cycle_events
                    GROUP BY cycle_id
                ),
                latest_events AS (
                    SELECT
                        e.cycle_id,
                        m.mention_time AS latest_mention_time,
                        m.direction AS latest_direction,
                        m.source_type AS latest_source_type,
                        m.reason_text AS latest_reason_text,
                        m.security_name AS latest_security_name
                    FROM em_cycle_events e
                    JOIN latest_event_keys k
                      ON k.cycle_id = e.cycle_id
                     AND k.max_event_sequence = e.event_sequence
                    JOIN em_stock_mentions m
                      ON m.mention_id = e.mention_id
                )
                SELECT c.cycle_id, c.author_uid, c.ts_code, c.cycle_open_time, c.cycle_close_time,
                       c.cycle_status, c.open_mention_id, c.close_mention_id, c.close_reason,
                       COALESCE(NULLIF(basic.name, ''), NULLIF(le.latest_security_name, ''), c.ts_code) AS security_name,
                       s.total_return, s.max_drawdown, s.hold_days, s.exit_quality_2d,
                       COALESCE(es.event_count, 0) AS event_count,
                       le.latest_mention_time, le.latest_direction, le.latest_source_type, le.latest_reason_text
                FROM em_stock_cycles c
                LEFT JOIN em_cycle_scores s ON s.cycle_id = c.cycle_id
                LEFT JOIN event_stats es ON es.cycle_id = c.cycle_id
                LEFT JOIN latest_events le ON le.cycle_id = c.cycle_id
                LEFT JOIN vw_ts_stock_basic basic ON basic.ts_code = c.ts_code
                ORDER BY c.cycle_open_time DESC, c.cycle_id DESC
                """
            )
        ).mappings()
        result = []
        for row in rows:
            item = dict(row)
            if item.get("exit_quality_2d") is not None:
                item["exit_quality_2d"] = bool(item["exit_quality_2d"])
            result.append(item)
        return result


def get_author_tracking_metadata(engine: Engine) -> dict[str, Any]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM em_author_posts) AS post_count,
                    (SELECT COUNT(*) FROM em_stock_mentions) AS mention_count,
                    (SELECT COUNT(DISTINCT author_uid) FROM em_author_posts WHERE author_uid IS NOT NULL AND author_uid <> '') AS author_count,
                    (SELECT MAX(post_publish_time) FROM em_author_posts) AS last_post_time,
                    (SELECT MAX(mention_time) FROM em_stock_mentions) AS last_mention_time
                """
            )
        ).mappings().one()
        return dict(row)


def list_cycle_events(engine: Engine, cycle_id: str) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT cycle_id, mention_id, event_sequence
                FROM em_cycle_events
                WHERE cycle_id = :cycle_id
                ORDER BY event_sequence ASC
                """
            ),
            {"cycle_id": cycle_id},
        ).mappings()
        return [dict(row) for row in rows]


def list_cycle_event_details(engine: Engine, cycle_id: str) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    e.cycle_id,
                    e.event_sequence,
                    m.mention_id,
                    m.author_uid,
                    m.post_id,
                    m.reply_id,
                    m.ts_code,
                    m.symbol,
                    m.security_name,
                    m.mention_time,
                    m.source_type,
                    m.direction,
                    m.confidence_score,
                    m.target_text,
                    m.risk_text,
                    m.reason_text,
                    m.rule_version,
                    p.post_title,
                    p.post_content,
                    p.post_publish_time,
                    p.post_guba_name,
                    r.reply_text
                FROM em_cycle_events e
                JOIN em_stock_mentions m
                  ON m.mention_id = e.mention_id
                LEFT JOIN em_author_posts p
                  ON p.post_id = m.post_id
                LEFT JOIN em_author_replies r
                  ON r.reply_id = m.reply_id
                WHERE e.cycle_id = :cycle_id
                ORDER BY e.event_sequence ASC
                """
            ),
            {"cycle_id": cycle_id},
        ).mappings()
        return [dict(row) for row in rows]


def list_cycle_price_history(
    engine: Engine,
    ts_code: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    lookback_days: int = 10,
    lookahead_days: int = 20,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"ts_code": ts_code}
    conditions = ["ts_code = :ts_code"]

    if start_date:
        start_value = normalize_timestamp(start_date).date() - timedelta(days=max(int(lookback_days), 0))
        conditions.append("trade_date >= :start_date")
        params["start_date"] = start_value.isoformat()

    if end_date:
        end_value = normalize_timestamp(end_date).date() + timedelta(days=max(int(lookahead_days), 0))
        conditions.append("trade_date <= :end_date")
        params["end_date"] = end_value.isoformat()

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT trade_date, open, high, low, close
                FROM {PRICE_DAILY_VIEW}
                WHERE {' AND '.join(conditions)}
                ORDER BY trade_date ASC
                """
            ),
            params,
        ).mappings()
        return [dict(row) for row in rows]


def build_author_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cycle_count = len(rows)
    active_count = sum(1 for row in rows if row.get("cycle_status") in {"active", "trimmed"})
    closed_rows = [row for row in rows if row.get("cycle_status") in {"closed", "expired"}]
    closed_count = len(closed_rows)
    scored_closed_rows = [row for row in closed_rows if row.get("total_return") is not None]
    win_count = sum(1 for row in scored_closed_rows if float(row.get("total_return") or 0) > 0)
    avg_return_values = [float(row["total_return"]) for row in scored_closed_rows]
    exit_evaluated_rows = [row for row in closed_rows if row.get("exit_quality_2d") is not None]
    effective_exit_count = sum(1 for row in exit_evaluated_rows if bool(row.get("exit_quality_2d")) is True)
    return {
        "cycle_count": cycle_count,
        "active_count": active_count,
        "closed_count": closed_count,
        "scored_closed_count": len(scored_closed_rows),
        "win_count": win_count,
        "win_rate": (win_count / len(scored_closed_rows)) if scored_closed_rows else 0.0,
        "avg_return": (sum(avg_return_values) / len(avg_return_values)) if avg_return_values else None,
        "effective_exit_rate": (effective_exit_count / len(exit_evaluated_rows)) if exit_evaluated_rows else None,
    }


def build_cycle_event_map(cycles: list[dict[str, Any]]) -> dict[str, list[str]]:
    mapping: defaultdict[str, list[str]] = defaultdict(list)
    for cycle in cycles:
        for mention_id in cycle.get("mention_ids") or []:
            mapping[cycle["cycle_id"]].append(mention_id)
    return dict(mapping)
