from __future__ import annotations

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import create_engine, inspect, text
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


def _ensure_table_columns(engine: Engine, table_name: str, column_definitions: dict[str, str]) -> None:
    try:
        existing_columns = {column["name"] for column in inspect(engine).get_columns(table_name)}
    except Exception:
        return
    missing_columns = [(name, definition) for name, definition in column_definitions.items() if name not in existing_columns]
    if not missing_columns:
        return

    with engine.begin() as conn:
        for column_name, column_definition in missing_columns:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"))


def _resolve_ocr_updated_at(ocr_status: Any, ocr_updated_at: Any = None) -> str | None:
    explicit_value = str(ocr_updated_at or "").strip()
    if explicit_value:
        return explicit_value

    status_value = str(ocr_status or "").strip().lower()
    if status_value and status_value not in {"pending", "skipped"}:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return None


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
            ocr_updated_at TEXT,
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
            benchmark_return REAL,
            excess_return REAL,
            max_drawdown REAL,
            hold_days INTEGER,
            exit_quality_2d INTEGER,
            exit_quality_5d INTEGER,
            exit_quality_10d INTEGER,
            exit_quality_20d INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_mention_overrides (
            mention_id TEXT PRIMARY KEY,
            override_ts_code TEXT,
            override_direction TEXT,
            is_excluded INTEGER NOT NULL DEFAULT 0,
            force_new_cycle INTEGER NOT NULL DEFAULT 0,
            override_note TEXT,
            updated_at TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS em_author_score_snapshots (
            author_uid TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            cycle_count INTEGER,
            active_count INTEGER,
            closed_count INTEGER,
            scored_closed_count INTEGER,
            win_count INTEGER,
            win_rate REAL,
            avg_return REAL,
            avg_excess_return REAL,
            avg_hold_days REAL,
            payoff_ratio REAL,
            effective_exit_rate REAL,
            updated_at TEXT,
            PRIMARY KEY (author_uid, snapshot_date)
        )
        """,
    ]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    _ensure_table_columns(
        engine,
        "em_author_post_images",
        {
            "ocr_updated_at": "TEXT",
        },
    )
    _ensure_table_columns(
        engine,
        "em_cycle_scores",
        {
            "benchmark_return": "REAL",
            "excess_return": "REAL",
            "exit_quality_5d": "INTEGER",
            "exit_quality_10d": "INTEGER",
            "exit_quality_20d": "INTEGER",
        },
    )
    _ensure_table_columns(
        engine,
        "em_author_score_snapshots",
        {
            "avg_excess_return": "REAL",
            "avg_hold_days": "REAL",
            "payoff_ratio": "REAL",
        },
    )


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
                            post_id, image_index, image_url, ocr_status, ocr_text, ocr_provider, ocr_updated_at
                        ) VALUES (
                            :post_id, :image_index, :image_url, :ocr_status, :ocr_text, :ocr_provider, :ocr_updated_at
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
                        "ocr_updated_at": _resolve_ocr_updated_at(
                            image_record.get("ocr_status"),
                            image_record.get("ocr_updated_at"),
                        ),
                    },
                )


def load_existing_post_payloads(engine: Engine, author_uid: str, post_ids: list[int | str]) -> dict[int, dict[str, Any]]:
    ensure_storage_objects(engine)
    normalized_post_ids = [int(post_id) for post_id in post_ids if str(post_id or "").strip()]
    if not normalized_post_ids:
        return {}

    post_id_params = {f"post_id_{index}": post_id for index, post_id in enumerate(normalized_post_ids)}
    post_id_placeholders = ", ".join(f":post_id_{index}" for index in range(len(normalized_post_ids)))
    params: dict[str, Any] = {"author_uid": author_uid, **post_id_params}

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT post_id, raw_payload_json
                FROM em_author_posts
                WHERE author_uid = :author_uid
                  AND post_id IN ({post_id_placeholders})
                """
            ),
            params,
        ).mappings()

        payloads: dict[int, dict[str, Any]] = {}
        for row in rows:
            try:
                payloads[int(row["post_id"])] = json.loads(row["raw_payload_json"] or "{}")
            except Exception:
                continue
        return payloads


def load_author_posts(engine: Engine, author_uid: str) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT raw_payload_json
                FROM em_author_posts
                WHERE author_uid = :author_uid
                ORDER BY post_publish_time DESC, post_id DESC
                """
            ),
            {"author_uid": author_uid},
        ).mappings()

        posts: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row["raw_payload_json"] or "{}")
            except Exception:
                continue
            if isinstance(payload, dict):
                posts.append(payload)
        return posts


def load_author_image_records_map(engine: Engine, author_uid: str) -> dict[int, list[dict[str, Any]]]:
    ensure_storage_objects(engine)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT i.post_id, i.image_index, i.image_url, i.ocr_status, i.ocr_text, i.ocr_provider, i.ocr_updated_at
                FROM em_author_post_images i
                JOIN em_author_posts p
                  ON p.post_id = i.post_id
                WHERE p.author_uid = :author_uid
                ORDER BY i.post_id ASC, i.image_index ASC
                """
            ),
            {"author_uid": author_uid},
        ).mappings()

        mapping: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            mapping[int(row["post_id"])].append(
                {
                    "post_id": int(row["post_id"]),
                    "image_index": row["image_index"],
                    "image_url": row["image_url"],
                    "ocr_status": row["ocr_status"],
                    "ocr_text": row["ocr_text"],
                    "ocr_provider": row["ocr_provider"],
                    "ocr_updated_at": row["ocr_updated_at"],
                }
            )
        return dict(mapping)


def load_post_image_records_map(
    engine: Engine,
    post_ids: list[int | str],
) -> dict[int, list[dict[str, Any]]]:
    ensure_storage_objects(engine)
    normalized_post_ids = [int(post_id) for post_id in post_ids if str(post_id or "").strip()]
    if not normalized_post_ids:
        return {}

    id_params = {f"post_id_{index}": post_id for index, post_id in enumerate(normalized_post_ids)}
    id_placeholders = ", ".join(f":post_id_{index}" for index in range(len(normalized_post_ids)))
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT post_id, image_index, image_url, ocr_status, ocr_text, ocr_provider, ocr_updated_at
                FROM em_author_post_images
                WHERE post_id IN ({id_placeholders})
                ORDER BY post_id ASC, image_index ASC
                """
            ),
            id_params,
        ).mappings()

        mapping: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            mapping[int(row["post_id"])].append(
                {
                    "post_id": int(row["post_id"]),
                    "image_index": row["image_index"],
                    "image_url": row["image_url"],
                    "ocr_status": row["ocr_status"],
                    "ocr_text": row["ocr_text"],
                    "ocr_provider": row["ocr_provider"],
                    "ocr_updated_at": row["ocr_updated_at"],
                }
            )
        return dict(mapping)


def list_pending_author_images(
    engine: Engine,
    author_uid: str,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    author_uid_text = str(author_uid or "").strip()
    if not author_uid_text:
        return []

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    p.author_uid,
                    i.post_id,
                    i.image_index,
                    i.image_url,
                    i.ocr_status,
                    i.ocr_text,
                    i.ocr_provider
                FROM em_author_post_images i
                JOIN em_author_posts p
                  ON p.post_id = i.post_id
                WHERE p.author_uid = :author_uid
                  AND COALESCE(i.ocr_status, '') = 'pending'
                ORDER BY p.post_publish_time DESC, i.post_id DESC, i.image_index ASC
                LIMIT :limit
                """
            ),
            {
                "author_uid": author_uid_text,
                "limit": max(int(limit or 0), 1),
            },
        ).mappings()
        return [dict(row) for row in rows]


def count_pending_author_images(engine: Engine, author_uid: str) -> int:
    ensure_storage_objects(engine)
    author_uid_text = str(author_uid or "").strip()
    if not author_uid_text:
        return 0

    with engine.begin() as conn:
        return int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM em_author_post_images i
                    JOIN em_author_posts p
                      ON p.post_id = i.post_id
                    WHERE p.author_uid = :author_uid
                      AND COALESCE(i.ocr_status, '') = 'pending'
                    """
                ),
                {"author_uid": author_uid_text},
            ).scalar_one()
        )


def update_post_image_ocr_result(
    engine: Engine,
    post_id: int | str,
    image_index: int,
    *,
    image_url: str | None = None,
    ocr_status: str | None = None,
    ocr_text: str | None = None,
    ocr_provider: str | None = None,
    ocr_updated_at: str | None = None,
) -> None:
    ensure_storage_objects(engine)
    resolved_ocr_updated_at = _resolve_ocr_updated_at(ocr_status, ocr_updated_at)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE em_author_post_images
                SET image_url = COALESCE(:image_url, image_url),
                    ocr_status = :ocr_status,
                    ocr_text = :ocr_text,
                    ocr_provider = :ocr_provider,
                    ocr_updated_at = :ocr_updated_at
                WHERE post_id = :post_id
                  AND image_index = :image_index
                """
            ),
            {
                "post_id": int(post_id),
                "image_index": int(image_index),
                "image_url": image_url,
                "ocr_status": ocr_status,
                "ocr_text": ocr_text,
                "ocr_provider": ocr_provider,
                "ocr_updated_at": resolved_ocr_updated_at,
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
                            cycle_id, ts_code, total_return, benchmark_return, excess_return,
                            max_drawdown, hold_days, exit_quality_2d, exit_quality_5d,
                            exit_quality_10d, exit_quality_20d
                        ) VALUES (
                            :cycle_id, :ts_code, :total_return, :benchmark_return, :excess_return,
                            :max_drawdown, :hold_days, :exit_quality_2d, :exit_quality_5d,
                            :exit_quality_10d, :exit_quality_20d
                        )
                        """
                    ),
                    {
                        "cycle_id": score.get("cycle_id"),
                        "ts_code": score.get("ts_code"),
                        "total_return": score.get("total_return"),
                        "benchmark_return": score.get("benchmark_return"),
                        "excess_return": score.get("excess_return"),
                        "max_drawdown": score.get("max_drawdown"),
                        "hold_days": score.get("hold_days"),
                        "exit_quality_2d": None if score.get("exit_quality_2d") is None else (1 if score.get("exit_quality_2d") else 0),
                        "exit_quality_5d": None if score.get("exit_quality_5d") is None else (1 if score.get("exit_quality_5d") else 0),
                        "exit_quality_10d": None if score.get("exit_quality_10d") is None else (1 if score.get("exit_quality_10d") else 0),
                        "exit_quality_20d": None if score.get("exit_quality_20d") is None else (1 if score.get("exit_quality_20d") else 0),
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
                       s.total_return, s.benchmark_return, s.excess_return, s.max_drawdown, s.hold_days,
                       s.exit_quality_2d, s.exit_quality_5d, s.exit_quality_10d, s.exit_quality_20d,
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
            for key in ("exit_quality_2d", "exit_quality_5d", "exit_quality_10d", "exit_quality_20d"):
                if item.get(key) is not None:
                    item[key] = bool(item[key])
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
                    (SELECT COUNT(*) FROM em_author_post_images WHERE COALESCE(ocr_status, '') = 'pending') AS pending_image_count,
                    (
                        SELECT COUNT(*)
                        FROM em_author_post_images
                        WHERE COALESCE(ocr_status, '') NOT IN ('', 'pending', 'skipped')
                    ) AS ocr_processed_image_count,
                    (SELECT MAX(post_publish_time) FROM em_author_posts) AS last_post_time,
                    (SELECT MAX(mention_time) FROM em_stock_mentions) AS last_mention_time,
                    (SELECT MAX(ocr_updated_at) FROM em_author_post_images) AS last_ocr_update_time
                """
            )
        ).mappings().one()
        return dict(row)


def upsert_mention_override(
    engine: Engine,
    mention_id: str,
    *,
    override_ts_code: str | None = None,
    override_direction: str | None = None,
    is_excluded: bool = False,
    force_new_cycle: bool = False,
    override_note: str | None = None,
) -> None:
    ensure_storage_objects(engine)
    mention_id_text = str(mention_id or "").strip()
    if not mention_id_text:
        raise ValueError("mention_id is required")

    normalized_ts_code = str(override_ts_code or "").strip().upper() or None
    normalized_direction = str(override_direction or "").strip() or None
    normalized_note = str(override_note or "").strip() or None

    should_delete = (
        normalized_ts_code is None
        and normalized_direction is None
        and not bool(is_excluded)
        and not bool(force_new_cycle)
        and normalized_note is None
    )

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM em_mention_overrides WHERE mention_id = :mention_id"), {"mention_id": mention_id_text})
        if should_delete:
            return
        conn.execute(
            text(
                """
                INSERT INTO em_mention_overrides (
                    mention_id, override_ts_code, override_direction, is_excluded,
                    force_new_cycle, override_note, updated_at
                ) VALUES (
                    :mention_id, :override_ts_code, :override_direction, :is_excluded,
                    :force_new_cycle, :override_note, :updated_at
                )
                """
            ),
            {
                "mention_id": mention_id_text,
                "override_ts_code": normalized_ts_code,
                "override_direction": normalized_direction,
                "is_excluded": 1 if is_excluded else 0,
                "force_new_cycle": 1 if force_new_cycle else 0,
                "override_note": normalized_note,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )


def load_mention_overrides_map(engine: Engine, mention_ids: list[str] | None = None) -> dict[str, dict[str, Any]]:
    ensure_storage_objects(engine)
    params: dict[str, Any] = {}
    where_clause = ""
    if mention_ids:
        normalized_ids = [str(mention_id or "").strip() for mention_id in mention_ids if str(mention_id or "").strip()]
        if normalized_ids:
            id_params = {f"mention_id_{index}": mention_id for index, mention_id in enumerate(normalized_ids)}
            params.update(id_params)
            id_placeholders = ", ".join(f":mention_id_{index}" for index in range(len(normalized_ids)))
            where_clause = f"WHERE mention_id IN ({id_placeholders})"

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT mention_id, override_ts_code, override_direction, is_excluded,
                       force_new_cycle, override_note, updated_at
                FROM em_mention_overrides
                {where_clause}
                """
            ),
            params,
        ).mappings()

        override_map: dict[str, dict[str, Any]] = {}
        for row in rows:
            override_map[str(row["mention_id"])] = {
                "mention_id": row["mention_id"],
                "override_ts_code": row["override_ts_code"],
                "override_direction": row["override_direction"],
                "is_excluded": bool(row["is_excluded"]),
                "force_new_cycle": bool(row["force_new_cycle"]),
                "override_note": row["override_note"],
                "updated_at": row["updated_at"],
            }
        return override_map


def upsert_author_score_snapshot(
    engine: Engine,
    author_uid: str,
    *,
    snapshot_date: str,
    summary: dict[str, Any],
) -> None:
    ensure_storage_objects(engine)
    author_uid_text = str(author_uid or "").strip()
    if not author_uid_text:
        raise ValueError("author_uid is required")

    normalized_snapshot_date = normalize_timestamp(snapshot_date).date().isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM em_author_score_snapshots
                WHERE author_uid = :author_uid AND snapshot_date = :snapshot_date
                """
            ),
            {
                "author_uid": author_uid_text,
                "snapshot_date": normalized_snapshot_date,
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO em_author_score_snapshots (
                    author_uid, snapshot_date, cycle_count, active_count, closed_count,
                    scored_closed_count, win_count, win_rate, avg_return,
                    avg_excess_return, avg_hold_days, payoff_ratio,
                    effective_exit_rate, updated_at
                ) VALUES (
                    :author_uid, :snapshot_date, :cycle_count, :active_count, :closed_count,
                    :scored_closed_count, :win_count, :win_rate, :avg_return,
                    :avg_excess_return, :avg_hold_days, :payoff_ratio,
                    :effective_exit_rate, :updated_at
                )
                """
            ),
            {
                "author_uid": author_uid_text,
                "snapshot_date": normalized_snapshot_date,
                "cycle_count": int(summary.get("cycle_count") or 0),
                "active_count": int(summary.get("active_count") or 0),
                "closed_count": int(summary.get("closed_count") or 0),
                "scored_closed_count": int(summary.get("scored_closed_count") or 0),
                "win_count": int(summary.get("win_count") or 0),
                "win_rate": summary.get("win_rate"),
                "avg_return": summary.get("avg_return"),
                "avg_excess_return": summary.get("avg_excess_return"),
                "avg_hold_days": summary.get("avg_hold_days"),
                "payoff_ratio": summary.get("payoff_ratio"),
                "effective_exit_rate": summary.get("effective_exit_rate"),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        )


def list_author_score_snapshots(
    engine: Engine,
    author_uid: str,
    *,
    limit: int = 60,
) -> list[dict[str, Any]]:
    ensure_storage_objects(engine)
    author_uid_text = str(author_uid or "").strip()
    if not author_uid_text:
        return []

    effective_limit = max(int(limit or 0), 1)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    author_uid,
                    snapshot_date,
                    cycle_count,
                    active_count,
                    closed_count,
                    scored_closed_count,
                    win_count,
                    win_rate,
                    avg_return,
                    avg_excess_return,
                    avg_hold_days,
                    payoff_ratio,
                    effective_exit_rate,
                    updated_at
                FROM em_author_score_snapshots
                WHERE author_uid = :author_uid
                ORDER BY snapshot_date DESC
                LIMIT :limit
                """
            ),
            {
                "author_uid": author_uid_text,
                "limit": effective_limit,
            },
        ).mappings()
        result = [dict(row) for row in rows]
        result.sort(key=lambda item: str(item.get("snapshot_date") or ""))
        return result


def load_price_history_by_codes(
    engine: Engine,
    ts_codes: list[str],
    *,
    start_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    normalized_codes = [str(code or "").strip() for code in ts_codes if str(code or "").strip()]
    if not normalized_codes:
        return {}

    code_params = {f"code_{index}": code for index, code in enumerate(normalized_codes)}
    code_placeholders = ", ".join(f":code_{index}" for index in range(len(normalized_codes)))
    conditions = [f"ts_code IN ({code_placeholders})"]
    params: dict[str, Any] = dict(code_params)

    if start_date:
        conditions.append("trade_date >= :start_date")
        params["start_date"] = normalize_timestamp(start_date).date().isoformat()

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT ts_code, trade_date, close
                FROM {PRICE_DAILY_VIEW}
                WHERE {' AND '.join(conditions)}
                ORDER BY ts_code ASC, trade_date ASC
                """
            ),
            params,
        ).mappings()

        history_by_code: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            trade_date_value = row.get("trade_date")
            if hasattr(trade_date_value, "isoformat"):
                trade_date_text = trade_date_value.isoformat()
            else:
                trade_date_text = str(trade_date_value)
            history_by_code[str(row["ts_code"])].append(
                {
                    "trade_date": trade_date_text,
                    "close": float(row["close"]),
                }
            )
        return dict(history_by_code)


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
                    r.reply_text,
                    o.override_ts_code,
                    o.override_direction,
                    o.is_excluded,
                    o.force_new_cycle,
                    o.override_note
                FROM em_cycle_events e
                JOIN em_stock_mentions m
                  ON m.mention_id = e.mention_id
                LEFT JOIN em_author_posts p
                  ON p.post_id = m.post_id
                LEFT JOIN em_author_replies r
                  ON r.reply_id = m.reply_id
                LEFT JOIN em_mention_overrides o
                  ON o.mention_id = m.mention_id
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
    def _pick_exit_quality(row: dict[str, Any]) -> bool | None:
        for key in ("exit_quality_10d", "exit_quality_5d", "exit_quality_20d", "exit_quality_2d"):
            value = row.get(key)
            if value is not None:
                return bool(value)
        return None

    cycle_count = len(rows)
    active_count = sum(1 for row in rows if row.get("cycle_status") in {"active", "trimmed"})
    closed_rows = [row for row in rows if row.get("cycle_status") in {"closed", "expired"}]
    closed_count = len(closed_rows)
    scored_closed_rows = [row for row in closed_rows if row.get("total_return") is not None]
    win_count = sum(1 for row in scored_closed_rows if float(row.get("total_return") or 0) > 0)
    avg_return_values = [float(row["total_return"]) for row in scored_closed_rows]
    avg_excess_return_values = [float(row["excess_return"]) for row in scored_closed_rows if row.get("excess_return") is not None]
    hold_day_values = [float(row["hold_days"]) for row in closed_rows if row.get("hold_days") is not None]
    positive_return_values = [float(row["total_return"]) for row in scored_closed_rows if float(row.get("total_return") or 0) > 0]
    negative_return_values = [abs(float(row["total_return"])) for row in scored_closed_rows if float(row.get("total_return") or 0) < 0]
    exit_signals = [_pick_exit_quality(row) for row in closed_rows]
    effective_exit_values = [value for value in exit_signals if value is not None]
    effective_exit_count = sum(1 for value in effective_exit_values if value is True)
    return {
        "cycle_count": cycle_count,
        "active_count": active_count,
        "closed_count": closed_count,
        "scored_closed_count": len(scored_closed_rows),
        "win_count": win_count,
        "win_rate": (win_count / len(scored_closed_rows)) if scored_closed_rows else 0.0,
        "avg_return": (sum(avg_return_values) / len(avg_return_values)) if avg_return_values else None,
        "avg_excess_return": (sum(avg_excess_return_values) / len(avg_excess_return_values)) if avg_excess_return_values else None,
        "avg_hold_days": (sum(hold_day_values) / len(hold_day_values)) if hold_day_values else None,
        "payoff_ratio": (
            (sum(positive_return_values) / len(positive_return_values)) / (sum(negative_return_values) / len(negative_return_values))
            if positive_return_values and negative_return_values
            else None
        ),
        "effective_exit_rate": (effective_exit_count / len(effective_exit_values)) if effective_exit_values else None,
    }


def build_cycle_event_map(cycles: list[dict[str, Any]]) -> dict[str, list[str]]:
    mapping: defaultdict[str, list[str]] = defaultdict(list)
    for cycle in cycles:
        for mention_id in cycle.get("mention_ids") or []:
            mapping[cycle["cycle_id"]].append(mention_id)
    return dict(mapping)
