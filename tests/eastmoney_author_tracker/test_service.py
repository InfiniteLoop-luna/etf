import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from src.eastmoney_author_tracker.cycles import DEFAULT_BENCHMARK_TS_CODE, score_cycles
from src.eastmoney_author_tracker.extract import extract_stock_mentions
from src.eastmoney_author_tracker.service import enrich_pending_author_images
from src.eastmoney_author_tracker.service import rebuild_author_tracking_from_archive
from src.eastmoney_author_tracker.service import sync_author_activity
from src.eastmoney_author_tracker.store import (
    load_author_posts,
    list_author_score_snapshots,
    replace_author_activity,
    upsert_mention_override,
)


def _build_sqlite_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


class SyncAuthorActivityTests(unittest.TestCase):
    def test_sync_author_activity_marks_post_images_pending_when_ocr_is_deferred(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        sample_posts = [
            {
                "post_id": 1001,
                "post_title": "看好 600030",
                "post_content": "图片里有补充观点。",
                "post_publish_time": "2026-05-08 14:57:43",
                "post_last_time": "2026-05-08 14:57:43",
                "post_type": 0,
                "post_pic_url": ["https://example.com/ocr-1.png"],
                "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
                "post_user": {"user_id": "4348595203199492"},
                "reply_list": [],
            }
        ]

        def fake_fetch_page(page_num: int):
            return sample_posts if page_num == 1 else []

        sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            max_pages=2,
        )

        with engine.begin() as conn:
            image_rows = conn.execute(
                text(
                    """
                    SELECT image_url, ocr_status, ocr_text, ocr_provider
                    FROM em_author_post_images
                    ORDER BY image_index ASC
                    """
                )
            ).mappings().all()

        self.assertEqual(len(image_rows), 1)
        self.assertEqual(image_rows[0]["image_url"], "https://example.com/ocr-1.png")
        self.assertEqual(image_rows[0]["ocr_status"], "pending")
        self.assertEqual(image_rows[0]["ocr_text"], "")
        self.assertEqual(image_rows[0]["ocr_provider"], "deferred")

    def test_sync_author_activity_keeps_existing_ocr_rows_for_unchanged_posts(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        existing_post = {
            "post_id": 1001,
            "post_title": "盘中记录",
            "post_content": "正文没有直接写代码。",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-08 14:57:43",
            "post_type": 0,
            "post_pic_url": ["https://example.com/ocr-keep.png"],
            "post_guba": {},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        replace_author_activity(
            engine,
            "4348595203199492",
            [existing_post],
            image_records_by_post={
                1001: [
                    {
                        "image_index": 0,
                        "image_url": "https://example.com/ocr-keep.png",
                        "ocr_status": "ok",
                        "ocr_text": "继续看好 600030",
                        "ocr_provider": "fake-ocr",
                        "ocr_updated_at": "2026-05-12 09:30:00",
                    }
                ]
            },
        )

        def fake_fetch_page(page_num: int):
            return [existing_post] if page_num == 1 else []

        result = sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            max_pages=2,
            unchanged_post_stop_count=1,
        )

        with engine.begin() as conn:
            image_rows = conn.execute(
                text(
                    """
                    SELECT ocr_status, ocr_text, ocr_provider, ocr_updated_at
                    FROM em_author_post_images
                    ORDER BY image_index ASC
                    """
                )
            ).mappings().all()

        self.assertEqual(result["mention_count"], 1)
        self.assertEqual(image_rows[0]["ocr_status"], "ok")
        self.assertEqual(image_rows[0]["ocr_text"], "继续看好 600030")
        self.assertEqual(image_rows[0]["ocr_provider"], "fake-ocr")
        self.assertEqual(image_rows[0]["ocr_updated_at"], "2026-05-12 09:30:00")

    def test_score_cycles_calculates_excess_return_and_multi_window_exit_quality(self):
        cycles = [
            {
                "cycle_id": "600030-20260508145743-1",
                "ts_code": "600030.SH",
                "cycle_status": "closed",
                "cycle_open_time": "2026-05-08 14:57:43",
                "cycle_close_time": "2026-05-12 10:00:00",
            }
        ]
        price_history_by_code = {
            "600030.SH": [
                {"trade_date": "2026-05-08", "close": 10.0},
                {"trade_date": "2026-05-09", "close": 10.6},
                {"trade_date": "2026-05-12", "close": 11.0},
                {"trade_date": "2026-05-13", "close": 10.9},
                {"trade_date": "2026-05-14", "close": 10.8},
                {"trade_date": "2026-05-15", "close": 11.1},
                {"trade_date": "2026-05-18", "close": 10.95},
                {"trade_date": "2026-05-19", "close": 11.0},
                {"trade_date": "2026-05-20", "close": 11.4},
                {"trade_date": "2026-05-21", "close": 11.3},
            ],
            DEFAULT_BENCHMARK_TS_CODE: [
                {"trade_date": "2026-05-08", "close": 100.0},
                {"trade_date": "2026-05-09", "close": 101.5},
                {"trade_date": "2026-05-12", "close": 103.0},
                {"trade_date": "2026-05-13", "close": 103.4},
                {"trade_date": "2026-05-14", "close": 103.5},
            ],
        }

        scores = score_cycles(cycles, price_history_by_code)

        self.assertEqual(len(scores), 1)
        self.assertAlmostEqual(scores[0]["total_return"], 0.10)
        self.assertAlmostEqual(scores[0]["benchmark_return"], 0.03)
        self.assertAlmostEqual(scores[0]["excess_return"], 0.07)
        self.assertIs(scores[0]["exit_quality_2d"], True)
        self.assertIs(scores[0]["exit_quality_5d"], True)
        self.assertIs(scores[0]["exit_quality_10d"], False)
        self.assertIs(scores[0]["exit_quality_20d"], False)

    def test_sync_author_activity_scores_cycles_from_database_price_history(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE vw_ts_stock_daily (
                        ts_code TEXT,
                        trade_date TEXT,
                        close REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE vw_ts_stock_basic (
                        ts_code TEXT,
                        name TEXT
                    )
                    """
                )
            )
            conn.execute(
                text("INSERT INTO vw_ts_stock_basic (ts_code, name) VALUES (:ts_code, :name)"),
                {"ts_code": "600030.SH", "name": "中信证券"},
            )
            conn.execute(
                text("INSERT INTO vw_ts_stock_basic (ts_code, name) VALUES (:ts_code, :name)"),
                {"ts_code": DEFAULT_BENCHMARK_TS_CODE, "name": "沪深300"},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO vw_ts_stock_daily (ts_code, trade_date, close)
                    VALUES (:ts_code, :trade_date, :close)
                    """
                ),
                [
                    {"ts_code": "600030.SH", "trade_date": "2026-05-08", "close": 10.0},
                    {"ts_code": "600030.SH", "trade_date": "2026-05-09", "close": 10.6},
                    {"ts_code": "600030.SH", "trade_date": "2026-05-12", "close": 11.2},
                    {"ts_code": "600030.SH", "trade_date": "2026-05-13", "close": 10.9},
                    {"ts_code": DEFAULT_BENCHMARK_TS_CODE, "trade_date": "2026-05-08", "close": 100.0},
                    {"ts_code": DEFAULT_BENCHMARK_TS_CODE, "trade_date": "2026-05-09", "close": 102.0},
                    {"ts_code": DEFAULT_BENCHMARK_TS_CODE, "trade_date": "2026-05-12", "close": 104.0},
                    {"ts_code": DEFAULT_BENCHMARK_TS_CODE, "trade_date": "2026-05-13", "close": 103.5},
                ],
            )

        sample_posts = [
            {
                "post_id": 1001,
                "post_title": "看好 600030",
                "post_content": "先看一波。",
                "post_publish_time": "2026-05-08 14:57:43",
                "post_last_time": "2026-05-08 14:57:43",
                "post_type": 0,
                "post_pic_url": [],
                "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
                "post_user": {"user_id": "4348595203199492"},
                "reply_list": [
                    {
                        "reply_id": 9001,
                        "reply_is_author": True,
                        "reply_text": "今天卖出。",
                        "reply_time": "2026-05-12 10:00:00",
                    }
                ],
            }
        ]

        def fake_fetch_page(page_num: int):
            return sample_posts if page_num == 1 else []

        result = sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            max_pages=2,
        )

        self.assertEqual(result["cycle_count"], 1)
        self.assertEqual(result["summary"]["scored_closed_count"], 1)
        self.assertEqual(result["summary"]["win_count"], 1)
        self.assertAlmostEqual(result["summary"]["win_rate"], 1.0)
        self.assertAlmostEqual(result["summary"]["avg_return"], 0.12)
        self.assertAlmostEqual(result["summary"]["avg_excess_return"], 0.08)
        self.assertAlmostEqual(result["summary"]["avg_hold_days"], 2.0)

    def test_enrich_pending_author_images_updates_ocr_rows_and_rebuilds_mentions(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        sample_posts = [
            {
                "post_id": 1001,
                "post_title": "盘中记录",
                "post_content": "正文没有直接写代码。",
                "post_publish_time": "2026-05-08 14:57:43",
                "post_last_time": "2026-05-08 14:57:43",
                "post_type": 0,
                "post_pic_url": ["https://example.com/ocr-2.png"],
                "post_guba": {},
                "post_user": {"user_id": "4348595203199492"},
                "reply_list": [],
            }
        ]

        def fake_fetch_page(page_num: int):
            return sample_posts if page_num == 1 else []

        sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            max_pages=2,
        )

        class _FakeOcrProvider:
            provider_name = "fake-ocr"

            def extract_image(self, image_url: str, image_index: int = 0):
                return {
                    "image_index": image_index,
                    "image_url": image_url,
                    "ocr_status": "ok",
                    "ocr_text": "继续看好 600030",
                    "ocr_provider": self.provider_name,
                }

        result = enrich_pending_author_images(
            engine,
            "4348595203199492",
            ocr_provider=_FakeOcrProvider(),
        )

        with engine.begin() as conn:
            image_rows = conn.execute(
                text(
                    """
                    SELECT ocr_status, ocr_text, ocr_provider, ocr_updated_at
                    FROM em_author_post_images
                    ORDER BY image_index ASC
                    """
                )
            ).mappings().all()
            mention_count = conn.execute(text("SELECT COUNT(*) FROM em_stock_mentions")).scalar_one()

        self.assertEqual(result["processed_image_count"], 1)
        self.assertEqual(result["pending_image_count"], 0)
        self.assertEqual(result["mention_count"], 1)
        self.assertEqual(result["cycle_count"], 1)
        self.assertEqual(image_rows[0]["ocr_status"], "ok")
        self.assertEqual(image_rows[0]["ocr_text"], "继续看好 600030")
        self.assertEqual(image_rows[0]["ocr_provider"], "fake-ocr")
        self.assertTrue(image_rows[0]["ocr_updated_at"])
        self.assertEqual(mention_count, 1)

    def test_sync_author_activity_stops_on_known_unchanged_posts_and_rebuilds_from_stored_archive(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE vw_ts_stock_basic (
                        ts_code TEXT,
                        name TEXT
                    )
                    """
                )
            )

        older_post = {
            "post_id": 1000,
            "post_title": "看好 000001",
            "post_content": "先放着。",
            "post_publish_time": "2026-05-07 10:00:00",
            "post_last_time": "2026-05-07 10:00:00",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "000001", "stockbar_name": "平安银行吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        latest_post = {
            "post_id": 1001,
            "post_title": "看好 600030",
            "post_content": "继续观察。",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-08 14:57:43",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        replace_author_activity(engine, "4348595203199492", [older_post, latest_post])

        seen_pages = []

        def fake_fetch_page(page_num: int):
            seen_pages.append(page_num)
            return [latest_post] if page_num == 1 else [older_post]

        result = sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            max_pages=3,
            unchanged_post_stop_count=1,
        )

        self.assertEqual(seen_pages, [1])
        self.assertEqual(result["post_count"], 2)
        self.assertEqual(result["mention_count"], 2)
        self.assertEqual(result["cycle_count"], 2)

    def test_sync_author_activity_pages_until_reply_cutoff_is_covered_and_refreshes_recent_replies(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        recent_post = {
            "post_id": 1001,
            "post_title": "看好 600030",
            "post_content": "继续观察。",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-09 09:35:00",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        older_post = {
            "post_id": 1000,
            "post_title": "看好 000001",
            "post_content": "先放着。",
            "post_publish_time": "2026-03-31 10:00:00",
            "post_last_time": "2026-03-31 10:00:00",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "000001", "stockbar_name": "平安银行吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        replace_author_activity(engine, "4348595203199492", [recent_post])

        seen_pages = []
        seen_reply_posts = []

        def fake_fetch_page(page_num: int):
            seen_pages.append(page_num)
            if page_num == 1:
                return [recent_post]
            if page_num == 2:
                return [older_post]
            return []

        def fake_fetch_replies(post: dict):
            seen_reply_posts.append(int(post["post_id"]))
            return [
                {
                    "reply_id": 9001,
                    "reply_is_author": True,
                    "reply_text": "今天先出货。",
                    "reply_time": "2026-05-12 10:00:00",
                }
            ]

        result = sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            fetch_replies_fn=fake_fetch_replies,
            reply_cutoff_date="2026-04-01",
            max_pages=3,
            unchanged_post_stop_count=1,
        )

        archived_posts = load_author_posts(engine, "4348595203199492")
        refreshed_post = next(post for post in archived_posts if int(post["post_id"]) == 1001)

        self.assertEqual(seen_pages, [1, 2])
        self.assertEqual(seen_reply_posts, [1001])
        self.assertEqual(result["mention_count"], 3)
        self.assertEqual([reply["reply_id"] for reply in refreshed_post["reply_list"]], [9001])
        self.assertTrue(refreshed_post["reply_list"][0]["reply_is_author"])

    def test_rebuild_author_tracking_from_archive_applies_direction_override(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        sample_post = {
            "post_id": 1001,
            "post_title": "看好 600030",
            "post_content": "先看一波。",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-08 14:57:43",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [
                {
                    "reply_id": 9001,
                    "reply_is_author": True,
                    "reply_text": "今天卖出。",
                    "reply_time": "2026-05-12 10:00:00",
                }
            ],
        }
        replace_author_activity(engine, "4348595203199492", [sample_post])
        mentions = extract_stock_mentions(sample_post, stock_name_aliases={}, ocr_records=[])
        reply_mention = next(item for item in mentions if item.get("reply_id") == 9001)
        upsert_mention_override(
            engine,
            reply_mention["mention_id"],
            override_direction="bullish",
            override_note="Keep this cycle active",
        )

        result = rebuild_author_tracking_from_archive(engine, "4348595203199492")

        self.assertEqual(result["cycle_count"], 1)
        self.assertEqual(result["summary"]["active_count"], 1)
        self.assertEqual(result["summary"]["closed_count"], 0)

    def test_rebuild_author_tracking_from_archive_can_force_new_cycle_boundary(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        first_post = {
            "post_id": 1001,
            "post_title": "看好 600030",
            "post_content": "先看一波。",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-08 14:57:43",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        second_post = {
            "post_id": 1002,
            "post_title": "继续看好 600030",
            "post_content": "这是新的一段逻辑。",
            "post_publish_time": "2026-05-09 10:00:00",
            "post_last_time": "2026-05-09 10:00:00",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        replace_author_activity(engine, "4348595203199492", [first_post, second_post])
        second_mentions = extract_stock_mentions(second_post, stock_name_aliases={}, ocr_records=[])
        second_open_mention = next(item for item in second_mentions if item.get("post_id") == 1002)
        upsert_mention_override(
            engine,
            second_open_mention["mention_id"],
            force_new_cycle=True,
            override_note="Split into a new idea cycle",
        )

        result = rebuild_author_tracking_from_archive(engine, "4348595203199492")

        self.assertEqual(result["cycle_count"], 2)

    def test_rebuild_author_tracking_from_archive_persists_author_score_snapshot(self):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))
            conn.execute(
                text("INSERT INTO vw_ts_stock_basic (ts_code, name) VALUES (:ts_code, :name)"),
                {"ts_code": "600030.SH", "name": "\u4e2d\u4fe1\u8bc1\u5238"},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO vw_ts_stock_daily (ts_code, trade_date, close)
                    VALUES (:ts_code, :trade_date, :close)
                    """
                ),
                [
                    {"ts_code": "600030.SH", "trade_date": "2026-05-08", "close": 10.0},
                    {"ts_code": "600030.SH", "trade_date": "2026-05-09", "close": 10.5},
                    {"ts_code": "600030.SH", "trade_date": "2026-05-12", "close": 11.0},
                ],
            )

        sample_post = {
            "post_id": 1001,
            "post_title": "\u770b\u597d 600030",
            "post_content": "\u5148\u770b\u4e00\u6ce2\u3002",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-08 14:57:43",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "\u4e2d\u4fe1\u8bc1\u5238\u5427"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [
                {
                    "reply_id": 9001,
                    "reply_is_author": True,
                    "reply_text": "\u4eca\u5929\u5148\u51fa\u8d27\u3002",
                    "reply_time": "2026-05-12 10:00:00",
                }
            ],
        }
        replace_author_activity(engine, "4348595203199492", [sample_post])

        result = rebuild_author_tracking_from_archive(
            engine,
            "4348595203199492",
            snapshot_date="2026-05-13",
        )
        rows = list_author_score_snapshots(engine, "4348595203199492")

        self.assertEqual(result["summary"]["closed_count"], 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["snapshot_date"], "2026-05-13")
        self.assertEqual(rows[0]["cycle_count"], 1)
        self.assertEqual(rows[0]["closed_count"], 1)
        self.assertAlmostEqual(rows[0]["win_rate"], 1.0)

    @patch("src.eastmoney_author_tracker.service.fetch_post_author_replies")
    def test_sync_author_activity_backfills_author_replies_since_cutoff_and_pages_until_cutoff(
        self,
        mock_fetch_post_author_replies,
    ):
        engine = _build_sqlite_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE vw_ts_stock_basic (ts_code TEXT, name TEXT)"))
            conn.execute(text("CREATE TABLE vw_ts_stock_daily (ts_code TEXT, trade_date TEXT, close REAL)"))

        may_post = {
            "post_id": 1001,
            "post_title": "看好 600030",
            "post_content": "5月继续观察。",
            "post_publish_time": "2026-05-08 14:57:43",
            "post_last_time": "2026-05-08 14:57:43",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600030", "stockbar_name": "中信证券吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        april_post = {
            "post_id": 1002,
            "post_title": "继续看好 000001",
            "post_content": "4月还在跟。",
            "post_publish_time": "2026-04-03 09:00:00",
            "post_last_time": "2026-04-03 09:00:00",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "000001", "stockbar_name": "平安银行吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }
        march_post = {
            "post_id": 1003,
            "post_title": "3月旧帖 600010",
            "post_content": "这个不用补回复。",
            "post_publish_time": "2026-03-29 10:00:00",
            "post_last_time": "2026-03-29 10:00:00",
            "post_type": 0,
            "post_pic_url": [],
            "post_guba": {"stockbar_code": "600010", "stockbar_name": "包钢股份吧"},
            "post_user": {"user_id": "4348595203199492"},
            "reply_list": [],
        }

        seen_pages: list[int] = []

        def fake_fetch_page(page_num: int):
            seen_pages.append(page_num)
            if page_num == 1:
                return [may_post]
            if page_num == 2:
                return [april_post, march_post]
            return []

        mock_fetch_post_author_replies.side_effect = lambda post_id, stockbar_code, **kwargs: [
            {
                "reply_id": 9000 + int(post_id),
                "reply_is_author": True,
                "user_id": "4348595203199492",
                "reply_text": f"作者回复 {post_id}",
                "reply_time": "2026-05-12 10:00:00" if int(post_id) == 1001 else "2026-04-04 09:30:00",
                "source_post_code": stockbar_code,
                "source_post_id": post_id,
            }
        ]

        result = sync_author_activity(
            engine,
            "4348595203199492",
            fetch_page_fn=fake_fetch_page,
            max_pages=5,
            reply_cutoff_date="2026-04-01",
        )

        self.assertEqual(seen_pages, [1, 2])
        self.assertEqual(result["post_count"], 3)
        self.assertGreaterEqual(result["mention_count"], 5)
        saved_posts = {int(post["post_id"]): post for post in load_author_posts(engine, "4348595203199492")}
        self.assertEqual(len(saved_posts[1001]["reply_list"]), 1)
        self.assertEqual(len(saved_posts[1002]["reply_list"]), 1)
        self.assertEqual(saved_posts[1003]["reply_list"], [])
        self.assertEqual(mock_fetch_post_author_replies.call_count, 2)


if __name__ == "__main__":
    unittest.main()
