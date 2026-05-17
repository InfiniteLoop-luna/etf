import math
import unittest

import numpy as np
import pandas as pd

from src.alpha_191 import add_alpha095_family
from src.ml_stock_dataset import (
    build_forward_label_frame,
    build_feature_frame,
    build_sample_frame,
    build_universe_rows,
    compute_feature_quality_flag,
    compute_listing_days,
)
from src.ml_stock_train_v1 import (
    compute_daily_topn_strategy_metrics,
    evaluate_classification_predictions,
    evaluate_regression_predictions,
    fit_classification_model,
    fit_regression_model,
    generate_walk_forward_cutoff_dates,
    prepare_training_data,
    resolve_target_column,
    run_walk_forward_evaluation,
    select_v1_feature_columns,
    split_by_date,
    train_classification_baseline,
    train_regression_baseline,
)


class MlStockUniverseTests(unittest.TestCase):
    def test_compute_listing_days_counts_calendar_days_inclusively(self):
        self.assertEqual(
            compute_listing_days(pd.Timestamp("2026-05-11"), pd.Timestamp("2026-05-11")),
            1,
        )
        self.assertEqual(
            compute_listing_days(pd.Timestamp("2026-05-11"), pd.Timestamp("2026-05-01")),
            11,
        )
        self.assertIsNone(
            compute_listing_days(pd.Timestamp("2026-05-01"), pd.Timestamp("2026-05-11"))
        )

    def test_build_universe_rows_sets_conservative_flags(self):
        source_df = pd.DataFrame(
            [
                {
                    "trade_date": "2026-05-11",
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "industry": "银行",
                    "market": "主板",
                    "exchange": "SZSE",
                    "list_date": "1991-04-03",
                    "list_status": "L",
                    "close": 10.5,
                    "daily_basic_close": 10.5,
                    "price_history_bars": 60,
                    "first_financial_ann_date": "2026-04-20",
                    "has_ever_st": True,
                },
                {
                    "trade_date": "2026-05-11",
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "*ST测试",
                    "industry": "地产",
                    "market": "主板",
                    "exchange": "SZSE",
                    "list_date": "1991-01-29",
                    "list_status": "D",
                    "close": 8.2,
                    "daily_basic_close": 8.2,
                    "price_history_bars": 200,
                    "first_financial_ann_date": "2026-04-20",
                    "has_ever_st": True,
                },
                {
                    "trade_date": "2026-05-11",
                    "ts_code": "301001.SZ",
                    "symbol": "301001",
                    "name": "新股样本",
                    "industry": "电子",
                    "market": "创业板",
                    "exchange": "SZSE",
                    "list_date": "2026-04-20",
                    "list_status": "L",
                    "close": 21.3,
                    "daily_basic_close": None,
                    "price_history_bars": 12,
                    "first_financial_ann_date": None,
                    "has_ever_st": False,
                },
            ]
        )

        rows = build_universe_rows(source_df, min_history_days=60)
        by_code = {row["ts_code"]: row for row in rows}

        eligible = by_code["000001.SZ"]
        self.assertGreaterEqual(eligible["listing_days"], 1)
        self.assertTrue(eligible["has_price"])
        self.assertTrue(eligible["has_daily_basic"])
        self.assertTrue(eligible["has_financial"])
        self.assertTrue(eligible["min_history_ok"])
        self.assertTrue(eligible["sample_eligible"])
        self.assertFalse(eligible["is_current_st"])
        self.assertTrue(eligible["has_ever_st"])

        delisted = by_code["000002.SZ"]
        self.assertFalse(delisted["sample_eligible"])
        self.assertTrue(delisted["is_current_st"])

        new_listing = by_code["301001.SZ"]
        self.assertFalse(new_listing["has_daily_basic"])
        self.assertFalse(new_listing["has_financial"])
        self.assertFalse(new_listing["min_history_ok"])
        self.assertFalse(new_listing["sample_eligible"])


class MlStockLabelTests(unittest.TestCase):
    def test_build_forward_label_frame_computes_returns_drawdown_and_upside(self):
        price_df = pd.DataFrame(
            [
                {"trade_date": "2026-01-02", "ts_code": "000001.SZ", "close": 10.0, "high": 10.5, "low": 9.8},
                {"trade_date": "2026-01-05", "ts_code": "000001.SZ", "close": 11.0, "high": 11.5, "low": 10.8},
                {"trade_date": "2026-01-06", "ts_code": "000001.SZ", "close": 9.0, "high": 9.5, "low": 8.0},
                {"trade_date": "2026-01-07", "ts_code": "000001.SZ", "close": 12.0, "high": 12.5, "low": 11.0},
                {"trade_date": "2026-01-08", "ts_code": "000001.SZ", "close": 12.0, "high": 13.0, "low": 11.5},
                {"trade_date": "2026-01-09", "ts_code": "000001.SZ", "close": 15.0, "high": 15.5, "low": 14.0},
            ]
        )

        labels = build_forward_label_frame(price_df)
        first = labels.loc[
            (labels["ts_code"] == "000001.SZ") & (labels["trade_date"] == pd.Timestamp("2026-01-02"))
        ].iloc[0]

        self.assertAlmostEqual(first["entry_price"], 10.0)
        self.assertEqual(first["entry_basis"], "close")
        self.assertAlmostEqual(first["ret_fwd_1d"], 0.10)
        self.assertAlmostEqual(first["ret_fwd_3d"], 0.20)
        self.assertAlmostEqual(first["ret_fwd_5d"], 0.50)
        self.assertTrue(first["y_up_1d"])
        self.assertTrue(first["y_up_3d"])
        self.assertTrue(first["y_up_5d"])
        self.assertAlmostEqual(first["max_dd_fwd_3d"], -0.20)
        self.assertAlmostEqual(first["max_dd_fwd_5d"], -0.20)
        self.assertAlmostEqual(first["max_upside_fwd_5d"], 0.55)
        self.assertTrue(first["future_price_available_5d"])
        self.assertFalse(first["future_price_available_20d"])
        self.assertFalse(first["suspended_in_horizon_flag"])
        self.assertTrue(math.isnan(first["ret_fwd_10d"]))
        self.assertTrue(math.isnan(first["ret_fwd_20d"]))

    def test_build_forward_label_frame_flags_suspension_when_market_dates_are_missing(self):
        price_df = pd.DataFrame(
            [
                {"trade_date": "2026-01-02", "ts_code": "000001.SZ", "close": 10.0, "high": 10.2, "low": 9.8},
                {"trade_date": "2026-01-05", "ts_code": "000001.SZ", "close": 10.5, "high": 10.7, "low": 10.1},
                {"trade_date": "2026-01-07", "ts_code": "000001.SZ", "close": 10.8, "high": 11.0, "low": 10.6},
                {"trade_date": "2026-01-08", "ts_code": "000001.SZ", "close": 11.0, "high": 11.2, "low": 10.7},
            ]
        )
        market_trade_dates = pd.to_datetime(
            ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]
        )

        labels = build_forward_label_frame(price_df, market_trade_dates=market_trade_dates)
        first = labels.loc[
            (labels["ts_code"] == "000001.SZ") & (labels["trade_date"] == pd.Timestamp("2026-01-02"))
        ].iloc[0]

        self.assertTrue(first["suspended_in_horizon_flag"])


class MlStockFeatureTests(unittest.TestCase):
    def _build_feature_source_df(self) -> pd.DataFrame:
        rows = []
        trade_dates = pd.date_range("2026-01-02", periods=25, freq="B")
        for idx, trade_date in enumerate(trade_dates):
            close_value = 10.0 + idx
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": "000001.SZ",
                    "open": close_value - 0.5,
                    "high": close_value + 1.0,
                    "low": close_value - 1.0,
                    "close": close_value,
                    "vol": 100.0 + idx,
                    "amount": 1000.0 + idx * 10.0,
                    "turnover_rate": 1.0 + idx * 0.01,
                    "turnover_rate_f": 1.2 + idx * 0.01,
                    "volume_ratio": 0.8 + idx * 0.02,
                    "pe": 10.0 + idx,
                    "pe_ttm": 12.0 + idx,
                    "pb": 1.0 + idx * 0.05,
                    "ps": 2.0 + idx * 0.03,
                    "ps_ttm": 2.2 + idx * 0.03,
                    "dv_ratio": 0.5 + idx * 0.01,
                    "dv_ttm": 0.6 + idx * 0.01,
                    "total_mv": 1_000_000.0 + idx * 1_000.0,
                    "circ_mv": 800_000.0 + idx * 800.0,
                    "w_ema5": 10.0 + idx * 0.4,
                    "w_ema30": 9.0 + idx * 0.35,
                    "m_ema5": 11.0 + idx * 0.3,
                    "m_ema30": 10.0 + idx * 0.25,
                    "is_weekly_ema_bearish": False,
                    "is_monthly_ema_bearish": False,
                }
            )
        return pd.DataFrame(rows)

    def test_build_feature_frame_computes_returns_and_ma_activity_ratios(self):
        feature_df = build_feature_frame(self._build_feature_source_df())
        last = feature_df.iloc[-1]

        self.assertAlmostEqual(last["ret_1d"], 34.0 / 33.0 - 1.0)
        self.assertAlmostEqual(last["ret_3d"], 34.0 / 31.0 - 1.0)
        self.assertAlmostEqual(last["ret_5d"], 34.0 / 29.0 - 1.0)
        self.assertAlmostEqual(last["ret_10d"], 34.0 / 24.0 - 1.0)
        self.assertAlmostEqual(last["ret_20d"], 34.0 / 14.0 - 1.0)
        self.assertTrue(math.isnan(last["ret_60d"]))
        self.assertAlmostEqual(last["close_over_ma5"], 34.0 / 32.0)
        self.assertAlmostEqual(last["close_over_ma20"], 34.0 / 24.5)
        self.assertTrue(math.isnan(last["close_over_ma60"]))
        self.assertAlmostEqual(last["ma5_over_ma20"], 32.0 / 24.5)
        self.assertTrue(math.isnan(last["ma20_over_ma60"]))
        self.assertAlmostEqual(last["vol_ma5_ratio"], 124.0 / 122.0)
        self.assertAlmostEqual(last["amount_ma5_ratio"], 1240.0 / 1220.0)
        self.assertAlmostEqual(last["w_ema5_over_30"], (10.0 + 24 * 0.4) / (9.0 + 24 * 0.35))
        self.assertAlmostEqual(last["m_ema5_over_30"], (11.0 + 24 * 0.3) / (10.0 + 24 * 0.25))
        self.assertTrue(last["has_price_feature"])
        self.assertTrue(last["has_daily_basic_feature"])
        self.assertTrue(last["has_technical_signal_feature"])
        self.assertAlmostEqual(last["feature_complete_ratio"], 1.0)
        self.assertEqual(last["quality_flag"], "complete")

    def test_build_feature_frame_computes_volatility_and_distance_features(self):
        feature_df = build_feature_frame(self._build_feature_source_df())
        last = feature_df.iloc[-1]

        close_series = pd.Series([10.0 + idx for idx in range(25)], dtype=float)
        daily_ret = close_series.pct_change()
        expected_vol_5d = daily_ret.rolling(5, min_periods=5).std(ddof=0).iloc[-1]
        expected_vol_20d = daily_ret.rolling(20, min_periods=20).std(ddof=0).iloc[-1]

        self.assertAlmostEqual(last["volatility_5d"], expected_vol_5d)
        self.assertAlmostEqual(last["volatility_20d"], expected_vol_20d)
        self.assertAlmostEqual(last["distance_to_20d_high"], 34.0 / 35.0 - 1.0)
        self.assertAlmostEqual(last["distance_to_20d_low"], 34.0 / 14.0 - 1.0)
        self.assertTrue(math.isnan(last["distance_to_60d_high"]))
        self.assertTrue(math.isnan(last["distance_to_60d_low"]))

    def test_feature_quality_flag_is_conservative_when_sources_are_missing(self):
        self.assertEqual(
            compute_feature_quality_flag(
                has_price_feature=True,
                has_daily_basic_feature=True,
                has_technical_signal_feature=True,
            ),
            "complete",
        )
        self.assertEqual(
            compute_feature_quality_flag(
                has_price_feature=True,
                has_daily_basic_feature=False,
                has_technical_signal_feature=False,
            ),
            "price_only",
        )
        self.assertEqual(
            compute_feature_quality_flag(
                has_price_feature=False,
                has_daily_basic_feature=True,
                has_technical_signal_feature=True,
            ),
            "insufficient",
        )


class Alpha191FactorTests(unittest.TestCase):
    def _build_source_df(self) -> pd.DataFrame:
        rows = []
        trade_dates = pd.date_range("2026-01-02", periods=25, freq="B")

        for idx, trade_date in enumerate(trade_dates):
            rows.append(
                {
                    "date": trade_date,
                    "code": "000001.SZ",
                    "amount": 1_000.0 + idx * 10.0,
                }
            )
            rows.append(
                {
                    "date": trade_date,
                    "code": "000002.SZ",
                    "amount": 100.0 + idx,
                }
            )

        source_df = pd.DataFrame(rows).sample(frac=1.0, random_state=7)
        source_df.index = pd.Index(np.arange(1000, 1000 + len(source_df)) * 3)
        return source_df

    def test_add_alpha095_family_keeps_input_row_order(self):
        source_df = self._build_source_df()

        result = add_alpha095_family(source_df, window=20, ddof=1)

        self.assertEqual(result.index.tolist(), source_df.index.tolist())
        self.assertEqual(
            result[["date", "code", "amount"]].to_dict("records"),
            source_df[["date", "code", "amount"]].to_dict("records"),
        )

    def test_add_alpha095_family_matches_manual_calculation_by_code(self):
        source_df = self._build_source_df()

        result = add_alpha095_family(source_df, window=20, ddof=1)
        ordered = result.sort_values(["code", "date"], kind="mergesort")

        stock_a = ordered.loc[ordered["code"] == "000001.SZ"].reset_index(drop=True)
        amount_a = stock_a["amount"].astype(float)
        expected_alpha095 = amount_a.rolling(20, min_periods=20).std(ddof=1)
        expected_cv = expected_alpha095 / amount_a.rolling(20, min_periods=20).mean()
        expected_logstd = np.log1p(amount_a).rolling(20, min_periods=20).std(ddof=1)
        amount_ret = amount_a.pct_change(fill_method=None)
        expected_pctstd = amount_ret.rolling(20, min_periods=20).std(ddof=1)

        last = stock_a.iloc[-1]
        last_idx = len(stock_a) - 1

        self.assertAlmostEqual(last["alpha095"], expected_alpha095.iloc[last_idx])
        self.assertAlmostEqual(last["alpha095_cv"], expected_cv.iloc[last_idx])
        self.assertAlmostEqual(last["alpha095_logstd"], expected_logstd.iloc[last_idx])
        self.assertAlmostEqual(last["alpha095_pctstd"], expected_pctstd.iloc[last_idx])

    def test_add_alpha095_family_isolates_stocks_and_respects_min_periods(self):
        source_df = self._build_source_df()

        result = add_alpha095_family(source_df, window=20, ddof=1).sort_values(
            ["code", "date"],
            kind="mergesort",
        )

        stock_a = result.loc[result["code"] == "000001.SZ"].reset_index(drop=True)
        stock_b = result.loc[result["code"] == "000002.SZ"].reset_index(drop=True)

        self.assertTrue(stock_a.loc[:18, "alpha095"].isna().all())
        self.assertTrue(stock_b.loc[:18, "alpha095"].isna().all())
        self.assertFalse(math.isclose(stock_a.iloc[-1]["alpha095"], stock_b.iloc[-1]["alpha095"]))
        self.assertTrue(stock_a["alpha095"].notna().sum() == 6)
        self.assertTrue(stock_b["alpha095"].notna().sum() == 6)

    def test_add_alpha095_family_raises_when_required_columns_are_missing(self):
        with self.assertRaises(ValueError):
            add_alpha095_family(pd.DataFrame({"date": [], "code": []}))


class MlStockSampleTests(unittest.TestCase):
    def _build_universe_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "Alpha",
                    "sample_eligible": True,
                    "created_at": pd.Timestamp("2026-05-11 09:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 09:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000002.SZ",
                    "symbol": "000002",
                    "name": "Beta",
                    "sample_eligible": False,
                    "created_at": pd.Timestamp("2026-05-11 09:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 09:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000003.SZ",
                    "symbol": "000003",
                    "name": "Gamma",
                    "sample_eligible": True,
                    "created_at": pd.Timestamp("2026-05-11 09:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 09:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000004.SZ",
                    "symbol": "000004",
                    "name": "Delta",
                    "sample_eligible": True,
                    "created_at": pd.Timestamp("2026-05-11 09:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 09:00:00"),
                },
            ]
        )

    def _build_feature_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000001.SZ",
                    "close": 10.5,
                    "quality_flag": "complete",
                    "created_at": pd.Timestamp("2026-05-11 10:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 10:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000002.SZ",
                    "close": 8.8,
                    "quality_flag": "partial",
                    "created_at": pd.Timestamp("2026-05-11 10:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 10:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000004.SZ",
                    "close": 12.3,
                    "quality_flag": "complete",
                    "created_at": pd.Timestamp("2026-05-11 10:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 10:00:00"),
                },
            ]
        )

    def _build_label_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000001.SZ",
                    "y_up_5d": True,
                    "ret_fwd_5d": 0.08,
                    "created_at": pd.Timestamp("2026-05-11 11:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 11:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000002.SZ",
                    "y_up_5d": False,
                    "ret_fwd_5d": -0.03,
                    "created_at": pd.Timestamp("2026-05-11 11:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 11:00:00"),
                },
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000003.SZ",
                    "y_up_5d": True,
                    "ret_fwd_5d": 0.12,
                    "created_at": pd.Timestamp("2026-05-11 11:00:00"),
                    "updated_at": pd.Timestamp("2026-05-11 11:00:00"),
                },
            ]
        )

    def test_build_sample_frame_keeps_only_eligible_complete_rows_by_default(self):
        sample_df = build_sample_frame(
            self._build_universe_df(),
            self._build_feature_df(),
            self._build_label_df(),
        )

        self.assertEqual(sample_df["ts_code"].tolist(), ["000001.SZ"])
        self.assertEqual(sample_df["trade_date"].tolist(), [pd.Timestamp("2026-01-02")])
        self.assertIn("symbol", sample_df.columns)
        self.assertIn("close", sample_df.columns)
        self.assertIn("y_up_5d", sample_df.columns)
        self.assertIn("created_at", sample_df.columns)
        self.assertIn("feature_created_at", sample_df.columns)
        self.assertIn("label_created_at", sample_df.columns)
        self.assertNotIn("created_at_x", sample_df.columns)
        self.assertNotIn("created_at_y", sample_df.columns)

    def test_build_sample_frame_can_include_ineligible_rows_when_requested(self):
        sample_df = build_sample_frame(
            self._build_universe_df(),
            self._build_feature_df(),
            self._build_label_df(),
            only_eligible=False,
        )

        self.assertEqual(sample_df["ts_code"].tolist(), ["000001.SZ", "000002.SZ"])
        self.assertEqual(sample_df["sample_eligible"].tolist(), [True, False])
        self.assertNotIn("000003.SZ", sample_df["ts_code"].tolist())
        self.assertNotIn("000004.SZ", sample_df["ts_code"].tolist())


class MlStockTrainingScaffoldTests(unittest.TestCase):
    def _build_training_sample_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": "2026-01-02",
                    "ts_code": "000001.SZ",
                    "sample_eligible": True,
                    "listing_days": 300,
                    "is_current_st": False,
                    "has_ever_st": False,
                    "close": 10.0,
                    "ret_5d": 0.05,
                    "ret_20d": 0.10,
                    "ma5_over_ma20": 1.05,
                    "volatility_20d": 0.02,
                    "turnover_rate": 1.2,
                    "pb": 1.5,
                    "w_ema5_over_30": 1.03,
                    "feature_complete_ratio": 1.0,
                    "quality_flag": "complete",
                    "y_up_5d": True,
                    "ret_fwd_5d": 0.08,
                },
                {
                    "trade_date": "2026-01-03",
                    "ts_code": "000002.SZ",
                    "sample_eligible": True,
                    "listing_days": 500,
                    "is_current_st": False,
                    "has_ever_st": True,
                    "close": 12.0,
                    "ret_5d": 0.02,
                    "ret_20d": 0.06,
                    "ma5_over_ma20": 1.02,
                    "volatility_20d": None,
                    "turnover_rate": 1.5,
                    "pb": 2.0,
                    "w_ema5_over_30": 1.01,
                    "feature_complete_ratio": 0.95,
                    "quality_flag": "complete",
                    "y_up_5d": False,
                    "ret_fwd_5d": -0.03,
                },
                {
                    "trade_date": "2026-01-04",
                    "ts_code": "000003.SZ",
                    "sample_eligible": True,
                    "listing_days": 120,
                    "is_current_st": True,
                    "has_ever_st": True,
                    "close": 8.0,
                    "ret_5d": -0.04,
                    "ret_20d": -0.08,
                    "ma5_over_ma20": 0.95,
                    "volatility_20d": 0.03,
                    "turnover_rate": 0.8,
                    "pb": 1.1,
                    "w_ema5_over_30": 0.98,
                    "feature_complete_ratio": 0.70,
                    "quality_flag": "partial",
                    "y_up_5d": True,
                    "ret_fwd_5d": None,
                },
            ]
        )

    def test_resolve_target_column_uses_task_defaults(self):
        self.assertEqual(resolve_target_column("classification", None), "y_up_5d")
        self.assertEqual(resolve_target_column("regression", None), "ret_fwd_5d")

    def test_select_v1_feature_columns_returns_present_curated_columns(self):
        columns = select_v1_feature_columns(self._build_training_sample_df())

        self.assertIn("listing_days", columns)
        self.assertIn("ret_20d", columns)
        self.assertIn("feature_complete_ratio", columns)
        self.assertNotIn("quality_flag", columns)
        self.assertNotIn("y_up_5d", columns)

    def test_prepare_training_data_filters_missing_target_and_fills_numeric_features(self):
        prepared = prepare_training_data(
            self._build_training_sample_df(),
            task_type="regression",
            fill_method="median",
        )

        self.assertEqual(prepared.target_column, "ret_fwd_5d")
        self.assertEqual(prepared.row_count_before_filter, 3)
        self.assertEqual(prepared.row_count_after_filter, 2)
        self.assertEqual(prepared.features.shape, (2, len(prepared.feature_columns)))
        self.assertAlmostEqual(
            prepared.features.loc[prepared.features.index[1], "volatility_20d"],
            0.02,
        )

    def test_split_by_date_respects_chronological_cutoff(self):
        prepared = prepare_training_data(
            self._build_training_sample_df(),
            task_type="classification",
            fill_method="median",
        )

        train_split, test_split = split_by_date(prepared, cutoff_date="2026-01-03")

        self.assertEqual(train_split.rows["ts_code"].tolist(), ["000001.SZ"])
        self.assertEqual(test_split.rows["ts_code"].tolist(), ["000002.SZ", "000003.SZ"])

    def test_classification_baseline_metrics_are_computed_on_tiny_sample(self):
        prepared = prepare_training_data(
            self._build_training_sample_df(),
            task_type="classification",
            fill_method="median",
        )
        train_split, test_split = split_by_date(prepared, cutoff_date="2026-01-04")

        model = train_classification_baseline(train_split.features, train_split.target)
        metrics = evaluate_classification_predictions(
            test_split.target,
            model.predict(test_split.features),
        )

        self.assertEqual(model.strategy, "single_feature_threshold")
        self.assertEqual(metrics["sample_count"], 1)
        self.assertAlmostEqual(metrics["accuracy"], 0.0)
        self.assertAlmostEqual(metrics["positive_rate_pred"], 0.0)
        self.assertAlmostEqual(metrics["positive_rate_actual"], 1.0)

    def test_regression_baseline_metrics_use_training_mean_prediction(self):
        prepared = prepare_training_data(
            self._build_training_sample_df(),
            task_type="regression",
            fill_method="median",
        )
        train_split, test_split = split_by_date(prepared, cutoff_date="2026-01-03")

        model = train_regression_baseline(train_split.target)
        metrics = evaluate_regression_predictions(
            test_split.target,
            model.predict(test_split.features),
        )

        self.assertEqual(model.strategy, "mean_target")
        self.assertEqual(metrics["sample_count"], 1)
        self.assertAlmostEqual(metrics["mae"], 0.11)
        self.assertAlmostEqual(metrics["rmse"], 0.11)
        self.assertAlmostEqual(metrics["target_mean"], -0.03)

    def test_fit_classification_model_supports_sklearn_logistic(self):
        train_features = pd.DataFrame(
            {
                "signal": [-2.0, -1.0, -0.5, 0.5, 1.0, 2.0],
                "quality": [1.0, 0.9, 0.8, 0.8, 0.9, 1.0],
            }
        )
        train_target = pd.Series([0, 0, 0, 1, 1, 1], dtype=int)
        test_features = pd.DataFrame(
            {
                "signal": [-1.5, 1.5],
                "quality": [0.95, 0.95],
            }
        )
        test_target = pd.Series([0, 1], dtype=int)

        run = fit_classification_model(
            train_features,
            train_target,
            test_features,
            model_kind="sklearn",
            classifier="logistic",
        )
        metrics = evaluate_classification_predictions(
            test_target,
            run.test_predictions,
            y_score=run.test_scores,
        )

        self.assertEqual(run.model_summary["model_kind"], "sklearn")
        self.assertEqual(run.model_summary["task_type"], "classification")
        self.assertEqual(run.model_summary["classifier"], "logistic")
        self.assertEqual(run.model_summary["estimator_name"], "LogisticRegression")
        self.assertEqual(run.train_predictions.tolist(), [0, 0, 0, 1, 1, 1])
        self.assertEqual(run.test_predictions.tolist(), [0, 1])
        self.assertEqual(run.test_scores.index.tolist(), test_features.index.tolist())
        self.assertEqual(metrics["sample_count"], 2)
        self.assertAlmostEqual(metrics["accuracy"], 1.0)
        self.assertAlmostEqual(metrics["positive_rate_pred"], 0.5)
        self.assertAlmostEqual(metrics["positive_rate_actual"], 0.5)
        self.assertAlmostEqual(metrics["average_label"], 0.5)
        self.assertAlmostEqual(metrics["roc_auc"], 1.0)

    def test_fit_regression_model_supports_sklearn_linear(self):
        train_features = pd.DataFrame({"signal": [0.0, 1.0, 2.0, 3.0]})
        train_target = pd.Series([1.0, 3.0, 5.0, 7.0], dtype=float)
        test_features = pd.DataFrame({"signal": [4.0, 5.0]})
        test_target = pd.Series([9.0, 11.0], dtype=float)

        run = fit_regression_model(
            train_features,
            train_target,
            test_features,
            model_kind="sklearn",
            regressor="linear",
        )
        metrics = evaluate_regression_predictions(test_target, run.test_predictions)

        self.assertEqual(run.model_summary["model_kind"], "sklearn")
        self.assertEqual(run.model_summary["task_type"], "regression")
        self.assertEqual(run.model_summary["regressor"], "linear")
        self.assertEqual(run.model_summary["estimator_name"], "LinearRegression")
        self.assertEqual(run.train_predictions.tolist(), [1.0, 3.0, 5.0, 7.0])
        self.assertEqual(run.test_predictions.tolist(), [9.0, 11.0])
        self.assertEqual(metrics["sample_count"], 2)
        self.assertAlmostEqual(metrics["mae"], 0.0)
        self.assertAlmostEqual(metrics["rmse"], 0.0)
        self.assertAlmostEqual(metrics["target_mean"], 10.0)
        self.assertAlmostEqual(metrics["r2"], 1.0)

    def test_fit_regression_model_supports_sklearn_ridge_summary(self):
        train_features = pd.DataFrame({"signal": [0.0, 1.0, 2.0, 3.0]})
        train_target = pd.Series([0.0, 1.0, 2.0, 3.0], dtype=float)
        test_features = pd.DataFrame({"signal": [4.0, 5.0]})

        run = fit_regression_model(
            train_features,
            train_target,
            test_features,
            model_kind="sklearn",
            regressor="ridge",
        )

        self.assertEqual(run.model_summary["model_kind"], "sklearn")
        self.assertEqual(run.model_summary["task_type"], "regression")
        self.assertEqual(run.model_summary["regressor"], "ridge")
        self.assertEqual(run.model_summary["estimator_name"], "Ridge")
        self.assertIn("alpha", run.model_summary["params"])
        self.assertEqual(len(run.test_predictions), 2)

    def test_compute_daily_topn_strategy_metrics_groups_by_day_and_rank(self):
        rows = pd.DataFrame(
            [
                {"trade_date": "2026-01-02", "ts_code": "000001.SZ", "ret_fwd_5d": 0.10, "y_up_5d": True},
                {"trade_date": "2026-01-02", "ts_code": "000002.SZ", "ret_fwd_5d": -0.05, "y_up_5d": False},
                {"trade_date": "2026-01-02", "ts_code": "000003.SZ", "ret_fwd_5d": -0.20, "y_up_5d": False},
                {"trade_date": "2026-01-03", "ts_code": "000004.SZ", "ret_fwd_5d": 0.30, "y_up_5d": True},
                {"trade_date": "2026-01-03", "ts_code": "000005.SZ", "ret_fwd_5d": 0.00, "y_up_5d": False},
                {"trade_date": "2026-01-03", "ts_code": "000006.SZ", "ret_fwd_5d": -0.10, "y_up_5d": False},
            ]
        )
        ranking_score = pd.Series([0.90, 0.20, 0.80, 0.95, 0.70, 0.10], dtype=float)

        metrics = compute_daily_topn_strategy_metrics(
            rows,
            ranking_score,
            return_column="ret_fwd_5d",
            label_column="y_up_5d",
            topn_levels=(1, 2),
        )

        self.assertEqual(metrics["row_count"], 6)
        self.assertEqual(metrics["day_count"], 2)
        self.assertEqual(metrics["score_source"], "prediction")
        self.assertEqual(metrics["return_column"], "ret_fwd_5d")
        self.assertEqual(metrics["label_column"], "y_up_5d")
        self.assertEqual(metrics["top1"]["days_ranked"], 2)
        self.assertEqual(metrics["top1"]["pick_count_total"], 2)
        self.assertAlmostEqual(metrics["top1"]["avg_return"], 0.20)
        self.assertAlmostEqual(metrics["top1"]["hit_rate"], 1.0)
        self.assertEqual(metrics["top2"]["days_ranked"], 2)
        self.assertEqual(metrics["top2"]["pick_count_total"], 4)
        self.assertAlmostEqual(metrics["top2"]["avg_return"], 0.05)
        self.assertAlmostEqual(metrics["top2"]["hit_rate"], 0.5)

    def _build_walk_forward_sample_df(self) -> pd.DataFrame:
        rows = []
        for idx, trade_date in enumerate(pd.date_range("2026-01-02", periods=6, freq="D")):
            signal = float(idx)
            rows.append(
                {
                    "trade_date": trade_date,
                    "ts_code": f"{idx + 1:06d}.SZ",
                    "sample_eligible": True,
                    "listing_days": 100 + idx,
                    "is_current_st": False,
                    "has_ever_st": bool(idx % 3 == 0),
                    "close": 10.0 + signal * 2.0,
                    "ret_5d": signal - 2.0,
                    "ret_20d": signal - 1.5,
                    "ma5_over_ma20": 0.95 + signal * 0.05,
                    "w_ema5_over_30": 0.96 + signal * 0.04,
                    "feature_complete_ratio": 1.0,
                    "y_up_5d": bool(idx % 2),
                    "ret_fwd_5d": 1.0 + signal * 2.0,
                }
            )
        return pd.DataFrame(rows)

    def test_generate_walk_forward_cutoff_dates_is_chronological_and_respects_limits(self):
        prepared = prepare_training_data(
            self._build_walk_forward_sample_df(),
            task_type="classification",
            fill_method="median",
        )

        cutoff_dates = generate_walk_forward_cutoff_dates(
            prepared,
            min_train_rows=2,
            min_test_rows=2,
            max_windows=2,
        )

        self.assertEqual(
            [cutoff.strftime("%Y-%m-%d") for cutoff in cutoff_dates],
            ["2026-01-05", "2026-01-06"],
        )

    def test_run_walk_forward_evaluation_supports_classification_baseline(self):
        prepared = prepare_training_data(
            self._build_walk_forward_sample_df(),
            task_type="classification",
            fill_method="median",
        )

        result = run_walk_forward_evaluation(
            prepared,
            model_kind="baseline",
            fill_method="median",
            min_train_rows=2,
            min_test_rows=2,
            max_windows=2,
        )

        summary = result.to_summary()
        self.assertEqual(summary["aggregate"]["window_count"], 2)
        self.assertEqual(summary["aggregate"]["selected_cutoff_count"], 2)
        self.assertEqual(summary["aggregate"]["candidate_cutoff_count"], 3)
        self.assertEqual(summary["aggregate"]["cutoff_dates"], ["2026-01-05", "2026-01-06"])
        self.assertEqual(summary["aggregate"]["rows_evaluated_total"], 5)
        self.assertIn("average_test_accuracy", summary["aggregate"])
        self.assertIn("aggregate_test_metrics", summary["aggregate"])
        self.assertIn("strategy_metrics", summary["aggregate"])
        self.assertEqual(summary["aggregate"]["strategy_metrics"]["window_count_with_strategy_metrics"], 2)
        self.assertEqual(summary["aggregate"]["strategy_metrics"]["topn_levels"], [1, 3, 5])
        self.assertEqual(len(summary["window_results"]), 2)
        self.assertEqual(summary["window_results"][0]["model_summary"]["model_kind"], "baseline")
        self.assertEqual(summary["window_results"][0]["train_rows"], 3)
        self.assertEqual(summary["window_results"][0]["test_rows"], 3)
        self.assertIn("strategy_metrics", summary["window_results"][0])
        self.assertEqual(summary["window_results"][0]["strategy_metrics"]["top1"]["days_ranked"], 3)

    def test_run_walk_forward_evaluation_supports_regression_sklearn(self):
        prepared = prepare_training_data(
            self._build_walk_forward_sample_df(),
            task_type="regression",
            fill_method="median",
        )

        result = run_walk_forward_evaluation(
            prepared,
            model_kind="sklearn",
            fill_method="median",
            regressor="linear",
            min_train_rows=3,
            min_test_rows=1,
            max_windows=2,
        )

        summary = result.to_summary()
        self.assertEqual(summary["aggregate"]["window_count"], 2)
        self.assertEqual(summary["aggregate"]["selected_cutoff_count"], 2)
        self.assertEqual(summary["aggregate"]["rows_evaluated_total"], 3)
        self.assertIn("average_test_rmse", summary["aggregate"])
        self.assertIn("aggregate_test_metrics", summary["aggregate"])
        self.assertIn("strategy_metrics", summary["aggregate"])
        self.assertEqual(summary["aggregate"]["strategy_metrics"]["window_count_with_strategy_metrics"], 2)
        self.assertEqual(summary["aggregate"]["strategy_metrics"]["return_column"], "ret_fwd_5d")
        self.assertEqual(summary["window_results"][0]["model_summary"]["model_kind"], "sklearn")
        self.assertEqual(summary["window_results"][0]["model_summary"]["regressor"], "linear")
        self.assertTrue(summary["aggregate"]["average_test_rmse"] >= 0.0)
        self.assertIn("top1", summary["window_results"][0]["strategy_metrics"])


if __name__ == "__main__":
    unittest.main()
