from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from .models import normalize_timestamp

DEFAULT_BENCHMARK_TS_CODE = "000300.SH"
EXIT_FLAT_THRESHOLD = 0.02


def _build_cycle_id(ts_code: str, mention_time: str, index: int) -> str:
    symbol = ts_code.split(".", 1)[0]
    stamp = normalize_timestamp(mention_time).strftime("%Y%m%d%H%M%S")
    return f"{symbol}-{stamp}-{index}"


def _close_cycle(cycle: dict, mention: dict | None, reason: str, close_time: str) -> None:
    cycle["cycle_status"] = "closed" if reason != "timeout" else "expired"
    cycle["close_reason"] = reason
    cycle["close_mention_id"] = mention.get("mention_id") if mention else None
    cycle["cycle_close_time"] = normalize_timestamp(close_time).strftime("%Y-%m-%d %H:%M:%S")
    cycle["last_event_time"] = cycle["cycle_close_time"]


def build_stock_cycles(mentions: list[dict], inactivity_days: int = 30, as_of_date: date | None = None) -> list[dict]:
    cycles: list[dict] = []
    active_by_code: dict[str, dict] = {}
    sequence_by_code: defaultdict[str, int] = defaultdict(int)

    ordered_mentions = sorted(mentions, key=lambda item: (item["ts_code"], normalize_timestamp(item["mention_time"])))
    for mention in ordered_mentions:
        ts_code = mention["ts_code"]
        mention_time = normalize_timestamp(mention["mention_time"])
        active_cycle = active_by_code.get(ts_code)
        if active_cycle:
            last_event_time = normalize_timestamp(active_cycle["last_event_time"])
            if (mention_time.date() - last_event_time.date()).days > inactivity_days:
                _close_cycle(active_cycle, None, "timeout", active_cycle["last_event_time"])
                active_by_code.pop(ts_code, None)
                active_cycle = None

        if bool(mention.get("force_new_cycle")) and active_cycle is not None:
            _close_cycle(active_cycle, None, "manual_split", active_cycle["last_event_time"])
            active_by_code.pop(ts_code, None)
            active_cycle = None

        direction = mention.get("direction") or "bullish"
        if direction == "bullish":
            if active_cycle is None:
                sequence_by_code[ts_code] += 1
                cycle = {
                    "cycle_id": _build_cycle_id(ts_code, mention["mention_time"], sequence_by_code[ts_code]),
                    "ts_code": ts_code,
                    "cycle_status": "active",
                    "cycle_open_time": normalize_timestamp(mention["mention_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "cycle_close_time": None,
                    "open_mention_id": mention["mention_id"],
                    "close_mention_id": None,
                    "close_reason": None,
                    "last_event_time": normalize_timestamp(mention["mention_time"]).strftime("%Y-%m-%d %H:%M:%S"),
                    "mention_ids": [mention["mention_id"]],
                    "event_count": 1,
                }
                cycles.append(cycle)
                active_by_code[ts_code] = cycle
            else:
                active_cycle["last_event_time"] = mention_time.strftime("%Y-%m-%d %H:%M:%S")
                active_cycle["mention_ids"].append(mention["mention_id"])
                active_cycle["event_count"] += 1
                if active_cycle["cycle_status"] == "trimmed":
                    active_cycle["cycle_status"] = "active"
            continue

        if active_cycle is None:
            continue

        active_cycle["last_event_time"] = mention_time.strftime("%Y-%m-%d %H:%M:%S")
        active_cycle["mention_ids"].append(mention["mention_id"])
        active_cycle["event_count"] += 1

        if direction == "trim_signal":
            active_cycle["cycle_status"] = "trimmed"
        elif direction == "exit_signal":
            _close_cycle(active_cycle, mention, "explicit_exit", mention["mention_time"])
            active_by_code.pop(ts_code, None)
        elif direction == "bearish":
            _close_cycle(active_cycle, mention, "thesis_reversal", mention["mention_time"])
            active_by_code.pop(ts_code, None)

    if as_of_date:
        for ts_code, cycle in list(active_by_code.items()):
            last_event = normalize_timestamp(cycle["last_event_time"]).date()
            if (as_of_date - last_event).days > inactivity_days:
                _close_cycle(cycle, None, "timeout", cycle["last_event_time"])
                active_by_code.pop(ts_code, None)

    for cycle in cycles:
        cycle.pop("last_event_time", None)
    return cycles


def _slice_price_window(rows: list[dict], start_date: date, end_date: date | None) -> list[dict]:
    ordered = sorted(rows, key=lambda item: normalize_timestamp(item["trade_date"]))
    window = []
    for row in ordered:
        row_date = normalize_timestamp(row["trade_date"]).date()
        if row_date < start_date:
            continue
        if end_date and row_date > end_date:
            continue
        window.append({"trade_date": row_date, "close": float(row["close"])})
    return window


def _calculate_max_drawdown(closes: list[float]) -> float | None:
    if not closes:
        return None
    peak = closes[0]
    max_drawdown = 0.0
    for close in closes:
        if close > peak:
            peak = close
        drawdown = (close / peak) - 1.0
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _calculate_total_return(window: list[dict]) -> float | None:
    if not window:
        return None
    open_close = window[0]["close"]
    final_close = window[-1]["close"]
    return (final_close / open_close) - 1.0 if open_close else None


def _evaluate_exit_quality(final_close: float, future_closes: list[float], horizon_days: int) -> bool | None:
    if final_close <= 0:
        return None
    horizon_window = future_closes[: max(int(horizon_days), 0)]
    if not horizon_window:
        return None
    return max(horizon_window) <= final_close * (1.0 + EXIT_FLAT_THRESHOLD)


def score_cycles(cycles: list[dict], price_history_by_code: dict[str, list[dict]]) -> list[dict]:
    scores: list[dict] = []
    benchmark_history = price_history_by_code.get(DEFAULT_BENCHMARK_TS_CODE) or []
    for cycle in cycles:
        history = price_history_by_code.get(cycle["ts_code"]) or []
        open_date = normalize_timestamp(cycle["cycle_open_time"]).date()
        close_date = normalize_timestamp(cycle["cycle_close_time"]).date() if cycle.get("cycle_close_time") else None
        window = _slice_price_window(history, open_date, close_date)
        if not window:
            continue

        final_close = window[-1]["close"]
        score_end_date = close_date or window[-1]["trade_date"]
        total_return = _calculate_total_return(window)
        max_drawdown = _calculate_max_drawdown([row["close"] for row in window])
        benchmark_return = None
        excess_return = None
        if benchmark_history:
            benchmark_window = _slice_price_window(benchmark_history, open_date, score_end_date)
            benchmark_return = _calculate_total_return(benchmark_window)
            if total_return is not None and benchmark_return is not None:
                excess_return = total_return - benchmark_return

        future_rows = []
        if close_date:
            for row in sorted(history, key=lambda item: normalize_timestamp(item["trade_date"])):
                row_date = normalize_timestamp(row["trade_date"]).date()
                if row_date > close_date:
                    future_rows.append(float(row["close"]))
        exit_quality_2d = _evaluate_exit_quality(final_close, future_rows, 2)
        exit_quality_5d = _evaluate_exit_quality(final_close, future_rows, 5)
        exit_quality_10d = _evaluate_exit_quality(final_close, future_rows, 10)
        exit_quality_20d = _evaluate_exit_quality(final_close, future_rows, 20)

        scores.append(
            {
                "cycle_id": cycle["cycle_id"],
                "ts_code": cycle["ts_code"],
                "total_return": total_return,
                "benchmark_return": benchmark_return,
                "excess_return": excess_return,
                "max_drawdown": max_drawdown,
                "hold_days": max(len(window) - 1, 0),
                "exit_quality_2d": exit_quality_2d,
                "exit_quality_5d": exit_quality_5d,
                "exit_quality_10d": exit_quality_10d,
                "exit_quality_20d": exit_quality_20d,
            }
        )
    return scores
