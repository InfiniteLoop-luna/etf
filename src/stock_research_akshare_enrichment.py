from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"
SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"

SUPPLEMENTAL_BLOCK_NAMES = (
    "business_composition",
    "news",
    "research_reports",
    "money_flow",
    "lhb",
    "industry_peer_hint",
)


@dataclass(frozen=True)
class StockResearchAkshareConfig:
    enabled: bool = False
    business_limit: int = 12
    news_limit: int = 8
    research_report_limit: int = 6
    money_flow_limit: int = 8
    lhb_limit: int = 6
    industry_peer_limit: int = 8


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int, minimum: int = 0, maximum: int = 50) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


@lru_cache(maxsize=1)
def _load_env_file() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    try:
        for raw_line in ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            values[key] = value
    except Exception as exc:
        logger.warning("Failed to read %s: %s", ENV_PATH, exc)
    return values


def _load_secrets_toml() -> dict[str, Any]:
    if not SECRETS_PATH.exists():
        return {}
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore
        except ImportError:
            return {}
    try:
        with open(SECRETS_PATH, "rb") as f:
            payload = tomllib.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        logger.warning("Failed to read %s: %s", SECRETS_PATH, exc)
        return {}


def _get_secret_section(payload: dict[str, Any], name: str) -> dict[str, Any]:
    value = payload.get(name)
    return value if isinstance(value, dict) else {}


def load_stock_research_akshare_config() -> StockResearchAkshareConfig:
    env_values = _load_env_file()
    secrets = _load_secrets_toml()
    section = _get_secret_section(secrets, "stock_research_akshare")

    def iter_candidates(name: str):
        env_value = os.getenv(name)
        if env_value not in {None, ""}:
            yield env_value
        env_file_value = env_values.get(name)
        if env_file_value not in {None, ""}:
            yield env_file_value
        secret_value = secrets.get(name)
        if secret_value not in {None, ""}:
            yield secret_value
        section_value = section.get(name)
        if section_value not in {None, ""}:
            yield section_value
        snake_value = section.get(name.lower())
        if snake_value not in {None, ""}:
            yield snake_value

    def pick(names: list[str], default: Any = None) -> Any:
        for name in names:
            for candidate in iter_candidates(name):
                return candidate
        return default

    return StockResearchAkshareConfig(
        enabled=_to_bool(
            pick(["STOCK_RESEARCH_ENABLE_AKSHARE", "STOCK_RESEARCH_AKSHARE_ENABLED"], False),
            False,
        ),
        business_limit=_to_int(pick(["STOCK_RESEARCH_BUSINESS_LIMIT"], 12), 12, 0, 30),
        news_limit=_to_int(pick(["STOCK_RESEARCH_NEWS_LIMIT"], 8), 8, 0, 30),
        research_report_limit=_to_int(pick(["STOCK_RESEARCH_REPORT_LIMIT"], 6), 6, 0, 20),
        money_flow_limit=_to_int(pick(["STOCK_RESEARCH_MONEY_FLOW_LIMIT"], 8), 8, 0, 30),
        lhb_limit=_to_int(pick(["STOCK_RESEARCH_LHB_LIMIT"], 6), 6, 0, 20),
        industry_peer_limit=_to_int(pick(["STOCK_RESEARCH_INDUSTRY_PEER_LIMIT"], 8), 8, 0, 30),
    )


def should_enable_stock_research_akshare(config: StockResearchAkshareConfig | None = None) -> bool:
    return bool((config or load_stock_research_akshare_config()).enabled)


def _normalize_ts_code(ts_code: str | None) -> str:
    return str(ts_code or "").strip().upper()


def _pure_symbol(ts_code: str | None) -> str:
    code = _normalize_ts_code(ts_code)
    return code.split(".", 1)[0]


def _market(ts_code: str | None) -> str:
    code = _normalize_ts_code(ts_code)
    if code.endswith(".SH"):
        return "sh"
    if code.endswith(".BJ"):
        return "bj"
    return "sz"


def _exchange_prefixed_symbol(ts_code: str | None) -> str:
    code = _normalize_ts_code(ts_code)
    symbol = _pure_symbol(code)
    if code.endswith(".SH"):
        return f"SH{symbol}"
    if code.endswith(".BJ"):
        return f"BJ{symbol}"
    return f"SZ{symbol}"


def _safe_text(value: Any, max_length: int = 220) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value).strip()
    if not text:
        return None
    if len(text) > max_length:
        return text[: max_length - 1].rstrip() + "..."
    return text


def _records(df: Any, *, limit: int, max_columns: int = 12) -> list[dict[str, Any]]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty or limit <= 0:
        return []
    out = df.head(limit).copy()
    columns = [str(col) for col in out.columns[:max_columns]]
    out.columns = [str(col) for col in out.columns]
    rows: list[dict[str, Any]] = []
    for row in out[columns].to_dict(orient="records"):
        clean = {str(key): _safe_text(value) for key, value in row.items()}
        clean = {key: value for key, value in clean.items() if value is not None}
        if clean:
            rows.append(clean)
    return rows


def _block(
    name: str,
    *,
    source: str,
    status: str,
    items: list[dict[str, Any]] | None = None,
    row_count: int = 0,
    error: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "source": source,
        "status": status,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "row_count": int(row_count or 0),
        "items": items or [],
        "error": str(error or "")[:500] or None,
        "meta": meta or {},
    }


def _disabled_supplemental(error: str | None = None) -> dict[str, dict[str, Any]]:
    status = "failed" if error else "disabled"
    return {
        name: _block(name, source="akshare", status=status, error=error)
        for name in SUPPLEMENTAL_BLOCK_NAMES
    }


def _call_block(
    name: str,
    source: str,
    fetcher,
    *,
    limit: int,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        return _block(name, source=source, status="disabled", meta=meta)
    try:
        df = fetcher()
        row_count = int(len(df)) if isinstance(df, pd.DataFrame) else 0
        items = _records(df, limit=limit)
        status = "ok" if items else "empty"
        return _block(name, source=source, status=status, items=items, row_count=row_count, meta=meta)
    except Exception as exc:
        logger.warning("AkShare supplemental block %s failed: %s", name, exc)
        return _block(name, source=source, status="failed", error=str(exc), meta=meta)


def build_stock_research_supplemental(
    ts_code: str,
    *,
    stock_name: str = "",
    industry: str = "",
    enabled: bool | None = None,
    config: StockResearchAkshareConfig | None = None,
    ak_client: Any | None = None,
) -> dict[str, dict[str, Any]]:
    config = config or load_stock_research_akshare_config()
    active = config.enabled if enabled is None else bool(enabled and config.enabled)
    if not active:
        return _disabled_supplemental()

    if ak_client is None:
        try:
            import akshare as ak_client  # type: ignore
        except Exception as exc:
            return _disabled_supplemental(error=f"akshare import failed: {exc}")

    pure_symbol = _pure_symbol(ts_code)
    prefixed_symbol = _exchange_prefixed_symbol(ts_code)
    market = _market(ts_code)
    industry_name = str(industry or "").strip()
    meta = {
        "ts_code": _normalize_ts_code(ts_code),
        "symbol": pure_symbol,
        "stock_name": str(stock_name or "").strip(),
    }

    return {
        "business_composition": _call_block(
            "business_composition",
            "akshare.stock_zygc_em",
            lambda: ak_client.stock_zygc_em(symbol=prefixed_symbol),
            limit=config.business_limit,
            meta={**meta, "ak_symbol": prefixed_symbol},
        ),
        "news": _call_block(
            "news",
            "akshare.stock_news_em",
            lambda: ak_client.stock_news_em(symbol=pure_symbol),
            limit=config.news_limit,
            meta=meta,
        ),
        "research_reports": _call_block(
            "research_reports",
            "akshare.stock_research_report_em",
            lambda: ak_client.stock_research_report_em(symbol=pure_symbol),
            limit=config.research_report_limit,
            meta=meta,
        ),
        "money_flow": _call_block(
            "money_flow",
            "akshare.stock_individual_fund_flow",
            lambda: ak_client.stock_individual_fund_flow(stock=pure_symbol, market=market),
            limit=config.money_flow_limit,
            meta={**meta, "market": market},
        ),
        "lhb": _call_block(
            "lhb",
            "akshare.stock_lhb_stock_detail_date_em",
            lambda: ak_client.stock_lhb_stock_detail_date_em(symbol=pure_symbol),
            limit=config.lhb_limit,
            meta=meta,
        ),
        "industry_peer_hint": _call_block(
            "industry_peer_hint",
            "akshare.stock_board_industry_cons_em",
            lambda: ak_client.stock_board_industry_cons_em(symbol=industry_name),
            limit=config.industry_peer_limit if industry_name else 0,
            meta={**meta, "industry": industry_name},
        ),
    }
