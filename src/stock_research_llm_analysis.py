from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

from src.distribution_llm_analysis import make_json_safe, parse_llm_json_object

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"
ENV_PATH = PROJECT_ROOT / ".env"

STOCK_RESEARCH_LLM_SECTION_MARKER = "## 🧠 个股深度研究 LLM 分析"
STOCK_RESEARCH_LLM_SCHEMA_VERSION = "stock-research-v1"

DEFAULT_STOCK_RESEARCH_LLM_BASE_URL = "https://api.deepseek.com"
DEFAULT_STOCK_RESEARCH_LLM_MODEL = "deepseek-v4-pro"
DEFAULT_STOCK_RESEARCH_LLM_TIMEOUT_SECONDS = 90
DEFAULT_STOCK_RESEARCH_LLM_TEMPERATURE = 0.2
DEFAULT_STOCK_RESEARCH_LLM_MAX_TOKENS = 3200

ALLOWED_RESEARCH_VERDICTS = {"重点跟踪", "谨慎跟踪", "观察", "规避"}
ALLOWED_RESEARCH_RISK_LEVELS = {"高", "中", "低"}
ALLOWED_RESEARCH_GRADES = {"A", "B", "C", "D"}


@dataclass
class StockResearchLLMConfig:
    enabled: bool = False
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    timeout_seconds: int = DEFAULT_STOCK_RESEARCH_LLM_TIMEOUT_SECONDS
    temperature: float = DEFAULT_STOCK_RESEARCH_LLM_TEMPERATURE
    max_tokens: int = DEFAULT_STOCK_RESEARCH_LLM_MAX_TOKENS

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.base_url and self.api_key and self.model)


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


def _is_ascii_text(value: str) -> bool:
    return all(ord(ch) < 128 for ch in str(value or ""))


def _normalize_api_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not _is_ascii_text(text):
        logger.warning("Stock research LLM API key contains non-ASCII characters; ignoring candidate value")
        return ""
    return text


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


def _get_secret_section(payload: dict[str, Any], name: str) -> dict[str, Any]:
    value = payload.get(name)
    return value if isinstance(value, dict) else {}


def load_stock_research_llm_config() -> StockResearchLLMConfig:
    env_values = _load_env_file()
    secrets = _load_secrets_toml()
    stock_section = _get_secret_section(secrets, "stock_research_llm")
    distribution_section = _get_secret_section(secrets, "distribution_llm")

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
        stock_value = stock_section.get(name)
        if stock_value not in {None, ""}:
            yield stock_value
        stock_snake = stock_section.get(name.lower())
        if stock_snake not in {None, ""}:
            yield stock_snake
        distribution_value = distribution_section.get(name)
        if distribution_value not in {None, ""}:
            yield distribution_value
        distribution_snake = distribution_section.get(name.lower())
        if distribution_snake not in {None, ""}:
            yield distribution_snake

    def pick(names: list[str], default: Any = None) -> Any:
        for name in names:
            for candidate in iter_candidates(name):
                return candidate
        return default

    def pick_api_key() -> str:
        for name in ["STOCK_RESEARCH_LLM_API_KEY", "DISTRIBUTION_LLM_API_KEY", "DEEPSEEK_API_KEY"]:
            for candidate in iter_candidates(name):
                normalized = _normalize_api_key(candidate)
                if normalized:
                    return normalized
        return ""

    enabled_default = pick(["DISTRIBUTION_LLM_ENABLED"], False)
    return StockResearchLLMConfig(
        enabled=_to_bool(pick(["STOCK_RESEARCH_LLM_ENABLED"], enabled_default), False),
        base_url=str(
            pick(
                ["STOCK_RESEARCH_LLM_BASE_URL", "DISTRIBUTION_LLM_BASE_URL"],
                DEFAULT_STOCK_RESEARCH_LLM_BASE_URL,
            )
        ).strip(),
        api_key=pick_api_key(),
        model=str(
            pick(
                ["STOCK_RESEARCH_LLM_MODEL", "DISTRIBUTION_LLM_MODEL"],
                DEFAULT_STOCK_RESEARCH_LLM_MODEL,
            )
        ).strip(),
        timeout_seconds=int(
            pick(
                ["STOCK_RESEARCH_LLM_TIMEOUT_SECONDS", "DISTRIBUTION_LLM_TIMEOUT_SECONDS"],
                DEFAULT_STOCK_RESEARCH_LLM_TIMEOUT_SECONDS,
            )
            or DEFAULT_STOCK_RESEARCH_LLM_TIMEOUT_SECONDS
        ),
        temperature=float(
            pick(
                ["STOCK_RESEARCH_LLM_TEMPERATURE", "DISTRIBUTION_LLM_TEMPERATURE"],
                DEFAULT_STOCK_RESEARCH_LLM_TEMPERATURE,
            )
            or DEFAULT_STOCK_RESEARCH_LLM_TEMPERATURE
        ),
        max_tokens=int(
            pick(
                ["STOCK_RESEARCH_LLM_MAX_TOKENS", "DISTRIBUTION_LLM_MAX_TOKENS"],
                DEFAULT_STOCK_RESEARCH_LLM_MAX_TOKENS,
            )
            or DEFAULT_STOCK_RESEARCH_LLM_MAX_TOKENS
        ),
    )


def should_require_stock_research_refresh(cached_report: str | None) -> bool:
    report_text = str(cached_report or "")
    return (
        not report_text
        or STOCK_RESEARCH_LLM_SECTION_MARKER not in report_text
        or STOCK_RESEARCH_LLM_SCHEMA_VERSION not in report_text
    )


def _coerce_text(value: Any, max_length: int = 500) -> str:
    text = str(value or "").strip()
    if len(text) > max_length:
        return text[: max_length - 1].rstrip() + "..."
    return text


def _coerce_text_list(value: Any, limit: int = 6, max_length: int = 220) -> list[str]:
    if value is None:
        return []
    raw_items = value if isinstance(value, list) else [value]
    items: list[str] = []
    for item in raw_items:
        text = _coerce_text(item, max_length=max_length)
        if text:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def _clamp_int(value: Any, default: int = 50, min_value: int = 0, max_value: int = 100) -> int:
    try:
        parsed = int(round(float(value)))
    except Exception:
        parsed = default
    return max(min_value, min(max_value, parsed))


def _normalize_quality_score(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    score = _clamp_int(raw.get("score"), default=50)
    grade = _coerce_text(raw.get("grade"), max_length=3)
    if grade not in ALLOWED_RESEARCH_GRADES:
        if score >= 80:
            grade = "A"
        elif score >= 65:
            grade = "B"
        elif score >= 50:
            grade = "C"
        else:
            grade = "D"
    return {
        "score": score,
        "grade": grade,
        "drivers": _coerce_text_list(raw.get("drivers"), limit=5),
        "weaknesses": _coerce_text_list(raw.get("weaknesses"), limit=5),
    }


def _normalize_step_analysis(value: Any) -> dict[str, str]:
    raw = value if isinstance(value, dict) else {}
    normalized: dict[str, str] = {}
    for index in range(9):
        key = f"step{index}"
        normalized[key] = _coerce_text(raw.get(key), max_length=650)
    return normalized


def normalize_stock_research_llm_result(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(result, dict) or not result:
        return None

    verdict = _coerce_text(result.get("verdict"), max_length=20)
    if verdict not in ALLOWED_RESEARCH_VERDICTS:
        verdict = "观察"

    risk_level = _coerce_text(result.get("risk_level"), max_length=10)
    if risk_level not in ALLOWED_RESEARCH_RISK_LEVELS:
        risk_level = "中" if verdict in {"谨慎跟踪", "观察"} else "高"

    return {
        "verdict": verdict,
        "risk_level": risk_level,
        "confidence": _clamp_int(result.get("confidence"), default=50),
        "summary": _coerce_text(result.get("summary"), max_length=500),
        "investment_thesis": _coerce_text(result.get("investment_thesis"), max_length=700),
        "valuation_view": _coerce_text(result.get("valuation_view"), max_length=500),
        "timing_view": _coerce_text(result.get("timing_view"), max_length=500),
        "quality_score": _normalize_quality_score(result.get("quality_score")),
        "key_evidence": _coerce_text_list(result.get("key_evidence"), limit=8),
        "risk_factors": _coerce_text_list(result.get("risk_factors"), limit=8),
        "watch_items": _coerce_text_list(result.get("watch_items"), limit=8),
        "step_analysis": _normalize_step_analysis(result.get("step_analysis")),
    }


def analyze_stock_research_payload(
    fact_pack: dict[str, Any],
    config: StockResearchLLMConfig | None = None,
) -> dict[str, Any] | None:
    resolved = config or load_stock_research_llm_config()
    if not resolved.configured:
        return None

    url = resolved.base_url.rstrip("/") + "/chat/completions"
    system_prompt = (
        "你是一个专业、审慎、证据驱动的A股个股深度研究员。"
        "你只能基于用户提供的结构化 FactPack 做分析，不能编造财务、价格、新闻、研报或行业数据。"
        "FactPack 可能包含 supplemental 补充证据块，覆盖主营构成、新闻、研报、资金流、龙虎榜和行业参考。"
        "只有当某个 supplemental 数据块 status=ok 时，才允许基于该块形成结论；status=empty/failed/disabled/missing 时只能作为数据缺口说明。"
        "当数据缺失、过期或口径不完整时必须明确降低置信度。"
        "不要给出确定性买卖指令，不要承诺收益。"
        "输出必须是单个 JSON 对象，字段固定为："
        "verdict, risk_level, confidence, summary, investment_thesis, valuation_view, timing_view, "
        "quality_score, key_evidence, risk_factors, watch_items, step_analysis。"
        "verdict 只能取：重点跟踪、谨慎跟踪、观察、规避。"
        "risk_level 只能取：高、中、低。confidence 和 quality_score.score 为 0-100 整数。"
        "quality_score 为对象，包含 score, grade, drivers, weaknesses；grade 只能取 A/B/C/D。"
        "step_analysis 必须包含 step0 到 step8，每项为一段简洁中文分析。"
        "key_evidence/risk_factors/watch_items 每项 3-8 条短句。"
    )
    base_user_prompt = (
        "请基于下面 FactPack 生成自选股深度研究结论。"
        "重点回答：公司质地如何、当前估值和位置是否匹配、主要风险是什么、后续应该跟踪哪些触发条件。"
        "必须引用 FactPack 中已有的日期、指标、补充证据和信号；不能引用不存在的外部事实。"
        "如果 supplemental 中包含 status=ok 的资金流、新闻、研报或主营构成数据，请把它们作为 key_evidence、risk_factors 或 watch_items 的候选证据；"
        "如果这些数据块不可用，请在结论里体现数据覆盖不足。"
        "只输出 JSON，不要输出 markdown。\n\n"
        + json.dumps(make_json_safe(fact_pack), ensure_ascii=False)
    )
    retry_user_prompt = (
        "请严格只返回单个 JSON 对象，不要输出解释、前缀、后缀、markdown、代码块。"
        "字段只允许 verdict, risk_level, confidence, summary, investment_thesis, valuation_view, timing_view, "
        "quality_score, key_evidence, risk_factors, watch_items, step_analysis。\n\n"
        + json.dumps(make_json_safe(fact_pack), ensure_ascii=False)
    )
    headers = {
        "Authorization": f"Bearer {resolved.api_key}",
        "Content-Type": "application/json",
    }

    for attempt_index, user_prompt in enumerate((base_user_prompt, retry_user_prompt), start=1):
        request_payload = {
            "model": resolved.model,
            "temperature": resolved.temperature,
            "max_tokens": resolved.max_tokens,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        try:
            response = requests.post(url, headers=headers, json=request_payload, timeout=resolved.timeout_seconds)
            response.raise_for_status()
            data = response.json()
            content = (
                ((data.get("choices") or [{}])[0].get("message") or {}).get("content")
                if isinstance(data, dict)
                else None
            )
            parsed = parse_llm_json_object(content or "")
            if not isinstance(parsed, dict):
                logger.warning("Stock research LLM returned non-JSON content")
                continue
            normalized = normalize_stock_research_llm_result(parsed)
            if normalized:
                normalized["model"] = resolved.model
                return normalized
        except Exception as exc:
            logger.warning("Stock research LLM analysis failed: %s", exc)
            if attempt_index >= 2:
                return None
    return None


def render_stock_research_llm_markdown(result: dict[str, Any] | None) -> list[str]:
    normalized = normalize_stock_research_llm_result(result)
    if not normalized:
        return []

    lines = ["", "---", STOCK_RESEARCH_LLM_SECTION_MARKER, ""]
    lines.append(f"- **综合判断**：{normalized['verdict']}")
    lines.append(f"- **风险等级**：{normalized['risk_level']}")
    lines.append(f"- **置信度**：{normalized['confidence']}/100")
    lines.append(f"- **公司质地评分**：{normalized['quality_score']['score']}/100（{normalized['quality_score']['grade']}级）")
    lines.append(f"- **分析版本**：{STOCK_RESEARCH_LLM_SCHEMA_VERSION}")
    if normalized.get("summary"):
        lines.extend(["", f"> {normalized['summary']}"])
    if normalized.get("investment_thesis"):
        lines.extend(["", "### 核心投资命题", normalized["investment_thesis"]])
    if normalized.get("valuation_view"):
        lines.extend(["", "### 估值与赔率", normalized["valuation_view"]])
    if normalized.get("timing_view"):
        lines.extend(["", "### 位置与节奏", normalized["timing_view"]])

    quality = normalized["quality_score"]
    for title, items in [
        ("质地加分项", quality.get("drivers") or []),
        ("质地扣分项", quality.get("weaknesses") or []),
        ("关键证据", normalized.get("key_evidence") or []),
        ("主要风险", normalized.get("risk_factors") or []),
        ("后续跟踪清单", normalized.get("watch_items") or []),
    ]:
        if not items:
            continue
        lines.extend(["", f"### {title}"])
        for item in items:
            lines.append(f"- {item}")

    step_titles = {
        "step0": "Step 0 任务锁定",
        "step1": "Step 1 宏观与周期定位",
        "step2": "Step 2 产业链拆解",
        "step3": "Step 3 公司质量评分",
        "step4": "Step 4 业绩弹性测算",
        "step5": "Step 5 风险分析",
        "step6": "Step 6 估值与买卖时机",
        "step7": "Step 7 对标分析",
        "step8": "Step 8 跟踪计划",
    }
    lines.extend(["", "### Step 0-8 分析框架"])
    for key, title in step_titles.items():
        content = normalized["step_analysis"].get(key)
        if content:
            lines.extend(["", f"#### {title}", content])

    lines.extend(["", "> 本报告仅供研究跟踪使用，不构成投资建议。"])
    return lines
