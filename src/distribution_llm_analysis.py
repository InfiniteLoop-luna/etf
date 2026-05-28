from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"
ENV_PATH = PROJECT_ROOT / ".env"
LLM_SECTION_MARKER = "## 🧠 大模型二次综合分析"
LLM_SCHEMA_VERSION = "professional-v2"
DEFAULT_DISTRIBUTION_LLM_BASE_URL = "https://api.deepseek.com"
DEFAULT_DISTRIBUTION_LLM_MODEL = "deepseek-v4-pro"
DEFAULT_DISTRIBUTION_LLM_TIMEOUT_SECONDS = 60
DEFAULT_DISTRIBUTION_LLM_TEMPERATURE = 0.2
DEFAULT_DISTRIBUTION_LLM_MAX_TOKENS = 1600
ALLOWED_DISTRIBUTION_VERDICTS = {"强出货", "疑似出货", "中性", "偏洗盘"}
ALLOWED_DISTRIBUTION_RISK_LEVELS = {"高", "中", "低"}
LIST_FIELD_LIMITS = {
    "evidence_for": 4,
    "evidence_against": 4,
    "key_levels": 4,
    "scenario_analysis": 3,
    "watch_items": 4,
    "action_suggestion": 4,
}


@dataclass
class DistributionLLMConfig:
    enabled: bool = False
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    timeout_seconds: int = 30
    temperature: float = 0.2
    max_tokens: int = 1200

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


def _is_ascii_text(value: str) -> bool:
    return all(ord(ch) < 128 for ch in str(value or ""))


def _normalize_api_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not _is_ascii_text(text):
        logger.warning("Distribution LLM API key contains non-ASCII characters; ignoring candidate value")
        return ""
    return text


def load_distribution_llm_config() -> DistributionLLMConfig:
    env_values = _load_env_file()
    secrets = _load_secrets_toml()
    section = _get_secret_section(secrets, "distribution_llm")

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

    def pick(name: str, default: Any = None) -> Any:
        for candidate in iter_candidates(name):
            return candidate
        return default

    def pick_api_key(default: str = "") -> str:
        for candidate in iter_candidates("DISTRIBUTION_LLM_API_KEY"):
            normalized = _normalize_api_key(candidate)
            if normalized:
                return normalized
        for candidate in iter_candidates("DEEPSEEK_API_KEY"):
            normalized = _normalize_api_key(candidate)
            if normalized:
                return normalized
        return default

    api_key_value = pick_api_key("")

    return DistributionLLMConfig(
        enabled=_to_bool(pick("DISTRIBUTION_LLM_ENABLED", False), False),
        base_url=str(pick("DISTRIBUTION_LLM_BASE_URL", DEFAULT_DISTRIBUTION_LLM_BASE_URL)).strip(),
        api_key=api_key_value,
        model=str(pick("DISTRIBUTION_LLM_MODEL", DEFAULT_DISTRIBUTION_LLM_MODEL)).strip(),
        timeout_seconds=int(pick("DISTRIBUTION_LLM_TIMEOUT_SECONDS", DEFAULT_DISTRIBUTION_LLM_TIMEOUT_SECONDS) or DEFAULT_DISTRIBUTION_LLM_TIMEOUT_SECONDS),
        temperature=float(pick("DISTRIBUTION_LLM_TEMPERATURE", DEFAULT_DISTRIBUTION_LLM_TEMPERATURE) or DEFAULT_DISTRIBUTION_LLM_TEMPERATURE),
        max_tokens=int(pick("DISTRIBUTION_LLM_MAX_TOKENS", DEFAULT_DISTRIBUTION_LLM_MAX_TOKENS) or DEFAULT_DISTRIBUTION_LLM_MAX_TOKENS),
    )


def should_require_llm_refresh(cached_report: str | None, config: DistributionLLMConfig | None = None) -> bool:
    _ = config or load_distribution_llm_config()
    report_text = str(cached_report or "")
    return not report_text or LLM_SECTION_MARKER not in report_text or LLM_SCHEMA_VERSION not in report_text


def _strip_json_fence(text: str) -> str:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    return raw


def _extract_first_balanced_json_object(text: str) -> str | None:
    raw = str(text or "")
    start = raw.find("{")
    if start < 0:
        return None

    depth = 0
    in_string = False
    escape = False
    for idx in range(start, len(raw)):
        ch = raw[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return raw[start:idx + 1]
    return None


def parse_llm_json_object(text: str) -> dict[str, Any] | None:
    raw = _strip_json_fence(text)
    candidates = [raw]
    extracted = _extract_first_balanced_json_object(raw)
    if extracted and extracted not in candidates:
        candidates.append(extracted)

    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def make_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): make_json_safe(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]

    try:
        import numpy as np  # type: ignore

        if isinstance(value, np.generic):
            return make_json_safe(value.item())
        if isinstance(value, np.ndarray):
            return [make_json_safe(item) for item in value.tolist()]
    except Exception:
        pass

    try:
        import pandas as pd  # type: ignore

        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "item"):
        try:
            return make_json_safe(value.item())
        except Exception:
            pass

    return str(value)


def _coerce_text(value: Any, max_length: int = 280) -> str:
    text = str(value or "").strip()
    if len(text) > max_length:
        return text[: max_length - 1].rstrip() + "..."
    return text


def _coerce_text_list(value: Any, limit: int = 4, max_length: int = 180) -> list[str]:
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


def _clamp_confidence(value: Any) -> int:
    try:
        confidence = int(round(float(value)))
    except Exception:
        return 50
    return max(0, min(100, confidence))


def _risk_level_from_verdict(verdict: str) -> str:
    if verdict == "强出货":
        return "高"
    if verdict == "疑似出货":
        return "中"
    return "低"


def normalize_distribution_llm_result(result: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(result, dict) or not result:
        return None

    verdict = _coerce_text(result.get("verdict"), max_length=20)
    if verdict not in ALLOWED_DISTRIBUTION_VERDICTS:
        verdict = "中性"

    risk_level = _coerce_text(result.get("risk_level"), max_length=10)
    if risk_level not in ALLOWED_DISTRIBUTION_RISK_LEVELS:
        risk_level = _risk_level_from_verdict(verdict)

    normalized: dict[str, Any] = {
        "verdict": verdict,
        "risk_level": risk_level,
        "confidence": _clamp_confidence(result.get("confidence")),
        "summary": _coerce_text(result.get("summary"), max_length=360),
        "professional_view": _coerce_text(result.get("professional_view"), max_length=500),
        "data_quality_note": _coerce_text(result.get("data_quality_note"), max_length=280),
    }
    for field_name, limit in LIST_FIELD_LIMITS.items():
        normalized[field_name] = _coerce_text_list(result.get(field_name), limit=limit)
    return normalized


def analyze_distribution_payload(payload: dict[str, Any], config: DistributionLLMConfig | None = None) -> dict[str, Any] | None:
    resolved = config or load_distribution_llm_config()
    if not resolved.configured:
        return None

    url = resolved.base_url.rstrip("/") + "/chat/completions"
    system_prompt = (
        "你是一个专业、审慎、偏风控视角的A股主力行为研究员。"
        "你只能基于用户提供的结构化行情证据做判断，不能编造成交、价格、日期、资金或盘口数据。"
        "请区分已确认的量价证据、反证与数据缺口；tick/minute 缺失时必须降低置信度并说明原因。"
        "不要给出绝对买卖指令，不要承诺收益，只能给风险监控和后续观察建议。"
        "输出必须是 JSON 对象，字段固定为："
        "verdict, risk_level, confidence, summary, professional_view, "
        "evidence_for, evidence_against, key_levels, scenario_analysis, watch_items, "
        "action_suggestion, data_quality_note。"
        "其中 verdict 只能取：强出货、疑似出货、中性、偏洗盘。"
        "risk_level 只能取：高、中、低。confidence 为 0-100 的整数。"
        "evidence_for/evidence_against/key_levels/watch_items/action_suggestion 每项 1-4 条，"
        "scenario_analysis 每项 1-3 条，所有数组元素必须是短句。"
    )
    base_user_prompt = (
        "请基于下面这份结构化 payload 做二次综合分析，输出要像专业研究员给交易员/风控看的结论："
        "先判断是否存在主力出货风险，再解释为什么；"
        "请引用 payload 中已有的信号名称、关键日期、阶段跌幅、分时/分笔覆盖情况，"
        "并给出后续 1-5 个交易日应观察的价量条件。"
        "只输出 JSON，不要输出 markdown。\n\n"
        + json.dumps(make_json_safe(payload), ensure_ascii=False)
    )
    retry_user_prompt = (
        "请严格只返回单个 JSON 对象，不要输出解释、前缀、后缀、markdown、代码块。"
        "字段只允许 verdict, risk_level, confidence, summary, professional_view, "
        "evidence_for, evidence_against, key_levels, scenario_analysis, watch_items, "
        "action_suggestion, data_quality_note。\n\n"
        + json.dumps(make_json_safe(payload), ensure_ascii=False)
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
            if not content:
                logger.warning("Distribution LLM returned empty content")
                continue
            parsed = parse_llm_json_object(content)
            if not isinstance(parsed, dict):
                logger.warning("Distribution LLM returned non-JSON object content")
                continue
            normalized = normalize_distribution_llm_result(parsed)
            if not normalized:
                continue
            normalized["model"] = resolved.model
            return normalized
        except Exception as exc:
            logger.warning("Distribution LLM analysis failed: %s", exc)
            if attempt_index >= 2:
                return None
    return None


def render_distribution_llm_markdown(result: dict[str, Any] | None) -> list[str]:
    normalized = normalize_distribution_llm_result(result)
    if not normalized:
        return []

    verdict = normalized["verdict"]
    risk_level = normalized["risk_level"]
    confidence_text = f"{normalized['confidence']}"
    summary = normalized.get("summary") or ""
    professional_view = normalized.get("professional_view") or ""
    data_quality_note = normalized.get("data_quality_note") or ""

    lines = ["", "---", LLM_SECTION_MARKER, ""]
    lines.append(f"- **综合判断**：{verdict}")
    lines.append(f"- **风险等级**：{risk_level}")
    lines.append(f"- **置信度**：{confidence_text}/100")
    lines.append(f"- **分析版本**：{LLM_SCHEMA_VERSION}")
    if summary:
        lines.extend(["", f"> {summary}"])
    if professional_view:
        lines.extend(["", "### 专业解读", professional_view])

    for title, key in [
        ("核心支持证据", "evidence_for"),
        ("反证与不确定点", "evidence_against"),
        ("关键价量观察位", "key_levels"),
        ("情景推演", "scenario_analysis"),
        ("后续观察点", "watch_items"),
        ("风控与操作提示", "action_suggestion"),
    ]:
        items = normalized.get(key) or []
        if not items:
            continue
        lines.extend(["", f"### {title}"])
        for item in items:
            lines.append(f"- {item}")
    if data_quality_note:
        lines.extend(["", "### 数据质量说明", f"- {data_quality_note}"])
    lines.extend(["", "> 以上为基于已缓存行情证据的风险分析，不构成确定性交易指令。"])
    return lines
