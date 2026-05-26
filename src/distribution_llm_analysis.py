from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SECRETS_PATH = PROJECT_ROOT / ".streamlit" / "secrets.toml"
LLM_SECTION_MARKER = "## 🧠 大模型二次综合分析"


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


def load_distribution_llm_config() -> DistributionLLMConfig:
    secrets = _load_secrets_toml()
    section = secrets.get("distribution_llm") if isinstance(secrets.get("distribution_llm"), dict) else {}

    def pick(name: str, default: Any = None) -> Any:
        if os.getenv(name) not in {None, ""}:
            return os.getenv(name)
        if name in secrets and secrets.get(name) not in {None, ""}:
            return secrets.get(name)
        if name in section and section.get(name) not in {None, ""}:
            return section.get(name)
        snake = name.lower()
        if snake in section and section.get(snake) not in {None, ""}:
            return section.get(snake)
        return default

    return DistributionLLMConfig(
        enabled=_to_bool(pick("DISTRIBUTION_LLM_ENABLED", False)),
        base_url=str(pick("DISTRIBUTION_LLM_BASE_URL", "")).strip(),
        api_key=str(pick("DISTRIBUTION_LLM_API_KEY", "")).strip(),
        model=str(pick("DISTRIBUTION_LLM_MODEL", "")).strip(),
        timeout_seconds=int(pick("DISTRIBUTION_LLM_TIMEOUT_SECONDS", 30) or 30),
        temperature=float(pick("DISTRIBUTION_LLM_TEMPERATURE", 0.2) or 0.2),
        max_tokens=int(pick("DISTRIBUTION_LLM_MAX_TOKENS", 1200) or 1200),
    )


def should_require_llm_refresh(cached_report: str | None, config: DistributionLLMConfig | None = None) -> bool:
    resolved = config or load_distribution_llm_config()
    if not resolved.configured:
        return False
    return not cached_report or LLM_SECTION_MARKER not in str(cached_report)


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


def analyze_distribution_payload(payload: dict[str, Any], config: DistributionLLMConfig | None = None) -> dict[str, Any] | None:
    resolved = config or load_distribution_llm_config()
    if not resolved.configured:
        return None

    url = resolved.base_url.rstrip("/") + "/chat/completions"
    system_prompt = (
        "你是一个严格基于结构化行情证据做判断的A股主力行为分析助手。"
        "不要编造数据，不要重复输入中的数字太多。"
        "输出必须是 JSON 对象，字段固定为："
        "verdict, confidence, summary, evidence_for, evidence_against, watch_items。"
        "其中 verdict 只能取：强出货、疑似出货、中性、偏洗盘。"
        "confidence 为 0-100 的整数。evidence_for / evidence_against / watch_items 都是 1-4 条字符串数组。"
    )
    user_prompt = (
        "请基于下面这份结构化 payload 做二次综合分析。"
        "如果 tick/minute 缺失，要明确降低置信度。"
        "只输出 JSON，不要输出 markdown。\n\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    request_payload = {
        "model": resolved.model,
        "temperature": resolved.temperature,
        "max_tokens": resolved.max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {resolved.api_key}",
        "Content-Type": "application/json",
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
            return None
        parsed = json.loads(_strip_json_fence(content))
        if not isinstance(parsed, dict):
            return None
        parsed["model"] = resolved.model
        return parsed
    except Exception as exc:
        logger.warning("Distribution LLM analysis failed: %s", exc)
        return None


def render_distribution_llm_markdown(result: dict[str, Any] | None) -> list[str]:
    if not isinstance(result, dict) or not result:
        return []
    verdict = str(result.get("verdict") or "-").strip() or "-"
    summary = str(result.get("summary") or "").strip()
    confidence = result.get("confidence")
    try:
        confidence_text = f"{int(confidence)}"
    except Exception:
        confidence_text = "-"

    lines = ["", "---", LLM_SECTION_MARKER, ""]
    lines.append(f"- **综合判断**：{verdict}")
    lines.append(f"- **置信度**：{confidence_text}/100")
    if summary:
        lines.extend(["", f"> {summary}"])

    for title, key in [
        ("支持证据", "evidence_for"),
        ("反证与不确定点", "evidence_against"),
        ("后续观察点", "watch_items"),
    ]:
        items = [str(item).strip() for item in (result.get(key) or []) if str(item).strip()]
        if not items:
            continue
        lines.extend(["", f"### {title}"])
        for item in items[:4]:
            lines.append(f"- {item}")
    return lines
