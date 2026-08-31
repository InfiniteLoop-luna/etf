from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from src.etf_morning_report import BEIJING_TZ, PROJECT_ROOT, REPORT_DIR, build_report_digest


NOTIFICATION_DIR = REPORT_DIR / "notifications"
SERVERCHAN_TURBO_API = "https://sctapi.ftqq.com"
SERVERCHAN_SC3_DOMAIN = "push.ft07.com"


class _ServerChanAPIError(RuntimeError):
    pass


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _load_env_values() -> dict[str, str]:
    path = PROJECT_ROOT / ".env"
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    try:
        for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            normalized = value.strip()
            if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
                normalized = normalized[1:-1]
            values[key.strip()] = normalized
    except Exception:
        return {}
    return values


def build_serverchan_endpoint(sendkey: str) -> str | None:
    """Resolve only the two endpoint formats documented by ServerChan."""
    normalized = str(sendkey or "").strip()
    if re.fullmatch(r"SCT[0-9A-Za-z_-]+", normalized):
        return f"{SERVERCHAN_TURBO_API}/{normalized}.send"
    match = re.fullmatch(r"sctp(\d+)t[0-9A-Za-z_-]+", normalized)
    if match:
        uid = match.group(1)
        return f"https://{uid}.{SERVERCHAN_SC3_DOMAIN}/send/{normalized}.send"
    return None


@dataclass(frozen=True)
class ServerChanNotifierConfig:
    enabled: bool = False
    sendkey: str = ""
    report_url: str = ""
    timeout_seconds: int = 12
    max_attempts: int = 2
    allow_partial: bool = False

    @property
    def endpoint(self) -> str | None:
        return build_serverchan_endpoint(self.sendkey)

    @property
    def configured(self) -> bool:
        return bool(self.enabled and self.endpoint)


def load_serverchan_notifier_config() -> ServerChanNotifierConfig:
    env_file = _load_env_values()

    def pick(name: str, default: Any = "") -> Any:
        return os.getenv(name) or env_file.get(name) or default

    sendkey = str(
        pick("MORNING_REPORT_SERVERCHAN_SENDKEY")
        or pick("SERVERCHAN_SENDKEY")
        or ""
    ).strip()
    enabled_default = bool(sendkey)
    return ServerChanNotifierConfig(
        enabled=_to_bool(pick("MORNING_REPORT_SERVERCHAN_ENABLED", enabled_default), enabled_default),
        sendkey=sendkey,
        report_url=str(pick("MORNING_REPORT_PUBLIC_URL", "")).strip(),
        timeout_seconds=max(3, _to_int(pick("MORNING_REPORT_SERVERCHAN_TIMEOUT_SECONDS", 12), 12)),
        max_attempts=max(1, min(3, _to_int(pick("MORNING_REPORT_SERVERCHAN_MAX_ATTEMPTS", 2), 2))),
        allow_partial=_to_bool(pick("MORNING_REPORT_SERVERCHAN_ALLOW_PARTIAL", False), False),
    )


def _safe_text(value: Any, max_length: int = 300) -> str:
    text = " ".join(str(value or "").split())
    if len(text) > max_length:
        text = text[: max_length - 1].rstrip() + "…"
    return text


def build_serverchan_message(report: dict, *, report_url: str | None = None) -> tuple[str, str]:
    fact_pack = report.get("fact_pack") or {}
    quality = fact_pack.get("data_quality") or {}
    digest = build_report_digest(fact_pack)
    analysis = ((report.get("llm") or {}).get("analysis") or {})
    focus_items = analysis.get("focus_items") or digest.get("focus_items") or []
    target = _safe_text(fact_pack.get("report_trade_date") or "--", 20)
    status = quality.get("report_status") or "partial"
    status_text = "关键数据已齐" if status == "complete" else "部分数据"
    mode = "证据校验版" if report.get("report_mode") == "llm" else "结构化事实版"
    title = _safe_text(f"ETF晨报 {target} {status_text}", 80).replace("\n", " ")

    lines = [
        f"# ETF 晨报｜{target}",
        "",
        f"> {status_text} · 数据覆盖 {_safe_text(quality.get('coverage_score') or 0, 10)}% · {mode}",
        "",
        "## 今天先看",
        "",
    ]
    if focus_items:
        for item in focus_items[:3]:
            lines.append(f"- {_safe_text(item.get('text') or '--', 240)}")
            if item.get("caveat"):
                lines.append(f"  - 限制：{_safe_text(item.get('caveat'), 160)}")
    else:
        lines.append("- 暂无通过校验的核心结论")
    lines.extend([
        "",
        f"**短线情绪**：{_safe_text(digest.get('risk_color'))}｜{_safe_text(digest.get('risk_text'))}",
        "",
        f"**资金主线**：{_safe_text(digest.get('top_sector'))}",
        "",
        f"**涨停 / 炸板**：{digest.get('limitup_count', 0)} / {digest.get('blowup_count', 0)}",
    ])
    warnings = quality.get("warnings") or []
    if warnings:
        lines.extend(["", "## 数据提示", ""])
        for warning in warnings[:3]:
            lines.append(f"- {_safe_text(warning, 180)}")
    resolved_report_url = str(
        load_serverchan_notifier_config().report_url if report_url is None else report_url
    ).strip()
    if resolved_report_url:
        lines.extend(["", f"[查看完整晨报]({resolved_report_url})"])
    lines.extend(["", "> 数据与模型输出仅供研究参考，不构成投资建议。"])
    return title, "\n".join(lines)[:10000]


def _delivery_key(report: dict, recipient_id: str = "global") -> str:
    target = str((report.get("fact_pack") or {}).get("report_trade_date") or "unknown")
    scope = str(recipient_id or "global")
    return hashlib.sha256(f"serverchan:{scope}:{target}".encode("utf-8")).hexdigest()[:24]


def _delivery_path(report: dict, recipient_id: str = "global") -> Path:
    target = str((report.get("fact_pack") or {}).get("report_trade_date") or "unknown")
    safe_target = "".join(ch for ch in target if ch.isdigit() or ch == "-") or "unknown"
    return NOTIFICATION_DIR / f"serverchan-{safe_target}-{_delivery_key(report, recipient_id)}.json"


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            temporary_path = Path(handle.name)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink(missing_ok=True)


def _safe_error(exc: Exception, config: ServerChanNotifierConfig) -> str:
    message = str(exc)
    for secret in (config.sendkey, config.endpoint or ""):
        if secret:
            message = message.replace(secret, "<redacted-serverchan-secret>")
    return f"{type(exc).__name__}: {message}"[:500]


def _post_serverchan_message(
    config: ServerChanNotifierConfig,
    title: str,
    desp: str,
    *,
    session=None,
) -> dict:
    if not config.configured:
        return {"status": "disabled", "attempt": 0}
    client = session or requests
    last_error = ""
    attempts_used = 0
    for attempt in range(1, config.max_attempts + 1):
        attempts_used = attempt
        try:
            response = client.post(
                config.endpoint,
                json={"title": title, "desp": desp},
                headers={"Content-Type": "application/json;charset=utf-8"},
                timeout=config.timeout_seconds,
            )
            response.raise_for_status()
            parsed = response.json()
            response_payload = parsed if isinstance(parsed, dict) else {}
            if int(response_payload.get("code", -1)) != 0:
                raise _ServerChanAPIError(
                    f"ServerChan code={response_payload.get('code')} message={response_payload.get('message')}"
                )
            return {
                "status": "delivered",
                "attempt": attempt,
                "response": {
                    "code": response_payload.get("code"),
                    "message": response_payload.get("message"),
                },
            }
        except _ServerChanAPIError as exc:
            last_error = _safe_error(exc, config)
            break
        except Exception as exc:
            last_error = _safe_error(exc, config)
    return {"status": "failed", "attempt": attempts_used, "error": last_error}


def send_serverchan_test(sendkey: str, *, session=None) -> dict:
    base = load_serverchan_notifier_config()
    config = ServerChanNotifierConfig(
        enabled=True,
        sendkey=str(sendkey or "").strip(),
        timeout_seconds=base.timeout_seconds,
        max_attempts=base.max_attempts,
    )
    if not config.configured:
        return {"channel": "serverchan", "status": "invalid_sendkey"}
    result = _post_serverchan_message(
        config,
        "ETF晨报 Server酱通道测试",
        "## 通道测试\n\n你的个人 Server酱通知设置连接正常。\n\n> 此消息仅用于验证推送，不是正式投资晨报。",
        session=session,
    )
    return {"channel": "serverchan", **result}


def send_serverchan_report(
    report: dict,
    *,
    config: ServerChanNotifierConfig | None = None,
    force: bool = False,
    recipient_id: str = "global",
    session=None,
) -> dict:
    from datetime import datetime

    resolved = config or load_serverchan_notifier_config()
    target = str((report.get("fact_pack") or {}).get("report_trade_date") or "")
    quality = (report.get("fact_pack") or {}).get("data_quality") or {}
    recipient_scope = str(recipient_id or "global")
    base_result = {
        "channel": "serverchan",
        "report_trade_date": target,
        "recipient": recipient_scope,
    }
    if not resolved.configured:
        return {**base_result, "status": "disabled"}
    if quality.get("report_status") != "complete" and not resolved.allow_partial:
        return {**base_result, "status": "suppressed_partial"}

    delivery_path = _delivery_path(report, recipient_scope)
    if delivery_path.exists() and not force:
        try:
            previous = json.loads(delivery_path.read_text(encoding="utf-8"))
            if previous.get("status") == "delivered":
                return {
                    **base_result,
                    "status": "duplicate_skipped",
                    "delivered_at": previous.get("delivered_at"),
                }
        except Exception:
            pass

    title, desp = build_serverchan_message(report, report_url=resolved.report_url)
    result = _post_serverchan_message(resolved, title, desp, session=session)
    now = datetime.now(BEIJING_TZ).isoformat(timespec="seconds")
    record = {**base_result, **result, "report_hash": report.get("report_hash")}
    if result.get("status") == "delivered":
        record["delivered_at"] = now
    else:
        record["failed_at"] = now
    _atomic_write_json(delivery_path, record)
    return record


def send_serverchan_report_for_users(
    report: dict,
    *,
    force: bool = False,
    session=None,
) -> dict:
    """Deliver once per unique configured recipient without exposing SendKeys."""
    base_config = load_serverchan_notifier_config()
    recipients: list[tuple[str, ServerChanNotifierConfig]] = []
    lookup_error = ""
    try:
        from src.user_notification_store import list_enabled_serverchan_credentials

        user_credentials = list_enabled_serverchan_credentials()
    except Exception as exc:
        user_credentials = []
        lookup_error = f"{type(exc).__name__}: user credential lookup failed"

    seen_secret_hashes: set[str] = set()
    for username, sendkey in user_credentials:
        secret_hash = hashlib.sha256(sendkey.encode("utf-8")).hexdigest()
        if secret_hash in seen_secret_hashes:
            continue
        seen_secret_hashes.add(secret_hash)
        recipients.append((
            f"user:{username}",
            ServerChanNotifierConfig(
                enabled=True,
                sendkey=sendkey,
                report_url=base_config.report_url,
                timeout_seconds=base_config.timeout_seconds,
                max_attempts=base_config.max_attempts,
                allow_partial=base_config.allow_partial,
            ),
        ))

    if base_config.configured:
        secret_hash = hashlib.sha256(base_config.sendkey.encode("utf-8")).hexdigest()
        if secret_hash not in seen_secret_hashes:
            recipients.append(("global", base_config))

    if not recipients:
        if lookup_error:
            return {"channel": "serverchan", "status": "failed", "error": lookup_error, "recipients": []}
        return {"channel": "serverchan", "status": "disabled", "recipients": []}

    results = [
        send_serverchan_report(
            report,
            config=config,
            force=force,
            recipient_id=recipient_id,
            session=session,
        )
        for recipient_id, config in recipients
    ]
    statuses = [str(item.get("status") or "failed") for item in results]
    if "failed" in statuses:
        aggregate_status = "partial_failure" if any(
            status in {"delivered", "duplicate_skipped"} for status in statuses
        ) else "failed"
    elif "delivered" in statuses:
        aggregate_status = "delivered"
    elif statuses and all(status == "duplicate_skipped" for status in statuses):
        aggregate_status = "duplicate_skipped"
    elif statuses and all(status == "suppressed_partial" for status in statuses):
        aggregate_status = "suppressed_partial"
    else:
        aggregate_status = statuses[0] if statuses else "disabled"
    return {
        "channel": "serverchan",
        "status": aggregate_status,
        "recipient_count": len(results),
        "recipients": results,
        **({"warning": lookup_error} if lookup_error else {}),
    }
