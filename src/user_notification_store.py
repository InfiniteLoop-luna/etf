from __future__ import annotations

import base64
import hashlib
import logging
import os
import re
from dataclasses import dataclass

from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.etf_morning_report import PROJECT_ROOT
from src.morning_report_notifier import build_serverchan_endpoint
from src.sync_tushare_security_data import build_db_url


logger = logging.getLogger(__name__)
TABLE_NAME = "app_user_notification_credentials"
SERVERCHAN_CHANNEL = "serverchan"
ENCRYPTION_ENV_NAMES = (
    "USER_SECRET_ENCRYPTION_KEY",
    "MORNING_REPORT_CREDENTIAL_KEY",
)


@dataclass(frozen=True)
class UserNotificationCredential:
    username: str
    channel: str
    enabled: bool
    key_hint: str
    updated_at: object | None = None


def normalize_username(username: str) -> str:
    normalized = re.sub(r"\s+", " ", str(username or "").strip())
    return normalized[:64]


def get_engine() -> Engine:
    return create_engine(build_db_url(), pool_pre_ping=True)


def ensure_user_notification_table(engine: Engine) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        username VARCHAR(64) NOT NULL,
        channel VARCHAR(32) NOT NULL,
        encrypted_secret TEXT NOT NULL,
        key_hint VARCHAR(32) NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (username, channel)
    );

    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_channel_enabled
        ON {TABLE_NAME} (channel, enabled, updated_at DESC);
    """
    with engine.begin() as conn:
        for statement in [item.strip() for item in sql.split(";") if item.strip()]:
            conn.execute(text(statement))


def _load_encryption_material() -> str:
    env_file_values: dict[str, str] = {}
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        try:
            for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                normalized = value.strip()
                if len(normalized) >= 2 and normalized[0] == normalized[-1] and normalized[0] in {'"', "'"}:
                    normalized = normalized[1:-1]
                env_file_values[key.strip()] = normalized
        except Exception:
            env_file_values = {}
    for name in ENCRYPTION_ENV_NAMES:
        value = str(os.getenv(name) or env_file_values.get(name) or "").strip()
        if value:
            if len(value) < 32:
                raise RuntimeError(f"{name} must contain at least 32 characters")
            return value
    raise RuntimeError("User notification credential encryption is not configured")


def notification_encryption_configured() -> bool:
    try:
        _load_encryption_material()
        return True
    except RuntimeError:
        return False


def _get_cipher() -> Fernet:
    material = _load_encryption_material().encode("utf-8")
    derived_key = base64.urlsafe_b64encode(hashlib.sha256(material).digest())
    return Fernet(derived_key)


def _encrypt_secret(secret: str) -> str:
    token = _get_cipher().encrypt(str(secret).encode("utf-8")).decode("ascii")
    return f"fernet:v1:{token}"


def _decrypt_secret(payload: str) -> str:
    normalized = str(payload or "")
    if not normalized.startswith("fernet:v1:"):
        raise InvalidToken("Unsupported encrypted credential format")
    return _get_cipher().decrypt(normalized.removeprefix("fernet:v1:").encode("ascii")).decode("utf-8")


def _build_key_hint(sendkey: str) -> str:
    normalized = str(sendkey or "").strip()
    prefix = "sctp" if normalized.startswith("sctp") else "SCT"
    return f"{prefix}…{normalized[-4:]}"


def get_user_serverchan_credential(
    username: str,
    engine: Engine | None = None,
) -> UserNotificationCredential | None:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return None
    actual_engine = engine or get_engine()
    ensure_user_notification_table(actual_engine)
    sql = f"""
    SELECT username, channel, enabled, key_hint, updated_at
    FROM {TABLE_NAME}
    WHERE username = :username AND channel = :channel
    LIMIT 1
    """
    with actual_engine.begin() as conn:
        row = conn.execute(
            text(sql),
            {"username": normalized_username, "channel": SERVERCHAN_CHANNEL},
        ).mappings().first()
    if row is None:
        return None
    return UserNotificationCredential(
        username=str(row["username"]),
        channel=str(row["channel"]),
        enabled=bool(row["enabled"]),
        key_hint=str(row["key_hint"]),
        updated_at=row["updated_at"],
    )


def save_user_serverchan_sendkey(
    username: str,
    sendkey: str,
    *,
    enabled: bool = True,
    engine: Engine | None = None,
) -> bool:
    normalized_username = normalize_username(username)
    normalized_sendkey = str(sendkey or "").strip()
    if not normalized_username:
        raise ValueError("username 不能为空")
    if not build_serverchan_endpoint(normalized_sendkey):
        raise ValueError("SendKey 格式无效，应以 SCT 或 sctp 开头")
    encrypted_secret = _encrypt_secret(normalized_sendkey)
    actual_engine = engine or get_engine()
    ensure_user_notification_table(actual_engine)
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        username, channel, encrypted_secret, key_hint, enabled, created_at, updated_at
    )
    VALUES (
        :username, :channel, :encrypted_secret, :key_hint, :enabled,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (username, channel)
    DO UPDATE SET
        encrypted_secret = EXCLUDED.encrypted_secret,
        key_hint = EXCLUDED.key_hint,
        enabled = EXCLUDED.enabled,
        updated_at = CURRENT_TIMESTAMP
    """
    with actual_engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "channel": SERVERCHAN_CHANNEL,
                "encrypted_secret": encrypted_secret,
                "key_hint": _build_key_hint(normalized_sendkey),
                "enabled": bool(enabled),
            },
        )
    return True


def set_user_serverchan_enabled(
    username: str,
    enabled: bool,
    *,
    engine: Engine | None = None,
) -> bool:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return False
    actual_engine = engine or get_engine()
    ensure_user_notification_table(actual_engine)
    sql = f"""
    UPDATE {TABLE_NAME}
    SET enabled = :enabled, updated_at = CURRENT_TIMESTAMP
    WHERE username = :username AND channel = :channel
    """
    with actual_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {
                "username": normalized_username,
                "channel": SERVERCHAN_CHANNEL,
                "enabled": bool(enabled),
            },
        )
    return int(result.rowcount or 0) > 0


def get_user_serverchan_sendkey(
    username: str,
    *,
    require_enabled: bool = True,
    engine: Engine | None = None,
) -> str | None:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return None
    actual_engine = engine or get_engine()
    ensure_user_notification_table(actual_engine)
    enabled_clause = "AND enabled = TRUE" if require_enabled else ""
    sql = f"""
    SELECT encrypted_secret
    FROM {TABLE_NAME}
    WHERE username = :username AND channel = :channel {enabled_clause}
    LIMIT 1
    """
    with actual_engine.begin() as conn:
        row = conn.execute(
            text(sql),
            {"username": normalized_username, "channel": SERVERCHAN_CHANNEL},
        ).first()
    if row is None:
        return None
    return _decrypt_secret(str(row[0]))


def list_enabled_serverchan_credentials(
    engine: Engine | None = None,
) -> list[tuple[str, str]]:
    actual_engine = engine or get_engine()
    ensure_user_notification_table(actual_engine)
    sql = f"""
    SELECT username, encrypted_secret
    FROM {TABLE_NAME}
    WHERE channel = :channel AND enabled = TRUE
    ORDER BY username ASC
    """
    with actual_engine.begin() as conn:
        rows = conn.execute(text(sql), {"channel": SERVERCHAN_CHANNEL}).all()
    if rows and not notification_encryption_configured():
        raise RuntimeError("User notification credential encryption is not configured")
    credentials: list[tuple[str, str]] = []
    failed_count = 0
    for row in rows:
        try:
            credentials.append((str(row[0]), _decrypt_secret(str(row[1]))))
        except Exception as exc:
            failed_count += 1
            logger.warning("Failed to decrypt ServerChan credential for user %s: %s", row[0], type(exc).__name__)
    if failed_count and not credentials:
        raise RuntimeError("No stored ServerChan credential could be decrypted")
    return credentials


def delete_user_serverchan_credential(
    username: str,
    *,
    engine: Engine | None = None,
) -> int:
    normalized_username = normalize_username(username)
    if not normalized_username:
        return 0
    actual_engine = engine or get_engine()
    ensure_user_notification_table(actual_engine)
    sql = f"DELETE FROM {TABLE_NAME} WHERE username = :username AND channel = :channel"
    with actual_engine.begin() as conn:
        result = conn.execute(
            text(sql),
            {"username": normalized_username, "channel": SERVERCHAN_CHANNEL},
        )
    return int(result.rowcount or 0)
