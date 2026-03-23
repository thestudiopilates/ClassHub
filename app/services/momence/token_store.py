from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.core.config import settings


def _token_path() -> Path:
    return Path(settings.momence_token_store_path).expanduser()


def load_tokens() -> dict[str, Any] | None:
    path = _token_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_tokens(payload: dict[str, Any]) -> dict[str, Any]:
    path = _token_path()
    normalized = dict(payload)
    access_token = normalized.get("access_token") or normalized.get("accessToken")
    refresh_token = normalized.get("refresh_token") or normalized.get("refreshToken")
    expires_in = normalized.get("expires_in") or normalized.get("expiresIn")
    expires_at_raw = (
        normalized.get("expires_at")
        or normalized.get("expiresAt")
        or normalized.get("accessTokenExpiresAt")
    )
    token_type = normalized.get("token_type") or normalized.get("tokenType") or "Bearer"

    normalized["access_token"] = access_token
    normalized["refresh_token"] = refresh_token
    normalized["token_type"] = token_type

    if expires_in:
        try:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=int(expires_in))
            normalized["expires_at"] = expires_at.isoformat()
        except Exception:
            pass
    elif expires_at_raw:
        normalized["expires_at"] = expires_at_raw

    path.write_text(json.dumps(normalized, indent=2), encoding="utf-8")
    return normalized


def clear_tokens() -> None:
    path = _token_path()
    if path.exists():
        path.unlink()


def access_token_is_fresh(tokens: dict[str, Any] | None) -> bool:
    if not tokens:
        return False
    access_token = tokens.get("access_token")
    expires_at = tokens.get("expires_at") or tokens.get("expiresAt") or tokens.get("accessTokenExpiresAt")
    if not access_token:
        return False
    if not expires_at:
        return True
    try:
        expiry = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
    except Exception:
        return False
    if expiry.tzinfo is None:
        expiry = expiry.replace(tzinfo=timezone.utc)
    return expiry - datetime.now(timezone.utc) > timedelta(minutes=2)
