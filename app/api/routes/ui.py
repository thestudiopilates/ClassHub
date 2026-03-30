import copy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.demo_data import build_demo_payload, _session_time_label
from app.services.automation import trigger_auto_warm_if_needed


router = APIRouter()

STATIC_ROOT = Path(__file__).resolve().parents[2] / "static" / "demo"

# ── Cache ────────────────────────────────────────────────────────────────
# The full payload (all clients, all sessions for the day) is built ONCE
# and cached until a sync invalidates it or the day rolls over.
# On each request, only a cheap time-filter is applied to show the current
# class + next 2 upcoming classes. No DB queries on refresh.
_demo_cache: dict[str, Any] = {"payload": None, "day": None, "built_at": 0.0}

# Maximum number of sessions to show: current in-progress + next upcoming
_MAX_VISIBLE_SESSIONS = 3
# How long after a class starts before it's hidden (minutes)
_SESSION_VISIBLE_MINUTES = 60


def invalidate_demo_cache() -> None:
    """Call after a sync completes to force a fresh build on next request."""
    _demo_cache["payload"] = None
    _demo_cache["day"] = None


def _filter_sessions_by_time(sessions: list[dict], now: datetime) -> list[dict]:
    """Cheap time filter: drop ended sessions, keep Now + next upcoming."""
    visible = []
    for session in sessions:
        starts_utc_str = session.get("startsAtUtc")
        if not starts_utc_str:
            visible.append(session)
            continue
        try:
            starts_utc = datetime.fromisoformat(starts_utc_str)
            if starts_utc.tzinfo is None:
                starts_utc = starts_utc.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            visible.append(session)
            continue
        session_end = starts_utc + timedelta(minutes=_SESSION_VISIBLE_MINUTES)
        if session_end < now:
            continue  # ended — hide
        visible.append(session)

    # Sort: in-progress first, then by start time
    visible.sort(key=lambda s: s.get("startsAtUtc") or "")

    # Limit to current + next few
    return visible[:_MAX_VISIBLE_SESSIONS]


def _apply_live_time_labels(sessions: list[dict]) -> list[dict]:
    """Re-compute the time labels (Now/Up next) based on current time."""
    result = []
    for session in sessions:
        s = copy.copy(session)
        starts_utc_str = s.get("startsAtUtc")
        if starts_utc_str:
            try:
                starts_utc = datetime.fromisoformat(starts_utc_str)
                if starts_utc.tzinfo is None:
                    starts_utc = starts_utc.replace(tzinfo=timezone.utc)
                s["time"] = _session_time_label(starts_utc)
            except (ValueError, TypeError):
                pass
        result.append(s)
    return result


@router.get("/demo")
def demo_ui() -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")


@router.get("/demo/data")
def demo_data(day: Optional[date] = None, db: Session = Depends(get_db)) -> dict:
    trigger_auto_warm_if_needed(day)

    today = day or date.today()
    now = datetime.now(timezone.utc)

    # Build the full payload once, cache it
    if _demo_cache["payload"] is None or _demo_cache["day"] != today:
        payload = build_demo_payload(db, day)
        _demo_cache["payload"] = payload
        _demo_cache["day"] = today
        _demo_cache["built_at"] = now.timestamp()

    cached = _demo_cache["payload"]

    # Cheap serve-time operations: filter sessions by time, refresh labels
    filtered_sessions = _filter_sessions_by_time(cached.get("sessions", []), now)
    live_sessions = _apply_live_time_labels(filtered_sessions)

    # Return cached payload with live-filtered sessions
    return {**cached, "sessions": live_sessions}
