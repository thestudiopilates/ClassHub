import copy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.demo_data import (
    build_client_profiles_cache,
    build_live_roster,
    _session_time_label,
)
from app.services.automation import trigger_auto_warm_if_needed


router = APIRouter()

STATIC_ROOT = Path(__file__).resolve().parents[2] / "static" / "demo"

# ── Full dashboard cache ──────────────────────────────────────────────────
# The entire dashboard payload is built ONCE and cached in memory.
# On each request, only the cheap time-filter + labels are re-applied.
# Invalidated by: sync completion, day rollover.
_dashboard: dict[str, Any] = {"payload": None, "day": None, "built_at": 0.0}

# How long a class lasts (minutes) — used to determine when a class has ended
_CLASS_DURATION_MINUTES = 55


def invalidate_demo_cache() -> None:
    """Call after a sync completes to force a fresh build on next request."""
    _dashboard["payload"] = None
    _dashboard["day"] = None


def _filter_sessions_by_time(sessions: list[dict], now: datetime) -> list[dict]:
    """Show current + all remaining sessions for the day. Hide ended classes."""
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
        # A class is "ended" when its duration has passed
        session_end = starts_utc + timedelta(minutes=_CLASS_DURATION_MINUTES)
        if session_end < now:
            continue  # ended — hide
        visible.append(session)

    # Sort: in-progress first, then by start time
    visible.sort(key=lambda s: s.get("startsAtUtc") or "")
    return visible


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


def _build_full_dashboard(db: Session, day: Optional[date]) -> dict[str, Any]:
    """Build the complete dashboard payload. Called once, then cached."""
    profiles = build_client_profiles_cache(db, day)
    today = day or date.today()
    live = build_live_roster(db, today, profiles)
    return {
        "meta": live["meta"],
        "summary": live["summary"],
        "freshness": live["freshness"],
        "celebrations": profiles["celebrations"],
        "people": profiles["people"],
        "frontdesk": live["frontdesk"],
        "sessions": live["sessions"],
    }


@router.get("/demo")
def demo_ui() -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")


@router.get("/demo/data")
def demo_data(day: Optional[date] = None, db: Session = Depends(get_db)) -> dict:
    trigger_auto_warm_if_needed(day)

    today = day or date.today()
    now = datetime.now(timezone.utc)

    # Build full dashboard once, cache until sync invalidates or day rolls
    if _dashboard["payload"] is None or _dashboard["day"] != today:
        _dashboard["payload"] = _build_full_dashboard(db, day)
        _dashboard["day"] = today
        _dashboard["built_at"] = now.timestamp()

    cached = _dashboard["payload"]

    # Only serve-time ops: filter sessions by time, refresh labels
    filtered_sessions = _filter_sessions_by_time(cached.get("sessions", []), now)
    live_sessions = _apply_live_time_labels(filtered_sessions)

    # Only send profiles for clients visible on screen (roster + frontdesk)
    needed_ids: set[str] = set()
    for session in live_sessions:
        for entry in session.get("roster", []):
            if pid := entry.get("personId"):
                needed_ids.add(pid)
    for item in cached.get("frontdesk", []):
        if pid := item.get("id"):
            needed_ids.add(pid)
    visible_people = {pid: cached["people"][pid] for pid in needed_ids if pid in cached["people"]}

    return {
        "meta": cached["meta"],
        "summary": cached["summary"],
        "freshness": cached["freshness"],
        "celebrations": cached["celebrations"],
        "people": visible_people,
        "frontdesk": cached["frontdesk"],
        "sessions": live_sessions,
    }
