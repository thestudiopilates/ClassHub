from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import SyncState

SYNC_STALE_HOURS = {
    "active_customers": 24,
    "birthdays": 24,
    "customer_fields": 24,
    "behavior": 24,
    "booking_history": 24,
    "memberships_notes": 4,
    "bookings": 2,
    "flags": 6,
}


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def record_sync_state(
    db: Session,
    domain: str,
    *,
    status: str,
    records_processed: int = 0,
    error_text: str | None = None,
    synced_at: datetime | None = None,
) -> SyncState:
    now = synced_at or datetime.now(timezone.utc)
    state = db.get(SyncState, domain)
    if state is None:
        state = SyncState(domain=domain)
    state.last_synced_at = now
    state.status = status
    state.records_processed = records_processed
    state.error_text = error_text
    if status == "completed":
        state.last_successful_at = now
    db.add(state)
    return state


def get_freshness_map(db: Session, now: datetime | None = None) -> Dict[str, dict]:
    current = now or datetime.now(timezone.utc)
    states = {state.domain: state for state in db.scalars(select(SyncState)).all()}
    freshness: Dict[str, dict] = {}
    for domain, stale_after_hours in SYNC_STALE_HOURS.items():
        state = states.get(domain)
        last_successful_at = _as_utc(state.last_successful_at) if state else None
        is_stale = True
        if last_successful_at is not None:
            is_stale = current - last_successful_at > timedelta(hours=stale_after_hours)
        freshness[domain] = {
            "domain": domain,
            "last_synced_at": _as_utc(state.last_synced_at) if state else None,
            "last_successful_at": last_successful_at,
            "status": state.status if state else "unknown",
            "records_processed": state.records_processed if state else 0,
            "error_text": state.error_text if state else None,
            "stale_after_hours": stale_after_hours,
            "is_stale": is_stale,
        }
    return freshness
