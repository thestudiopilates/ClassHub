from __future__ import annotations

import threading
import time
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import Booking, Client, SyncState
from app.db.session import SessionLocal
from app.schemas import SyncRunResponse
from app.services.sync.jobs import sync_roster_client_history, sync_upcoming_bookings

_AUTO_WARM_LOCK = threading.Lock()
_AUTO_WARM_IN_PROGRESS = False


def _local_day_bounds(day: date) -> tuple[datetime, datetime]:
    from zoneinfo import ZoneInfo

    local_tz = ZoneInfo(settings.default_timezone)
    start_local = datetime.combine(day, datetime.min.time(), tzinfo=local_tz)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def get_roster_client_count(db: Session, day: date) -> int:
    start_dt, end_dt = _local_day_bounds(day)
    return db.scalar(
        select(func.count(func.distinct(Booking.client_id))).where(
            Booking.starts_at >= start_dt,
            Booking.starts_at < end_dt,
        )
    ) or 0


def roster_history_is_fresh_for_day(db: Session, day: date) -> bool:
    state = db.get(SyncState, "roster_history")
    if state is None or state.last_successful_at is None:
        return False
    start_dt, _ = _local_day_bounds(day)
    last_successful = state.last_successful_at
    if last_successful.tzinfo is None:
        last_successful = last_successful.replace(tzinfo=timezone.utc)
    else:
        last_successful = last_successful.astimezone(timezone.utc)
    return last_successful >= start_dt


def sync_roster_client_history_full(
    db: Session,
    *,
    day: date | None = None,
    batch_size: int | None = None,
    start_offset: int = 0,
    max_batches: int | None = None,
) -> SyncRunResponse:
    target_day = day or datetime.now().date()
    total_clients = get_roster_client_count(db, target_day)
    offset = max(start_offset, 0)
    processed_total = 0
    batches_run = 0
    last_result: SyncRunResponse | None = None
    current_batch_size = max(1, batch_size or settings.ops_roster_history_batch_size)

    while offset < total_clients:
        result = sync_roster_client_history(
            db,
            day=target_day,
            max_clients=current_batch_size,
            offset=offset,
        )
        last_result = result
        processed_total += result.records_processed
        batches_run += 1
        if result.status != "completed":
            return result
        offset += current_batch_size
        if max_batches is not None and batches_run >= max_batches:
            break
        if offset < total_clients and settings.ops_roster_history_pause_seconds > 0:
            time.sleep(settings.ops_roster_history_pause_seconds)

    if last_result is None:
        return SyncRunResponse(
            job_name="sync_roster_client_history_full",
            status="completed",
            records_processed=0,
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            error_text=None,
        )

    return SyncRunResponse(
        job_name="sync_roster_client_history_full",
        status="completed",
        records_processed=processed_total,
        started_at=last_result.started_at,
        finished_at=datetime.now(timezone.utc),
        error_text=None,
    )


def run_preopen_ops_sync(db: Session, *, day: date | None = None) -> dict[str, SyncRunResponse]:
    target_day = day or datetime.now().date()
    bookings = sync_upcoming_bookings(db)
    roster = sync_roster_client_history_full(db, day=target_day)
    return {"bookings": bookings, "roster_history": roster}


def _auto_warm_worker(target_day: date, max_batches: int | None) -> None:
    global _AUTO_WARM_IN_PROGRESS
    db = SessionLocal()
    try:
        if not roster_history_is_fresh_for_day(db, target_day):
            sync_upcoming_bookings(db)
            sync_roster_client_history_full(db, day=target_day, max_batches=max_batches)
    finally:
        db.close()
        with _AUTO_WARM_LOCK:
            _AUTO_WARM_IN_PROGRESS = False


def trigger_auto_warm_if_needed(day: date | None = None) -> bool:
    global _AUTO_WARM_IN_PROGRESS
    if not settings.ops_auto_warm_enabled:
        return False

    target_day = day or (datetime.now().date() + timedelta(days=settings.ops_auto_warm_day_offset))
    db = SessionLocal()
    try:
        if roster_history_is_fresh_for_day(db, target_day):
            return False
    finally:
        db.close()

    with _AUTO_WARM_LOCK:
        if _AUTO_WARM_IN_PROGRESS:
            return False
        _AUTO_WARM_IN_PROGRESS = True

    thread = threading.Thread(
        target=_auto_warm_worker,
        args=(target_day, settings.ops_auto_warm_max_batches),
        daemon=True,
        name="ops-auto-warm",
    )
    thread.start()
    return True
