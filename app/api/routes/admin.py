from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.momence.client import MomenceClient
from app.services.automation import run_preopen_ops_sync
from app.schemas import (
    BookingHistoryProgressResponse,
    BookingHistoryRunRequest,
    MomenceTokenImportRequest,
    RosterHistoryRunRequest,
    SyncRunResponse,
    TargetedRefreshRequest,
)
from app.services.momence.token_store import save_tokens
from app.services.sync.jobs import (
    get_booking_history_progress,
    refresh_clients_by_member_ids,
    refresh_client_by_member_id,
    sync_bookings_for_day,
    sync_active_customers_from_browser,
    sync_client_behavior_from_reports,
    sync_birthdays_from_browser,
    sync_browser_seed_data,
    sync_customer_fields_from_browser,
    sync_recent_booking_history_chunks,
    sync_recent_booking_history,
    sync_roster_client_history,
    sync_upcoming_bookings,
)

router = APIRouter()


@router.post("/sync/preopen")
def run_preopen_sync(db: Session = Depends(get_db)) -> dict[str, SyncRunResponse]:
    return run_preopen_ops_sync(db)


@router.post("/sync/upcoming-bookings", response_model=SyncRunResponse)
def run_upcoming_bookings_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_upcoming_bookings(db)


@router.post("/sync/bookings/day", response_model=SyncRunResponse)
def run_bookings_for_day_sync(day: date | None = Query(default=None), db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_bookings_for_day(db, day or date.today())


@router.post("/sync/booking-history", response_model=SyncRunResponse)
def run_recent_booking_history_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_recent_booking_history(db)


@router.post("/sync/booking-history/chunks", response_model=SyncRunResponse)
def run_recent_booking_history_chunks(
    request: BookingHistoryRunRequest, db: Session = Depends(get_db)
) -> SyncRunResponse:
    return sync_recent_booking_history_chunks(db, max_chunks=request.max_chunks)


@router.get("/sync/booking-history/progress", response_model=BookingHistoryProgressResponse)
def get_recent_booking_history_progress(db: Session = Depends(get_db)) -> BookingHistoryProgressResponse:
    return get_booking_history_progress(db)


@router.post("/sync/roster-history", response_model=SyncRunResponse)
def run_roster_history_sync(
    request: RosterHistoryRunRequest, db: Session = Depends(get_db)
) -> SyncRunResponse:
    return sync_roster_client_history(
        db,
        day=request.day,
        max_clients=request.max_clients,
        offset=request.offset,
    )


@router.post("/sync/client/{momence_member_id}", response_model=SyncRunResponse)
def run_single_client_refresh(
    momence_member_id: str, db: Session = Depends(get_db)
) -> SyncRunResponse:
    return refresh_client_by_member_id(db, momence_member_id)


@router.post("/sync/clients/context", response_model=SyncRunResponse)
def run_targeted_client_context_refresh(
    request: TargetedRefreshRequest, db: Session = Depends(get_db)
) -> SyncRunResponse:
    return refresh_clients_by_member_ids(db, request.member_ids)


@router.post("/sync/browser/customers", response_model=SyncRunResponse)
def run_browser_customer_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_active_customers_from_browser(db)


@router.post("/sync/browser/birthdays", response_model=SyncRunResponse)
def run_browser_birthdays_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_birthdays_from_browser(db)


@router.post("/sync/browser/customer-fields", response_model=SyncRunResponse)
def run_browser_customer_fields_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_customer_fields_from_browser(db)


@router.post("/sync/browser/seed", response_model=SyncRunResponse)
def run_browser_seed_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_browser_seed_data(db)


@router.post("/sync/browser/behavior", response_model=SyncRunResponse)
def run_browser_behavior_sync(db: Session = Depends(get_db)) -> SyncRunResponse:
    return sync_client_behavior_from_reports(db)


@router.get("/debug/momence/profile")
async def debug_momence_profile() -> dict:
    client = MomenceClient()
    return await client.fetch_auth_profile()


@router.get("/debug/momence/sessions")
async def debug_momence_sessions(
    day: date | None = Query(default=None),
    days: int = Query(default=7, ge=1, le=14),
) -> dict:
    client = MomenceClient()
    start = day or date.today()
    end = start + timedelta(days=days)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        **(await client.debug_session_window(start, end)),
    }


@router.post("/debug/momence/import-tokens")
def debug_import_momence_tokens(request: MomenceTokenImportRequest) -> dict:
    tokens = save_tokens(dict(request.payload))
    return {
        "connected": bool(tokens.get("access_token")),
        "has_refresh_token": bool(tokens.get("refresh_token")),
        "expires_at": tokens.get("expires_at"),
    }
