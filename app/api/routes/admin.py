from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models import Client, ClientActivity, ClientNote, ClientPreference, ClientProfileData
from app.services.momence.client import MomenceClient
from app.services.automation import run_preopen_ops_sync
from app.schemas import (
    BookingHistoryProgressResponse,
    BookingHistoryRunRequest,
    MomenceTokenImportRequest,
    SeedImportRequest,
    SeedImportResponse,
    RosterHistoryRunRequest,
    SyncRunResponse,
    TargetedRefreshRequest,
)
from app.services.momence.token_store import save_tokens
from app.services.domain import refresh_all_flags
from app.services.sync_state import record_sync_state
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


@router.post("/seed/import-batch", response_model=SeedImportResponse)
def import_seed_batch(request: SeedImportRequest, db: Session = Depends(get_db)) -> SeedImportResponse:
    imported_clients = 0
    imported_notes = 0
    imported_profile_rows = 0
    imported_preference_rows = 0
    imported_birthdays = 0
    for item in request.clients:
        client = db.query(Client).filter(Client.momence_member_id == item.member_id).one_or_none()
        if client is None:
            client = Client(momence_member_id=item.member_id)
        client.first_name = item.first_name
        client.last_name = item.last_name
        client.full_name = item.full_name or " ".join(part for part in [item.first_name, item.last_name] if part).strip()
        client.email = item.email
        client.phone = item.phone
        client.birthday = item.birthday
        client.source_updated_at = item.source_updated_at
        db.add(client)
        db.flush()
        if item.birthday is not None:
            imported_birthdays += 1

        if item.activity is not None:
            activity = client.activity or ClientActivity(client_id=client.id)
            activity.last_checkin_at = item.activity.last_checkin_at
            activity.last_booking_at = item.activity.last_booking_at
            activity.next_booking_at = item.activity.next_booking_at
            activity.first_visit_at = item.activity.first_visit_at
            activity.total_visits = item.activity.total_visits
            activity.lifetime_visits_baseline = item.activity.lifetime_visits_baseline
            activity.lifetime_visits_increment = item.activity.lifetime_visits_increment
            activity.lifetime_visits_baseline_as_of = item.activity.lifetime_visits_baseline_as_of
            activity.visits_last_30d = item.activity.visits_last_30d
            activity.visits_previous_30d = item.activity.visits_previous_30d
            activity.has_active_membership = item.activity.has_active_membership
            activity.active_membership_name = item.activity.active_membership_name
            db.add(activity)

        if item.profile_data is not None:
            profile = client.profile_data or ClientProfileData(client_id=client.id)
            profile.fun_fact = item.profile_data.fun_fact
            profile.pregnant_status = item.profile_data.pregnant_status
            profile.pregnancy_due_date = item.profile_data.pregnancy_due_date
            profile.heard_about_us = item.profile_data.heard_about_us
            db.add(profile)
            imported_profile_rows += 1

        if item.preferences is not None:
            pref = client.preferences or ClientPreference(client_id=client.id)
            pref.favorite_time_of_day = item.preferences.favorite_time_of_day
            pref.favorite_weekdays = item.preferences.favorite_weekdays
            pref.favorite_instructors = item.preferences.favorite_instructors
            pref.favorite_formats = item.preferences.favorite_formats
            pref.preference_basis = item.preferences.preference_basis
            db.add(pref)
            imported_preference_rows += 1

        if item.notes:
            db.query(ClientNote).filter(ClientNote.client_id == client.id).delete()
            for note in item.notes:
                db.add(
                    ClientNote(
                        client_id=client.id,
                        note_type=note.type,
                        note_text=note.text,
                        is_injury_flag=note.is_injury_flag,
                        is_front_desk_flag=note.is_front_desk_flag,
                        is_instructor_flag=note.is_instructor_flag,
                        source_updated_at=note.source_updated_at,
                    )
                )
                imported_notes += 1

        imported_clients += 1

    db.commit()
    record_sync_state(db, "active_customers", status="completed", records_processed=imported_clients)
    if imported_birthdays:
        record_sync_state(db, "birthdays", status="completed", records_processed=imported_birthdays)
    if imported_profile_rows:
        record_sync_state(db, "customer_fields", status="completed", records_processed=imported_profile_rows)
    if imported_preference_rows:
        record_sync_state(db, "behavior", status="completed", records_processed=imported_preference_rows)
    if imported_notes:
        record_sync_state(db, "memberships_notes", status="completed", records_processed=imported_notes)
    recomputed = False
    if request.recompute_flags:
        refresh_all_flags(db)
        record_sync_state(db, "flags", status="completed", records_processed=imported_clients)
        recomputed = True
    db.commit()
    return SeedImportResponse(
        imported_clients=imported_clients,
        imported_notes=imported_notes,
        recomputed_flags=recomputed,
    )
