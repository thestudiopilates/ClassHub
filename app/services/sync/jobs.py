from __future__ import annotations

import asyncio
import csv
from datetime import date, datetime, timedelta, timezone
from collections import Counter, defaultdict
import re
import httpx
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import Booking, Client, ClientActivity, ClientMembership, ClientNote, ClientPreference, ClientProfileData, SyncRun, SyncState
from app.schemas import BookingHistoryProgressResponse, SyncRunResponse
from app.services.domain import refresh_all_flags
from app.services.momence.browser import MomenceBrowserClient
from app.services.momence.client import MomenceClient
from app.services.sync_state import record_sync_state

LOCAL_TZ = ZoneInfo(settings.default_timezone)

INJURY_PATTERNS = (
    r"\binjur(?:y|ed)\b",
    r"\bsurger(?:y|ies)\b",
    r"\bpain\b",
    r"\bshoulder\b",
    r"\bknee\b",
    r"\bback\b",
    r"\bneck\b",
    r"\bhip\b",
    r"\bankle\b",
    r"\bwrist\b",
    r"\bspine\b",
    r"\bhernia\b",
    r"\bpregnan\w*\b",
    r"\bprenatal\b",
    r"\bpostpartum\b",
    r"\bmodif(?:y|ication|ications)\b",
    r"\blimited mobility\b",
)


def _upsert_customer(db: Session, customer: dict) -> None:
    member_id = str(customer["memberId"])
    client = db.query(Client).filter(Client.momence_member_id == member_id).one_or_none()
    if client is None:
        client = Client(momence_member_id=member_id)
    client.first_name = customer.get("firstName")
    client.last_name = customer.get("lastName")
    client.full_name = " ".join(filter(None, [client.first_name, client.last_name]))
    client.email = customer.get("email")
    client.phone = customer.get("phoneNumber")
    client.source_updated_at = datetime.now(timezone.utc)
    db.add(client)
    db.flush()

    activity = db.query(ClientActivity).filter(ClientActivity.client_id == client.id).one_or_none()
    if activity is None:
        activity = ClientActivity(client_id=client.id)
    activity.first_visit_at = _parse_datetime(customer.get("firstSeen"))
    activity.last_checkin_at = _parse_datetime(customer.get("lastSeen"))
    activity.last_booking_at = activity.last_checkin_at
    activity.activity_updated_at = datetime.now(timezone.utc)
    db.add(activity)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_date(value: str | None):
    if not value:
        return None
    for pattern in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.000Z", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            continue
    return None


def _local_day_bounds(day: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(day, datetime.min.time(), tzinfo=LOCAL_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _normalized(row: dict[str, str]) -> dict[str, str]:
    return {key.strip().lower(): (value or "").strip() for key, value in row.items()}


def _first_matching_key(row: dict[str, str], options: list[str]) -> str | None:
    normalized_keys = list(row.keys())
    for option in options:
        if option in normalized_keys:
            return option
    for key in normalized_keys:
        for option in options:
            if option in key:
                return key
    return None


def _start_run(db: Session, job_name: str) -> SyncRun:
    run = SyncRun(job_name=job_name, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def _finish_run(db: Session, run: SyncRun, status: str, records_processed: int, error_text: str | None = None) -> SyncRunResponse:
    run.status = status
    run.records_processed = records_processed
    run.error_text = error_text
    run.finished_at = datetime.now(timezone.utc)
    db.add(run)
    db.commit()
    db.refresh(run)
    return SyncRunResponse(
        job_name=run.job_name,
        status=run.status,
        records_processed=run.records_processed,
        started_at=run.started_at,
        finished_at=run.finished_at,
        error_text=run.error_text,
    )


def _is_injury_note(note_text: str) -> bool:
    lowered = note_text.lower()
    return any(re.search(pattern, lowered) for pattern in INJURY_PATTERNS)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_subscription_active(item: dict, now: datetime) -> bool:
    if item.get("isVoided"):
        return False
    end_date = _parse_iso_datetime(item.get("endDate"))
    if end_date and end_date < now:
        return False
    return True


def _is_credit_active(item: dict, now: datetime) -> bool:
    if item.get("isVoided"):
        return False
    end_date = _parse_iso_datetime(item.get("endDate"))
    if end_date and end_date < now:
        return False
    classes_left = item.get("classesLeft")
    money_left = item.get("moneyLeft")
    try:
        if classes_left is not None and float(classes_left) > 0:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if money_left is not None and float(money_left) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def _membership_collections(memberships: dict | list[dict]) -> list[tuple[str, list[dict]]]:
    if isinstance(memberships, list):
        grouped: dict[str, list[dict]] = defaultdict(list)
        for item in memberships:
            grouped[item.get("type") or "unknown"].append(item)
        return list(grouped.items())

    return [
        ("subscription", memberships.get("subscriptions") or []),
        ("credit", memberships.get("creditsAndEvents") or []),
        ("subscription", memberships.get("netflixSubscriptions") or []),
    ]


def _membership_name(item: dict) -> str | None:
    membership = item.get("membership") or {}
    return item.get("membershipName") or membership.get("name")


def _select_active_membership_name(memberships: dict | list[dict], now: datetime) -> tuple[bool, str | None]:
    for membership_type, collection in _membership_collections(memberships):
        checker = _is_subscription_active if membership_type == "subscription" else _is_credit_active
        active_items = [item for item in collection if checker(item, now)]
        active_items.sort(key=lambda item: _parse_iso_datetime(item.get("endDate")) or datetime.max.replace(tzinfo=timezone.utc))
        if active_items:
            return True, _membership_name(active_items[0])
    return False, None


def _safe_int(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalize_membership_rows(memberships: dict | list[dict], now: datetime) -> list[dict]:
    rows: list[dict] = []
    for membership_type, items in _membership_collections(memberships):
        for item in items:
            source_id = item.get("id") or item.get("membershipId") or item.get("subscriptionId") or item.get("creditId")
            started_at = _parse_iso_datetime(item.get("startDate") or item.get("createdAt"))
            ended_at = _parse_iso_datetime(item.get("endDate"))
            status = "active"
            if item.get("isVoided"):
                status = "voided"
            elif ended_at and ended_at < now:
                status = "ended"
            elif item.get("isFrozen"):
                status = "frozen"
            elif item.get("renewalCancelled"):
                status = "renewal_cancelled"
            rows.append(
                {
                    "source_membership_id": str(source_id) if source_id is not None else None,
                    "membership_name": _membership_name(item),
                    "membership_type": membership_type,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "status": status,
                    "classes_left": _safe_int(item.get("classesLeft")),
                    "money_left": _safe_int(item.get("moneyLeft")),
                    "is_frozen": bool(item.get("isFrozen")),
                    "renewal_cancelled": bool(item.get("renewalCancelled")),
                    "source_updated_at": _parse_iso_datetime(item.get("updatedAt") or item.get("modifiedAt") or item.get("createdAt")),
                }
            )
    return rows


def _apply_member_context(db: Session, client: Client, context: dict, now: datetime) -> int:
    notes = context.get("notes") or []
    memberships = context.get("memberships") or {}

    activity = client.activity
    if activity is None:
        activity = ClientActivity(client_id=client.id)
    has_active_membership, membership_name = _select_active_membership_name(memberships, now)
    activity.has_active_membership = has_active_membership
    activity.active_membership_name = membership_name
    activity.activity_updated_at = now
    db.add(activity)

    db.query(ClientMembership).filter(ClientMembership.client_id == client.id).delete()
    membership_rows = _normalize_membership_rows(memberships, now)
    for row in membership_rows:
        db.add(
            ClientMembership(
                client_id=client.id,
                source_membership_id=row["source_membership_id"],
                membership_name=row["membership_name"],
                membership_type=row["membership_type"],
                started_at=row["started_at"],
                ended_at=row["ended_at"],
                status=row["status"],
                classes_left=row["classes_left"],
                money_left=row["money_left"],
                is_frozen=row["is_frozen"],
                renewal_cancelled=row["renewal_cancelled"],
                source_updated_at=row["source_updated_at"],
            )
        )

    db.query(ClientNote).filter(ClientNote.client_id == client.id).delete()
    inserted = 0
    for note in notes:
        note_text = (note.get("notePreview") or "").strip()
        if not note_text:
            continue
        injury_flag = _is_injury_note(note_text)
        db.add(
            ClientNote(
                client_id=client.id,
                note_type=note.get("type") or note.get("assignmentType"),
                note_text=note_text,
                is_injury_flag=injury_flag,
                is_front_desk_flag=True,
                is_instructor_flag=injury_flag,
                source_updated_at=_parse_iso_datetime(note.get("modifiedAt") or note.get("createdAt")),
            )
        )
        inserted += 1
    return inserted


def _fetch_member_context_http(http: httpx.Client, host_id: int, member_id: str) -> dict:
    notes_response = http.get(f"/_api/primary/host/{host_id}/customer-notes", params={"memberId": member_id})
    memberships_response = http.get(f"/_api/primary/host/{host_id}/customers/{member_id}/memberships")

    notes: list[dict] = []
    memberships: dict = {}
    if notes_response.status_code == 200 and "application/json" in notes_response.headers.get("content-type", ""):
        notes = notes_response.json()
    if memberships_response.status_code == 200 and "application/json" in memberships_response.headers.get(
        "content-type", ""
    ):
        memberships = memberships_response.json()
    return {"notes": notes, "memberships": memberships}


def _read_booking_rows() -> list[dict[str, str]]:
    csv_path = settings.momence_session_bookings_csv_path.strip()
    if csv_path:
        source = Path(csv_path).expanduser()
        if not source.exists():
            raise RuntimeError(f"MOMENCE_SESSION_BOOKINGS_CSV_PATH does not exist: {source}")
        with source.open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    if settings.momence_allow_browser_booking_report_sync and settings.momence_session_bookings_report_url:
        browser = MomenceBrowserClient()
        return browser.download_report_csv(_build_upcoming_bookings_report_url(), timeout_ms=120000)

    return []


def _build_upcoming_bookings_report_url(reference: datetime | None = None) -> str:
    current = reference or datetime.now(timezone.utc)
    window_start = current.replace(hour=4, minute=0, second=0, microsecond=0)
    window_end = window_start + timedelta(days=settings.momence_upcoming_booking_days)
    params = {
        "computedSaleValue": "true",
        "day": current.date().isoformat(),
        "endDate": window_end.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "endDate2": window_end.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "includeVatInRevenue": "true",
        "preset": "-1",
        "preset2": "-1",
        "startDate": window_start.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "startDate2": window_start.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "timeZone": settings.default_timezone,
    }
    return f"https://momence.com/dashboard/{settings.momence_host_id}/reports/session-bookings?{urlencode(params)}"


def _parse_booking_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    patterns = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d, %I:%M %p",
        "%m/%d/%Y, %I:%M %p",
        "%m/%d/%Y %I:%M %p",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for pattern in patterns:
        try:
            parsed = datetime.strptime(candidate, pattern)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _derive_booking_id(row: dict[str, str], starts_at: datetime | None) -> str:
    booking_key = _first_matching_key(row, ["booking id", "reservation id", "attendance id"])
    if booking_key and row.get(booking_key):
        return row[booking_key]
    member_key = _first_matching_key(row, ["member id", "customer id"])
    session_key = _first_matching_key(row, ["session id", "class id", "event id"])
    email_key = _first_matching_key(row, ["customer email", "e-mail", "email"])
    class_key = _first_matching_key(row, ["class name", "class", "session name", "item", "event"])
    instructor_key = _first_matching_key(row, ["teacher", "instructor"])
    parts = [
        row.get(member_key) if member_key else None,
        row.get(session_key) if session_key else None,
        row.get(email_key) if email_key else None,
        row.get(class_key) if class_key else None,
        row.get(instructor_key) if instructor_key else None,
        starts_at.isoformat() if starts_at else None,
    ]
    return "|".join(part or "" for part in parts)


def _upsert_upcoming_booking_rows(db: Session, rows: list[dict[str, str]]) -> tuple[int, list[str]]:
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(days=settings.momence_upcoming_booking_days)
    impacted_member_ids: list[str] = []
    processed = 0
    booking_map: dict[str, Booking] = {}

    for raw_row in rows:
        row = _normalized(raw_row)
        email_key = _first_matching_key(row, ["customer email", "e-mail", "email"])
        if not email_key or not row.get(email_key):
            continue
        client = db.query(Client).filter(Client.email == row[email_key]).one_or_none()
        if client is None:
            continue

        start_key = _first_matching_key(row, ["class date", "session date", "session start", "starts at", "start", "date"])
        starts_at = _parse_booking_datetime(row.get(start_key) if start_key else None)
        if starts_at is None or starts_at < now or starts_at > window_end:
            continue

        booking_id = _derive_booking_id(row, starts_at)
        if not booking_id.strip():
            continue

        booking = booking_map.get(booking_id)
        if booking is None:
            booking = db.query(Booking).filter(Booking.momence_booking_id == booking_id).one_or_none()
        if booking is None:
            booking = Booking(momence_booking_id=booking_id)
        booking_map[booking_id] = booking

        session_key = _first_matching_key(row, ["session id", "class id", "event id"])
        class_key = _first_matching_key(row, ["class", "session name", "item", "event"])
        instructor_key = _first_matching_key(row, ["teacher", "instructor"])
        location_key = _first_matching_key(row, ["location", "home location", "studio"])
        status_key = _first_matching_key(row, ["status", "booking status"])
        cancelled_key = _first_matching_key(row, ["cancelled"])
        late_cancelled_key = _first_matching_key(row, ["late cancelled"])
        no_show_key = _first_matching_key(row, ["no show"])
        end_key = _first_matching_key(row, ["end", "ends at", "session end"])
        waitlist_key = _first_matching_key(row, ["waitlist", "is waitlist"])

        derived_session_parts = [
            row.get(class_key) or "class",
            row.get(location_key) or "location",
            row.get(instructor_key) or "instructor",
            starts_at.isoformat(),
        ]
        booking.momence_session_id = row.get(session_key) or f"session|{'|'.join(derived_session_parts)}"
        booking.client_id = client.id
        booking.class_name = row.get(class_key)
        booking.location_name = row.get(location_key)
        booking.instructor_name = row.get(instructor_key)
        booking.starts_at = starts_at
        booking.ends_at = _parse_booking_datetime(row.get(end_key) if end_key else None)
        if cancelled_key and row.get(cancelled_key, "").lower() == "yes":
            booking.status = "cancelled"
        elif late_cancelled_key and row.get(late_cancelled_key, "").lower() == "yes":
            booking.status = "late_cancelled"
        elif no_show_key and row.get(no_show_key, "").lower() == "yes" and starts_at < now:
            booking.status = "no_show"
        else:
            booking.status = row.get(status_key) or "booked"
        booking.is_waitlist = (row.get(waitlist_key) or "").lower() in {"1", "true", "yes", "waitlist"}
        booking.synced_at = now
        db.add(booking)

        activity = client.activity
        if activity is None:
            activity = ClientActivity(client_id=client.id)
        current_next_booking_at = _as_utc(activity.next_booking_at)
        if current_next_booking_at is None or starts_at < current_next_booking_at:
            activity.next_booking_at = starts_at
        last_booking_candidates = [value for value in [_as_utc(activity.last_booking_at), starts_at] if value is not None]
        activity.last_booking_at = max(last_booking_candidates) if last_booking_candidates else starts_at
        activity.activity_updated_at = now
        db.add(activity)

        impacted_member_ids.append(client.momence_member_id)
        processed += 1

    return processed, list(dict.fromkeys(impacted_member_ids))


def _session_location_name(session: dict) -> str | None:
    location = session.get("inPersonLocation") or {}
    return location.get("name")


def _session_instructor_name(session: dict) -> str | None:
    teacher = session.get("teacher") or {}
    first_name = (teacher.get("firstName") or "").strip()
    last_name = (teacher.get("lastName") or "").strip()
    return " ".join(part for part in [first_name, last_name] if part) or None


def _booking_status_from_api(booking: dict) -> str:
    if booking.get("cancelledAt"):
        return "cancelled"
    if booking.get("checkedIn") is True:
        return "checked_in"
    return "booked"


def _upsert_client_from_booking_member(db: Session, member: dict, now: datetime) -> Client:
    member_id = str(member["id"])
    client = db.query(Client).filter(Client.momence_member_id == member_id).one_or_none()
    if client is None:
        client = Client(momence_member_id=member_id)
    client.first_name = member.get("firstName")
    client.last_name = member.get("lastName")
    client.full_name = " ".join(filter(None, [client.first_name, client.last_name]))
    client.email = member.get("email")
    client.phone = member.get("phoneNumber")
    client.source_updated_at = now
    db.add(client)
    db.flush()
    return client


def _reset_upcoming_booking_window(db: Session, now: datetime, window_end: datetime) -> None:
    local_day_start = now.astimezone(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(
        timezone.utc
    )
    db.query(Booking).filter(Booking.starts_at >= local_day_start, Booking.starts_at <= window_end).delete(
        synchronize_session=False
    )


def _reset_booking_window(db: Session, window_start: datetime, window_end: datetime) -> None:
    db.query(Booking).filter(Booking.starts_at >= window_start, Booking.starts_at <= window_end).delete(
        synchronize_session=False
    )


def _upsert_upcoming_bookings_from_api(db: Session, booking_rows: list[dict]) -> tuple[int, list[str]]:
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(days=settings.momence_upcoming_booking_days)
    impacted_member_ids: list[str] = []
    processed = 0

    _reset_upcoming_booking_window(db, now, window_end)

    for row in booking_rows:
        session = row.get("session") or {}
        booking_payload = row.get("booking") or {}
        member = booking_payload.get("member") or {}
        member_id = member.get("id")
        booking_id = booking_payload.get("id")
        starts_at = _parse_iso_datetime(session.get("startsAt"))
        if not member_id or not booking_id or starts_at is None:
            continue
        if starts_at < now or starts_at > window_end:
            continue

        client = _upsert_client_from_booking_member(db, member, now)
        booking = Booking(
            momence_booking_id=str(booking_id),
            momence_session_id=str(session.get("id") or ""),
            client_id=client.id,
            class_name=session.get("name"),
            location_name=_session_location_name(session),
            instructor_name=_session_instructor_name(session),
            starts_at=starts_at,
            ends_at=_parse_iso_datetime(session.get("endsAt")),
            status=_booking_status_from_api(booking_payload),
            is_waitlist=False,
            synced_at=now,
        )
        db.add(booking)

        activity = client.activity
        if activity is None:
            activity = ClientActivity(client_id=client.id)
        current_next_booking_at = _as_utc(activity.next_booking_at)
        if current_next_booking_at is None or starts_at < current_next_booking_at:
            activity.next_booking_at = starts_at
        current_last_booking_at = _as_utc(activity.last_booking_at)
        if current_last_booking_at is None or starts_at > current_last_booking_at:
            activity.last_booking_at = starts_at
        activity.activity_updated_at = now
        db.add(activity)

        impacted_member_ids.append(client.momence_member_id)
        processed += 1

    return processed, list(dict.fromkeys(impacted_member_ids))


def _upsert_historical_bookings_from_api(
    db: Session, booking_rows: list[dict], window_start: datetime, window_end: datetime
) -> tuple[int, list[str]]:
    now = datetime.now(timezone.utc)
    impacted_member_ids: list[str] = []
    processed = 0

    _reset_booking_window(db, window_start, window_end)

    for row in booking_rows:
        session = row.get("session") or {}
        booking_payload = row.get("booking") or {}
        member = booking_payload.get("member") or {}
        member_id = member.get("id")
        booking_id = booking_payload.get("id")
        starts_at = _parse_iso_datetime(session.get("startsAt"))
        if not member_id or not booking_id or starts_at is None:
            continue
        if starts_at < window_start or starts_at > window_end:
            continue

        client = _upsert_client_from_booking_member(db, member, now)
        booking = Booking(
            momence_booking_id=str(booking_id),
            momence_session_id=str(session.get("id") or ""),
            client_id=client.id,
            class_name=session.get("name"),
            location_name=_session_location_name(session),
            instructor_name=_session_instructor_name(session),
            starts_at=starts_at,
            ends_at=_parse_iso_datetime(session.get("endsAt")),
            status=_booking_status_from_api(booking_payload),
            is_waitlist=False,
            synced_at=now,
        )
        db.add(booking)

        activity = client.activity
        if activity is None:
            activity = ClientActivity(client_id=client.id)
        current_last_booking_at = _as_utc(activity.last_booking_at)
        if current_last_booking_at is None or starts_at > current_last_booking_at:
            activity.last_booking_at = starts_at
        if activity.first_visit_at is None or (_as_utc(activity.first_visit_at) and starts_at < _as_utc(activity.first_visit_at)):
            activity.first_visit_at = starts_at
        activity.activity_updated_at = now
        db.add(activity)

        impacted_member_ids.append(client.momence_member_id)
        processed += 1

    return processed, list(dict.fromkeys(impacted_member_ids))


def _read_upcoming_bookings_from_host_api() -> list[dict]:
    now = datetime.now(timezone.utc)
    window_end = now + timedelta(days=settings.momence_upcoming_booking_days)
    client = MomenceClient()
    return asyncio.run(client.fetch_upcoming_bookings(now, window_end))


def _read_upcoming_bookings_window_from_host_api(window_start: datetime, window_end: datetime) -> list[dict]:
    client = MomenceClient()
    return asyncio.run(client.fetch_upcoming_bookings(window_start, window_end))


def _read_booking_history_window_from_host_api(window_start: datetime, window_end: datetime) -> list[dict]:
    client = MomenceClient()
    return asyncio.run(client.fetch_session_bookings_between(window_start, window_end))


def _read_member_booking_history_from_host_api(momence_member_id: str) -> list[dict]:
    client = MomenceClient()
    return asyncio.run(client.fetch_member_session_bookings(momence_member_id))


def _get_history_cursor(db: Session, fallback_start: datetime) -> datetime:
    state = db.get(SyncState, "booking_history")
    if state and state.last_successful_at is not None:
        return _as_utc(state.last_successful_at) or fallback_start
    return fallback_start


def get_booking_history_progress(db: Session) -> BookingHistoryProgressResponse:
    now = datetime.now(timezone.utc)
    history_days = min(settings.momence_history_booking_days, 60)
    chunk_days = max(1, min(settings.momence_history_booking_chunk_days, 7))
    window_start = now - timedelta(days=history_days)
    state = db.get(SyncState, "booking_history")
    cursor = _get_history_cursor(db, window_start)
    if cursor < window_start:
        cursor = window_start
    if cursor > now:
        cursor = now
    # Treat a near-current cursor as complete so tiny clock drift does not keep
    # the progress endpoint artificially "incomplete" forever.
    complete_threshold = now - timedelta(minutes=5)
    return BookingHistoryProgressResponse(
        window_start=window_start,
        window_end=now,
        cursor=cursor,
        chunk_days=chunk_days,
        complete=cursor >= complete_threshold,
        records_processed_last_chunk=state.records_processed if state else 0,
        status=state.status if state else "unknown",
        error_text=state.error_text if state else None,
    )


def refresh_clients_by_member_ids(db: Session, momence_member_ids: list[str]) -> SyncRunResponse:
    unique_member_ids = list(dict.fromkeys(member_id.strip() for member_id in momence_member_ids if member_id.strip()))
    run = _start_run(db, "refresh_clients_batch")
    try:
        if not unique_member_ids:
            raise RuntimeError("No member ids provided")
        if len(unique_member_ids) > settings.momence_max_context_refresh_batch:
            raise RuntimeError(
                f"Requested {len(unique_member_ids)} clients, limit is {settings.momence_max_context_refresh_batch}"
            )

        clients = db.query(Client).filter(Client.momence_member_id.in_(unique_member_ids)).all()
        client_map = {client.momence_member_id: client for client in clients}
        missing = [member_id for member_id in unique_member_ids if member_id not in client_map]
        if missing:
            raise RuntimeError(f"Client ids not found locally: {', '.join(missing[:5])}")

        now = datetime.now(timezone.utc)
        records_processed = 0
        api_client = MomenceClient()
        for member_id in unique_member_ids:
            notes = asyncio.run(api_client.fetch_member_notes(member_id))
            memberships = asyncio.run(api_client.fetch_member_memberships(member_id))
            context = {"notes": notes, "memberships": memberships}
            records_processed += _apply_member_context(db, client_map[member_id], context, now)
        db.commit()
        refresh_all_flags(db)
        record_sync_state(db, "memberships_notes", status="completed", records_processed=len(unique_member_ids))
        record_sync_state(db, "flags", status="completed", records_processed=len(unique_member_ids))
        return _finish_run(db, run, "completed", records_processed)
    except Exception as exc:  # pragma: no cover - targeted browser integration
        db.rollback()
        record_sync_state(db, "memberships_notes", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_upcoming_bookings(db: Session) -> SyncRunResponse:
    run = _start_run(db, "sync_upcoming_bookings")
    try:
        try:
            api_rows = _read_upcoming_bookings_from_host_api()
            records_processed, impacted_member_ids = _upsert_upcoming_bookings_from_api(db, api_rows)
        except Exception:
            rows = _read_booking_rows()
            records_processed, impacted_member_ids = _upsert_upcoming_booking_rows(db, rows)
        db.commit()
        if impacted_member_ids and settings.momence_browser_profile_dir.strip():
            try:
                limited_member_ids = impacted_member_ids[: settings.momence_max_context_refresh_batch]
                refresh_clients_by_member_ids(db, limited_member_ids)
            except Exception as exc:
                record_sync_state(
                    db,
                    "memberships_notes",
                    status="failed",
                    records_processed=0,
                    error_text=f"Optional enrichment skipped: {exc}",
                )
                db.commit()
        record_sync_state(db, "bookings", status="completed", records_processed=records_processed)
        return _finish_run(db, run, "completed", records_processed)
    except Exception as exc:  # pragma: no cover - scaffolding path
        db.rollback()
        record_sync_state(db, "bookings", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_bookings_for_day(db: Session, day: date) -> SyncRunResponse:
    run = _start_run(db, "sync_bookings_for_day")
    try:
        start_local = datetime.combine(day, datetime.min.time(), tzinfo=LOCAL_TZ)
        end_local = start_local + timedelta(days=1)
        window_start = start_local.astimezone(timezone.utc)
        window_end = end_local.astimezone(timezone.utc)
        api_rows = _read_upcoming_bookings_window_from_host_api(window_start, window_end)
        records_processed, _ = _upsert_upcoming_bookings_from_api(db, api_rows)
        db.commit()
        record_sync_state(db, "bookings", status="completed", records_processed=records_processed)
        return _finish_run(db, run, "completed", records_processed)
    except Exception as exc:
        db.rollback()
        record_sync_state(db, "bookings", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_recent_booking_history(db: Session) -> SyncRunResponse:
    run = _start_run(db, "sync_recent_booking_history")
    try:
        now = datetime.now(timezone.utc)
        history_days = min(settings.momence_history_booking_days, 60)
        chunk_days = max(1, min(settings.momence_history_booking_chunk_days, 7))
        backfill_start = now - timedelta(days=history_days)
        cursor = _get_history_cursor(db, backfill_start)
        if cursor < backfill_start:
            cursor = backfill_start
        if cursor >= now:
            record_sync_state(db, "booking_history", status="completed", records_processed=0, synced_at=now)
            db.commit()
            return _finish_run(db, run, "completed", 0)

        window_start = cursor
        window_end = min(window_start + timedelta(days=chunk_days), now)
        rows = _read_booking_history_window_from_host_api(window_start, window_end)
        records_processed, _ = _upsert_historical_bookings_from_api(db, rows, window_start, window_end)
        db.commit()
        preferences_processed = recompute_preferences_from_bookings(db)
        refresh_all_flags(db)
        record_sync_state(
            db,
            "booking_history",
            status="completed",
            records_processed=records_processed,
            synced_at=window_end,
        )
        record_sync_state(db, "behavior", status="completed", records_processed=preferences_processed)
        record_sync_state(db, "flags", status="completed", records_processed=records_processed)
        db.commit()
        return _finish_run(db, run, "completed", records_processed)
    except Exception as exc:  # pragma: no cover - host api integration
        db.rollback()
        record_sync_state(db, "booking_history", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_recent_booking_history_chunks(db: Session, *, max_chunks: int) -> SyncRunResponse:
    run = _start_run(db, "sync_recent_booking_history_chunks")
    completed_chunks = 0
    processed_total = 0
    try:
        chunk_limit = max(1, min(max_chunks, 5))
        last_error: str | None = None
        while completed_chunks < chunk_limit:
            progress = get_booking_history_progress(db)
            if progress.complete:
                break
            result = sync_recent_booking_history(db)
            processed_total += result.records_processed
            completed_chunks += 1
            if result.status != "completed":
                last_error = result.error_text
                break
            if result.records_processed == 0:
                break
        final_status = "completed" if last_error is None else "failed"
        return _finish_run(db, run, final_status, processed_total, last_error)
    except Exception as exc:  # pragma: no cover - orchestration path
        db.rollback()
        return _finish_run(db, run, "failed", processed_total, str(exc))


def _recompute_client_activity_from_bookings(db: Session, client: Client, now: datetime) -> None:
    activity = client.activity
    if activity is None:
        activity = ClientActivity(client_id=client.id)

    bookings = db.scalars(
        select(Booking)
        .where(Booking.client_id == client.id, Booking.status != "cancelled")
        .order_by(Booking.starts_at.asc())
    ).all()

    if bookings:
        non_cancelled = [booking for booking in bookings if booking.status != "cancelled"]
        if non_cancelled:
            activity.last_booking_at = non_cancelled[-1].starts_at

        attended = [
            booking
            for booking in non_cancelled
            if (_as_utc(booking.starts_at) or now) <= now and booking.status == "checked_in"
        ]
        if attended:
            activity.first_visit_at = attended[0].starts_at
        lifetime_count = len(attended)
        activity.lifetime_visits_baseline = lifetime_count
        activity.lifetime_visits_baseline_as_of = now
        activity.total_visits = lifetime_count + (activity.lifetime_visits_increment or 0)

        if attended:
            activity.last_checkin_at = attended[-1].starts_at

        future_bookings = [
            booking
            for booking in non_cancelled
            if (_as_utc(booking.starts_at) or now) >= now and booking.status in {"booked", "checked_in"}
        ]
        activity.next_booking_at = future_bookings[0].starts_at if future_bookings else None

    activity.activity_updated_at = now
    db.add(activity)


def _upsert_member_booking_history_rows(db: Session, client: Client, rows: list[dict]) -> int:
    now = datetime.now(timezone.utc)
    processed = 0
    seen_booking_ids: set[str] = set()

    for item in rows:
        if not isinstance(item, dict):
            continue
        session = item.get("session") or {}
        booking_id = item.get("id")
        starts_at = _parse_iso_datetime(session.get("startsAt")) if isinstance(session, dict) else None
        if not booking_id or starts_at is None:
            continue
        booking_id = str(booking_id)
        if booking_id in seen_booking_ids:
            continue
        seen_booking_ids.add(booking_id)

        booking = db.query(Booking).filter(Booking.momence_booking_id == booking_id).one_or_none()
        if booking is None:
            booking = Booking(momence_booking_id=booking_id)

        booking.momence_session_id = str(session.get("id") or booking.momence_session_id or "")
        booking.client_id = client.id
        booking.class_name = session.get("name") or booking.class_name
        booking.location_name = _session_location_name(session) or booking.location_name
        booking.instructor_name = _session_instructor_name(session) or booking.instructor_name
        booking.starts_at = starts_at
        booking.ends_at = _parse_iso_datetime(session.get("endsAt")) or booking.ends_at
        booking.status = _booking_status_from_api(item)
        booking.is_waitlist = False
        booking.synced_at = now
        db.add(booking)
        processed += 1

    _recompute_client_activity_from_bookings(db, client, now)
    return processed


def sync_roster_client_history(db: Session, *, day: date | None = None, max_clients: int = 25, offset: int = 0) -> SyncRunResponse:
    run = _start_run(db, "sync_roster_client_history")
    try:
        target_day = day or datetime.now(LOCAL_TZ).date()
        start_dt, end_dt = _local_day_bounds(target_day)
        limit = max(1, min(max_clients, settings.momence_max_context_refresh_batch))
        clients = db.scalars(
            select(Client)
            .join(Booking, Booking.client_id == Client.id)
            .where(Booking.starts_at >= start_dt, Booking.starts_at < end_dt)
            .distinct()
            .order_by(Client.full_name)
            .offset(max(offset, 0))
            .limit(limit)
        ).all()

        if not clients:
            record_sync_state(db, "roster_history", status="completed", records_processed=0)
            db.commit()
            return _finish_run(db, run, "completed", 0)

        processed = 0
        failures: list[str] = []
        for client in clients:
            try:
                rows = _read_member_booking_history_from_host_api(client.momence_member_id)
                processed += _upsert_member_booking_history_rows(db, client, rows)
            except Exception as exc:
                failures.append(f"{client.momence_member_id}: {exc}")

        db.commit()
        preferences_processed = recompute_preferences_from_bookings(db)
        refresh_all_flags(db)
        record_sync_state(
            db,
            "roster_history",
            status="completed" if not failures else "partial",
            records_processed=processed,
            error_text="; ".join(failures[:5]) if failures else None,
        )
        record_sync_state(db, "behavior", status="completed", records_processed=preferences_processed)
        record_sync_state(db, "flags", status="completed", records_processed=processed)
        db.commit()
        return _finish_run(db, run, "completed" if not failures else "partial", processed, "; ".join(failures[:5]) if failures else None)
    except Exception as exc:  # pragma: no cover - targeted host api integration
        db.rollback()
        record_sync_state(db, "roster_history", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def refresh_client_by_member_id(db: Session, momence_member_id: str) -> SyncRunResponse:
    return refresh_clients_by_member_ids(db, [momence_member_id])


def recompute_flags_job(db: Session) -> SyncRunResponse:
    run = _start_run(db, "recompute_flags")
    try:
        records_processed = refresh_all_flags(db)
        record_sync_state(db, "flags", status="completed", records_processed=records_processed)
        return _finish_run(db, run, "completed", records_processed)
    except Exception as exc:  # pragma: no cover - scaffolding path
        record_sync_state(db, "flags", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_active_customers_from_browser(db: Session, active_days: int = 180) -> SyncRunResponse:
    run = _start_run(db, "sync_active_customers_from_browser")
    try:
        browser = MomenceBrowserClient()
        customers = browser.fetch_active_customers(active_days=active_days)
        for customer in customers:
            _upsert_customer(db, customer)
        db.commit()
        record_sync_state(db, "active_customers", status="completed", records_processed=len(customers))
        refresh_all_flags(db)
        record_sync_state(db, "flags", status="completed", records_processed=len(customers))
        return _finish_run(db, run, "completed", len(customers))
    except Exception as exc:  # pragma: no cover - browser integration
        db.rollback()
        record_sync_state(db, "active_customers", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_birthdays_from_browser(db: Session) -> SyncRunResponse:
    run = _start_run(db, "sync_birthdays_from_browser")
    try:
        browser = MomenceBrowserClient()
        rows = browser.download_birthdays_csv()
        updates = 0
        for row in rows:
            email = row.get("E-mail")
            if not email:
                continue
            client = db.query(Client).filter(Client.email == email).one_or_none()
            if client is None:
                continue
            client.birthday = _parse_date(row.get("Birthday"))
            db.add(client)
            updates += 1
        db.commit()
        record_sync_state(db, "birthdays", status="completed", records_processed=updates)
        refresh_all_flags(db)
        record_sync_state(db, "flags", status="completed", records_processed=updates)
        return _finish_run(db, run, "completed", updates)
    except Exception as exc:  # pragma: no cover - browser integration
        db.rollback()
        record_sync_state(db, "birthdays", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_customer_fields_from_browser(db: Session) -> SyncRunResponse:
    run = _start_run(db, "sync_customer_fields_from_browser")
    try:
        browser = MomenceBrowserClient()
        if not settings.momence_customer_field_values_report_url:
            raise RuntimeError("MOMENCE_CUSTOMER_FIELD_VALUES_REPORT_URL is not configured.")
        rows = browser.download_report_csv(settings.momence_customer_field_values_report_url, timeout_ms=120000)
        updates = _apply_customer_field_rows(db, rows)
        db.commit()
        record_sync_state(db, "customer_fields", status="completed", records_processed=updates)
        return _finish_run(db, run, "completed", updates)
    except Exception as exc:  # pragma: no cover - browser integration
        db.rollback()
        record_sync_state(db, "customer_fields", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_browser_seed_data(db: Session) -> SyncRunResponse:
    run = _start_run(db, "sync_browser_seed_data")
    try:
        browser = MomenceBrowserClient()
        customers = browser.fetch_active_customers(active_days=180)
        for customer in customers:
            _upsert_customer(db, customer)
        birthday_rows = browser.download_birthdays_csv()
        for row in birthday_rows:
            email = row.get("E-mail")
            if not email:
                continue
            client = db.query(Client).filter(Client.email == email).one_or_none()
            if client is None:
                continue
            client.birthday = _parse_date(row.get("Birthday"))
            db.add(client)
        if settings.momence_customer_field_values_report_url:
            field_rows = browser.download_report_csv(settings.momence_customer_field_values_report_url, timeout_ms=120000)
            field_updates = _apply_customer_field_rows(db, field_rows)
            record_sync_state(db, "customer_fields", status="completed", records_processed=field_updates)
        db.commit()
        record_sync_state(db, "active_customers", status="completed", records_processed=len(customers))
        record_sync_state(db, "birthdays", status="completed", records_processed=len(birthday_rows))
        refresh_all_flags(db)
        record_sync_state(db, "flags", status="completed", records_processed=len(customers))
        return _finish_run(db, run, "completed", len(customers))
    except Exception as exc:  # pragma: no cover - browser integration
        db.rollback()
        record_sync_state(db, "active_customers", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def sync_client_behavior_from_reports(db: Session) -> SyncRunResponse:
    run = _start_run(db, "sync_client_behavior_from_reports")
    try:
        browser = MomenceBrowserClient()
        processed = 0

        if settings.momence_customer_list_report_url:
            rows = browser.download_report_csv(settings.momence_customer_list_report_url, timeout_ms=45000)
            processed += _apply_customer_list_rows(db, rows)

        if settings.momence_customer_attendance_report_url:
            rows = browser.download_report_csv(settings.momence_customer_attendance_report_url, timeout_ms=45000)
            processed += _apply_customer_list_rows(db, rows)

        if settings.momence_session_bookings_report_url:
            rows = browser.download_report_csv(settings.momence_session_bookings_report_url, timeout_ms=45000)
            processed += _apply_session_booking_rows(db, rows)

        db.commit()
        record_sync_state(db, "behavior", status="completed", records_processed=processed)
        refresh_all_flags(db)
        record_sync_state(db, "flags", status="completed", records_processed=processed)
        return _finish_run(db, run, "completed", processed)
    except Exception as exc:  # pragma: no cover - browser integration
        db.rollback()
        record_sync_state(db, "behavior", status="failed", records_processed=0, error_text=str(exc))
        return _finish_run(db, run, "failed", 0, str(exc))


def _apply_customer_list_rows(db: Session, rows: list[dict[str, str]]) -> int:
    processed = 0
    for raw_row in rows:
        row = _normalized(raw_row)
        email_key = _first_matching_key(row, ["e-mail", "email"])
        if not email_key or not row.get(email_key):
            continue
        client = db.query(Client).filter(Client.email == row[email_key]).one_or_none()
        if client is None:
            continue
        activity = db.query(ClientActivity).filter(ClientActivity.client_id == client.id).one_or_none()
        if activity is None:
            activity = ClientActivity(client_id=client.id)
        visits_key = _first_matching_key(row, ["# of visits", "total visits", "visits", "visit count", "class visits"])
        if visits_key and row.get(visits_key):
            digits = "".join(ch for ch in row[visits_key] if ch.isdigit())
            if digits:
                baseline = int(digits)
                activity.lifetime_visits_baseline = baseline
                activity.lifetime_visits_baseline_as_of = datetime.now(timezone.utc)
                activity.total_visits = baseline + (activity.lifetime_visits_increment or 0)
        last_seen_key = _first_matching_key(row, ["last seen", "last activity", "last visit", "most recent visit"])
        if last_seen_key and row.get(last_seen_key):
            parsed = _try_parse_report_datetime(row[last_seen_key])
            if parsed:
                activity.last_checkin_at = parsed
        db.add(activity)
        processed += 1
    return processed


def _try_parse_report_datetime(value: str) -> datetime | None:
    patterns = [
        "%Y-%m-%d, %I:%M %p",
        "%m/%d/%Y, %I:%M %p",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ]
    for pattern in patterns:
        try:
            parsed = datetime.strptime(value, pattern)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _apply_session_booking_rows(db: Session, rows: list[dict[str, str]]) -> int:
    stats: dict[str, dict[str, Counter]] = defaultdict(
        lambda: {
            "formats": Counter(),
            "instructors": Counter(),
            "weekdays": Counter(),
            "times": Counter(),
        }
    )

    for raw_row in rows:
        row = _normalized(raw_row)
        email_key = _first_matching_key(row, ["customer email", "e-mail", "email"])
        if not email_key or not row.get(email_key):
            continue
        email = row[email_key]

        class_key = _first_matching_key(row, ["class name", "class", "session name", "item", "event"])
        instructor_key = _first_matching_key(row, ["teacher", "instructor"])
        start_key = _first_matching_key(row, ["class date", "session date", "starts at", "session start", "start", "date"])

        if class_key and row.get(class_key):
            stats[email]["formats"][row[class_key]] += 1
        if instructor_key and row.get(instructor_key):
            stats[email]["instructors"][row[instructor_key]] += 1
        if start_key and row.get(start_key):
            parsed = _try_parse_report_datetime(row[start_key])
            if parsed:
                stats[email]["weekdays"][parsed.strftime("%A")] += 1
                hour = parsed.hour
                if hour < 11:
                    stats[email]["times"]["morning"] += 1
                elif hour < 17:
                    stats[email]["times"]["midday"] += 1
                else:
                    stats[email]["times"]["evening"] += 1

    processed = 0
    for email, counters in stats.items():
        client = db.query(Client).filter(Client.email == email).one_or_none()
        if client is None:
            continue
        preference = client.preferences
        if preference is None:
            from app.db.models import ClientPreference

            preference = ClientPreference(client_id=client.id)
        preference.favorite_formats = "|".join(name for name, _ in counters["formats"].most_common(3))
        preference.favorite_instructors = "|".join(name for name, _ in counters["instructors"].most_common(3))
        preference.favorite_weekdays = "|".join(name for name, _ in counters["weekdays"].most_common(3))
        preference.favorite_time_of_day = (
            counters["times"].most_common(1)[0][0] if counters["times"] else None
        )
        preference.preference_basis = "session_bookings_report"
        preference.computed_at = datetime.now(timezone.utc)
        db.add(preference)
        processed += 1
    return processed


def recompute_preferences_from_bookings(db: Session, *, lookback_days: int = 180) -> int:
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=lookback_days)
    bookings = db.scalars(
        select(Booking).where(
            Booking.starts_at >= window_start,
            Booking.starts_at < now,
            Booking.status != "cancelled",
        )
    ).all()

    stats: dict[object, dict[str, Counter]] = defaultdict(
        lambda: {
            "formats": Counter(),
            "instructors": Counter(),
            "weekdays": Counter(),
            "times": Counter(),
        }
    )

    for booking in bookings:
        starts_at = _as_utc(booking.starts_at)
        if starts_at is None:
            continue
        client_stats = stats[booking.client_id]
        if booking.class_name:
            client_stats["formats"][booking.class_name] += 1
        if booking.instructor_name:
            client_stats["instructors"][booking.instructor_name] += 1
        client_stats["weekdays"][starts_at.strftime("%A")] += 1
        local_hour = starts_at.astimezone(LOCAL_TZ).hour
        if local_hour < 11:
            client_stats["times"]["morning"] += 1
        elif local_hour < 17:
            client_stats["times"]["midday"] += 1
        else:
            client_stats["times"]["evening"] += 1

    processed = 0
    upsert_sql = text(
        """
        INSERT INTO client_preferences (
            client_id,
            favorite_time_of_day,
            favorite_weekdays,
            favorite_instructors,
            favorite_formats,
            preference_basis,
            computed_at
        ) VALUES (
            :client_id,
            :favorite_time_of_day,
            :favorite_weekdays,
            :favorite_instructors,
            :favorite_formats,
            :preference_basis,
            :computed_at
        )
        ON CONFLICT(client_id) DO UPDATE SET
            favorite_time_of_day = excluded.favorite_time_of_day,
            favorite_weekdays = excluded.favorite_weekdays,
            favorite_instructors = excluded.favorite_instructors,
            favorite_formats = excluded.favorite_formats,
            preference_basis = excluded.preference_basis,
            computed_at = excluded.computed_at
        """
    )
    for client_id, counters in stats.items():
        db.execute(
            upsert_sql,
            {
                "client_id": client_id.hex if hasattr(client_id, "hex") else str(client_id).replace("-", ""),
                "favorite_time_of_day": counters["times"].most_common(1)[0][0] if counters["times"] else None,
                "favorite_weekdays": "|".join(name for name, _ in counters["weekdays"].most_common(3)),
                "favorite_instructors": "|".join(name for name, _ in counters["instructors"].most_common(3)),
                "favorite_formats": "|".join(name for name, _ in counters["formats"].most_common(3)),
                "preference_basis": "booking_history",
                "computed_at": now,
            },
        )
        processed += 1
    return processed


def _apply_customer_field_rows(db: Session, rows: list[dict[str, str]]) -> int:
    updates = 0
    for raw_row in rows:
        row = _normalized(raw_row)
        email_key = _first_matching_key(row, ["e-mail", "email", "customer email"])
        field_name_key = _first_matching_key(row, ["field name"])
        field_value_key = _first_matching_key(row, ["field value"])
        if not email_key or not field_name_key or not field_value_key:
            continue
        email = row.get(email_key)
        field_name = row.get(field_name_key)
        field_value = row.get(field_value_key)
        if not email or not field_name:
            continue
        client = db.query(Client).filter(Client.email == email).one_or_none()
        if client is None:
            continue
        profile = client.profile_data
        if profile is None:
            profile = ClientProfileData(client_id=client.id)
        if field_name == "Tell us one fun fact about you!":
            profile.fun_fact = field_value or None
        elif field_name == "Pregnant":
            profile.pregnant_status = field_value or None
        elif field_name == "Pregnancy Due Date":
            profile.pregnancy_due_date = _parse_date(field_value)
        elif field_name == "How Did You Hear About Us?":
            profile.heard_about_us = field_value or None
        else:
            continue
        profile.updated_at = datetime.now(timezone.utc)
        db.add(profile)
        updates += 1
    return updates
