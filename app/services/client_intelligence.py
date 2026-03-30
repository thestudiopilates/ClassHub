from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import re
from zoneinfo import ZoneInfo

from app.db.models import Booking, Client, ClientActivity

VISIT_MILESTONES = {25, 50, 100, 200, 300, 400, 500, 750, 1000}


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def booking_as_local(value: datetime | None, tz_name: str = "America/New_York") -> datetime | None:
    if value is None:
        return None
    local_tz = ZoneInfo(tz_name)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).astimezone(local_tz)
    return value.astimezone(local_tz)


def canonical_lifetime_visits(activity: ClientActivity | None) -> int:
    if activity is None:
        return 0
    baseline_total = (activity.lifetime_visits_baseline or 0) + (activity.lifetime_visits_increment or 0)
    fallback_total = activity.total_visits or 0
    rolling_floor = (activity.visits_last_30d or 0) + (activity.visits_previous_30d or 0)
    return max(baseline_total, fallback_total, rolling_floor)


def attended_bookings(client: Client, now: datetime | None = None) -> list[Booking]:
    reference = now or datetime.now(timezone.utc)
    attended: list[Booking] = []
    for booking in getattr(client, "bookings", []):
        starts_at = as_utc(booking.starts_at)
        if starts_at is None or starts_at > reference:
            continue
        if booking.status != "checked_in":
            continue
        attended.append(booking)
    attended.sort(key=lambda item: as_utc(item.starts_at) or reference)
    return attended


def canonical_client_lifetime_visits(client: Client, now: datetime | None = None) -> int:
    attended = attended_bookings(client, now)
    attended_total = len(attended)
    activity_total = canonical_lifetime_visits(client.activity)
    return max(attended_total, activity_total)


def canonical_visit_windows(client: Client, now: datetime | None = None) -> tuple[int, int]:
    reference = now or datetime.now(timezone.utc)
    attended = attended_bookings(client, reference)
    activity = client.activity
    activity_current = activity.visits_last_30d if activity else 0
    activity_previous = activity.visits_previous_30d if activity else 0
    if attended:
        current_start = reference - timedelta(days=30)
        previous_start = reference - timedelta(days=60)
        current = sum(1 for booking in attended if (as_utc(booking.starts_at) or reference) >= current_start)
        previous = sum(
            1
            for booking in attended
            if previous_start <= (as_utc(booking.starts_at) or reference) < current_start
        )
        return max(current, activity_current or 0), max(previous, activity_previous or 0)
    if activity is None:
        return 0, 0
    return activity_current or 0, activity_previous or 0


def normalize_text_label(value: str | None) -> str | None:
    if not value:
        return None
    collapsed = " ".join(value.split()).strip()
    return collapsed or None


def normalize_format_label(value: str | None) -> str | None:
    label = normalize_text_label(value)
    if not label:
        return None
    label = re.sub(r"\s*-\s*(Emory|W\.?\s*Midtown|West\s*Midtown)\s*$", "", label, flags=re.IGNORECASE)
    label = re.sub(r"\s*-\s*\((Emory|W\.?\s*Midtown|West\s*Midtown)\)\s*$", "", label, flags=re.IGNORECASE)
    label = re.sub(r"\s*\((Emory|W\.?\s*Midtown|West\s*Midtown)\)\s*$", "", label, flags=re.IGNORECASE)
    return " ".join(label.split()).strip()


def normalize_instructor_key(value: str | None) -> str | None:
    label = normalize_text_label(value)
    if not label:
        return None
    return re.sub(r"[^a-z0-9]+", "", label.casefold())


def prefer_official_bookings(bookings: list[Booking]) -> list[Booking]:
    if any(booking.ends_at is not None for booking in bookings):
        return [booking for booking in bookings if booking.ends_at is not None]
    return bookings


def filter_relevant_bookings(
    bookings: list[Booking],
    day: date,
    *,
    now_local: datetime | None = None,
    tz_name: str = "America/New_York",
    default_duration_minutes: int = 75,
) -> list[Booking]:
    if not bookings:
        return bookings

    local_now = now_local or datetime.now(ZoneInfo(tz_name))
    if day != local_now.date():
        return bookings

    relevant: list[Booking] = []
    for booking in bookings:
        starts_local = booking_as_local(booking.starts_at, tz_name)
        if starts_local is None:
            continue
        ends_local = booking_as_local(booking.ends_at, tz_name)
        if ends_local is None:
            ends_local = starts_local + timedelta(minutes=default_duration_minutes)
        if ends_local >= local_now:
            relevant.append(booking)
    return relevant
