from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import Booking, Client, ClientActivity, ClientFlag, Milestone
from app.services.domain import VISIT_MILESTONES, build_client_profile, build_flag_summary, build_milestones
from app.services.sync_state import get_freshness_map

LOCAL_TZ = ZoneInfo("America/New_York")


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _as_local(value: datetime | None) -> datetime | None:
    current = _as_utc(value)
    if current is None:
        return None
    return current.astimezone(LOCAL_TZ)


def _booking_as_local(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=LOCAL_TZ)
    return value.astimezone(LOCAL_TZ)


def _risk_rank(level: str | None) -> int:
    if level == "high":
        return 3
    if level == "medium":
        return 2
    if level == "low":
        return 1
    return 0


def _canonical_lifetime_visits(activity: ClientActivity | None) -> int:
    if activity is None:
        return 0

    baseline_total = (activity.lifetime_visits_baseline or 0) + (activity.lifetime_visits_increment or 0)
    fallback_total = activity.total_visits or 0
    rolling_floor = (activity.visits_last_30d or 0) + (activity.visits_previous_30d or 0)
    return max(baseline_total, fallback_total, rolling_floor)


def _attended_bookings(client: Client, now: datetime | None = None) -> list[Booking]:
    reference = now or datetime.now(timezone.utc)
    attended: list[Booking] = []
    for booking in client.bookings:
        starts_at = _as_utc(booking.starts_at)
        if starts_at is None or starts_at > reference:
            continue
        if booking.status != "checked_in":
            continue
        attended.append(booking)
    attended.sort(key=lambda item: _as_utc(item.starts_at) or reference)
    return attended


def _canonical_client_lifetime_visits(client: Client, now: datetime | None = None) -> int:
    attended = _attended_bookings(client, now)
    if attended:
        return len(attended)
    return _canonical_lifetime_visits(client.activity)


def _canonical_visit_windows(client: Client, now: datetime | None = None) -> tuple[int, int]:
    reference = now or datetime.now(timezone.utc)
    attended = _attended_bookings(client, reference)
    activity = client.activity
    activity_current = activity.visits_last_30d if activity else 0
    activity_previous = activity.visits_previous_30d if activity else 0
    if attended:
        current_start = reference - timedelta(days=30)
        previous_start = reference - timedelta(days=60)
        current = sum(1 for booking in attended if (_as_utc(booking.starts_at) or reference) >= current_start)
        previous = sum(
            1
            for booking in attended
            if previous_start <= (_as_utc(booking.starts_at) or reference) < current_start
        )
        return max(current, activity_current or 0), max(previous, activity_previous or 0)
    if activity is None:
        return 0, 0
    return activity_current or 0, activity_previous or 0


def _join_date_label(client: Client) -> str:
    attended = _attended_bookings(client)
    if attended:
        return _booking_as_local(attended[0].starts_at).strftime("%b %-d, %Y")
    activity = client.activity
    if activity is None or activity.first_visit_at is None:
        return "Join date still loading"
    return _booking_as_local(activity.first_visit_at).strftime("%b %-d, %Y")


def _membership_sort_key(value) -> datetime:
    fallback = datetime.min.replace(tzinfo=timezone.utc)
    candidate = value.started_at or value.ended_at or value.source_updated_at
    if candidate is None:
        return fallback
    current = _as_utc(candidate)
    return current or fallback


def _active_memberships(client: Client) -> list:
    now = datetime.now(timezone.utc)
    active: list = []
    for membership in client.memberships:
        status = (membership.status or "").lower()
        ended_at = _as_utc(membership.ended_at)
        if status == "active" or ended_at is None or ended_at >= now:
            active.append(membership)
    active.sort(key=_membership_sort_key, reverse=True)
    return active


def _active_membership_label(client: Client) -> str | None:
    memberships = _active_memberships(client)
    if memberships:
        return memberships[0].membership_name or "Active membership"
    if client.memberships:
        memberships = sorted(client.memberships, key=_membership_sort_key, reverse=True)
        first = memberships[0]
        if first.membership_name:
            return first.membership_name
    if client.activity and client.activity.active_membership_name:
        return client.activity.active_membership_name
    return None


def _current_membership_record(client: Client):
    memberships = _active_memberships(client)
    if memberships:
        return memberships[0]
    if client.memberships:
        memberships = sorted(client.memberships, key=_membership_sort_key, reverse=True)
        return memberships[0]
    return None


def _membership_expiration_context(client: Client) -> str | None:
    membership = _current_membership_record(client)
    if membership is None:
        return None
    bits: list[str] = []
    if membership.is_frozen:
        bits.append("Frozen")
    if membership.classes_left is not None:
        bits.append(f"{membership.classes_left} classes left")
    if membership.ended_at is not None:
        end_label = _booking_as_local(membership.ended_at).strftime("%b %-d")
        if membership.renewal_cancelled:
            bits.append(f"Ends {end_label}")
        else:
            name = (membership.membership_name or "").lower()
            if "pack" in name or "credit" in name or membership.classes_left is not None:
                bits.append(f"Expires {end_label}")
            else:
                bits.append(f"Through {end_label}")
    return " · ".join(bits) if bits else None


def _booking_class_number_today(client: Client, booking: Booking | None, now: datetime) -> int | None:
    if booking is None:
        return None
    base_lifetime = _canonical_client_lifetime_visits(client, now)
    booking_day = _booking_as_local(booking.starts_at).date() if booking.starts_at else None
    if booking_day is None:
        return None
    day_bookings = [
        item
        for item in client.bookings
        if item.status != "cancelled"
        and item.starts_at is not None
        and _booking_as_local(item.starts_at).date() == booking_day
    ]
    day_bookings.sort(key=lambda item: (_as_utc(item.starts_at) or now, item.momence_booking_id))
    already_counted_today = sum(
        1
        for item in day_bookings
        if item.status == "checked_in" and (_as_utc(item.starts_at) or now) <= now
    )
    base_before_day = max(base_lifetime - already_counted_today, 0)
    for index, item in enumerate(day_bookings, start=1):
        if item.momence_booking_id == booking.momence_booking_id:
            return base_before_day + index
    return None


def _booking_milestone_label(client: Client, booking: Booking | None, now: datetime) -> str | None:
    class_number = _booking_class_number_today(client, booking, now)
    if class_number is None:
        return None
    if class_number in VISIT_MILESTONES:
        return f"{class_number}th class today"
    return None


def _ordinal_label(value: int | None) -> str | None:
    if value is None:
        return None
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _slug_client(client: Client) -> str:
    return client.momence_member_id


def _full_name(client: Client) -> str:
    return client.full_name or " ".join(part for part in [client.first_name, client.last_name] if part).strip() or client.momence_member_id


def _format_date_label(value: datetime | None) -> str:
    if value is None:
        return "Never"
    current = _as_utc(value)
    now = datetime.now(timezone.utc)
    delta = now - current
    days = max(delta.days, 0)
    if days == 0:
        hours = max(int(delta.total_seconds() // 3600), 0)
        return "Today" if hours < 6 else f"{hours}h ago"
    if days < 30:
        return f"{days}d ago"
    months = max(days // 30, 1)
    return f"{months}mo ago"


def _format_booking_label(value: datetime | None) -> str:
    if value is None:
        return "Recently active"
    current = _booking_as_local(value)
    return current.strftime("%b %-d · %-I:%M %p")


def _local_day_bounds(day: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(day, datetime.min.time(), tzinfo=LOCAL_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _resolve_demo_day(db: Session, requested_day: date | None) -> date:
    if requested_day is not None:
        return requested_day

    current_day = datetime.now(LOCAL_TZ).date()
    start_dt, end_dt = _local_day_bounds(current_day)
    has_current_day_bookings = db.scalar(
        select(func.count())
        .select_from(Booking)
        .where(Booking.starts_at >= start_dt, Booking.starts_at < end_dt)
    )
    if has_current_day_bookings:
        return current_day

    next_start = db.scalar(select(Booking.starts_at).where(Booking.starts_at >= start_dt).order_by(Booking.starts_at).limit(1))
    if next_start is not None:
        return _as_local(next_start).date()

    latest_start = db.scalar(select(Booking.starts_at).order_by(Booking.starts_at.desc()).limit(1))
    if latest_start is not None:
        return _as_local(latest_start).date()

    return current_day


def _prefer_official_bookings(bookings: list[Booking]) -> list[Booking]:
    if any(booking.ends_at is not None for booking in bookings):
        return [booking for booking in bookings if booking.ends_at is not None]
    return bookings


def _filter_relevant_bookings(bookings: list[Booking], day: date) -> list[Booking]:
    if not bookings:
        return bookings

    local_now = datetime.now(LOCAL_TZ)
    if day != local_now.date():
        return bookings

    relevant: list[Booking] = []
    for booking in bookings:
        starts_local = _booking_as_local(booking.starts_at)
        if starts_local is None:
            continue

        ends_local = _booking_as_local(booking.ends_at)
        if ends_local is None:
            ends_local = starts_local + timedelta(minutes=75)

        # Keep classes that are still in progress or about to happen.
        if ends_local >= local_now:
            relevant.append(booking)

    return relevant


def _churn_reason(client: Client, flags_summary) -> tuple[str, str]:
    activity = client.activity
    current_30, previous_30 = _canonical_visit_windows(client)
    if flags_summary.new_client:
        return (
            "New client with an early visit pattern still forming.",
            "New client baseline. Do not overreact until more booking history is established.",
        )
    if previous_30 > 0:
        if flags_summary.churn_risk == "high":
            return (
                f"High risk because the last 30 days dropped to {current_30} visits from {previous_30} in the previous 30 days.",
                "High risk when the last 30 days are 50% or less of the previous 30-day window.",
            )
        if flags_summary.churn_risk == "medium":
            return (
                f"Medium risk because the last 30 days are at {current_30} visits versus {previous_30} in the previous 30 days.",
                "Medium risk when the last 30 days fall below 80% of the previous 30-day window.",
            )
        return (
            f"Low risk because the last 30 days are holding at {current_30} visits versus {previous_30} in the previous 30 days.",
            "Low risk when the last 30 days are at least 80% of the previous 30-day window.",
        )
    if flags_summary.churn_risk == "high":
        last_seen = _format_date_label(activity.last_checkin_at if activity else None)
        return (
            f"High risk because engagement has dropped and the client has not checked in recently ({last_seen}).",
            "Temporary rule while live booking history is still being connected. Upgrade next to month-over-month booking decline.",
        )
    if flags_summary.churn_risk == "medium":
        return (
            "Medium risk because recent activity is softer than expected for an active client.",
            "Temporary rule while live booking history is still being connected. Upgrade next to month-over-month booking decline.",
        )
    return (
        "Low risk because recent activity is stable or a future booking is already on the calendar.",
        "Low risk when attendance is recent or a next booking exists.",
    )


def _profile_chips(client: Client, flags_summary) -> list[str]:
    chips: list[str] = []
    milestones = build_milestones(client, datetime.now(timezone.utc))
    display_milestone = next((item for item in milestones if item.type != "visit_count"), None)
    if display_milestone:
        chips.append(f"Celebrate {display_milestone.value or display_milestone.type}")
    if client.profile_data and client.profile_data.fun_fact:
        chips.append("Reference fun fact")
    if flags_summary.welcome_back:
        chips.append("Welcome back gently")
    if flags_summary.new_client:
        chips.append("Guide next booking")
    if client.profile_data and client.profile_data.pregnant_status:
        chips.append("Offer thoughtful modifications")
    if not chips:
        chips.append("Personal welcome")
    if len(chips) == 1:
        chips.append("Confirm how class felt")
    if len(chips) == 2:
        chips.append("Keep service personal")
    return chips[:3]


def _build_badges(client: Client, flags_summary) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    milestones = build_milestones(client, datetime.now(timezone.utc))
    display_milestone = next((item for item in milestones if item.type != "visit_count"), None)
    if display_milestone:
        badges.append({"label": display_milestone.value or display_milestone.type, "tone": "birthday"})
    if flags_summary.birthday_this_week:
        badges.append({"label": "Birthday week", "tone": "birthday"})
    if flags_summary.welcome_back:
        badges.append({"label": "Return moment", "tone": "info"})
    if flags_summary.new_client:
        badges.append({"label": "New client", "tone": "info"})
    if flags_summary.churn_risk == "high":
        badges.append({"label": "High risk", "tone": "risk"})
    elif flags_summary.churn_risk == "medium":
        badges.append({"label": "Medium risk", "tone": "risk"})
    elif flags_summary.churn_risk == "low":
        badges.append({"label": "Low risk", "tone": "positive"})
    return badges[:3]


def _profile_details(client: Client, churn_level: str, current_reason: str) -> list[dict[str, str]]:
    activity = client.activity
    profile_data = client.profile_data
    preferences = client.preferences
    favorite_instructors = (
        [item for item in (preferences.favorite_instructors or "").split("|") if item] if preferences else []
    )
    favorite_formats = [item for item in (preferences.favorite_formats or "").split("|") if item] if preferences else []
    lifetime_visits = _canonical_client_lifetime_visits(client)
    current_30, previous_30 = _canonical_visit_windows(client)
    active_membership_name = _active_membership_label(client)
    details: list[dict[str, str]] = [
        {"label": "Lifetime classes", "value": str(lifetime_visits)},
        {"label": "With us since", "value": _join_date_label(client)},
        {"label": "Last 30 days", "value": str(current_30)},
        {"label": "Previous 30 days", "value": str(previous_30)},
        {"label": "How heard about us", "value": profile_data.heard_about_us if profile_data and profile_data.heard_about_us else "Unknown"},
        {"label": "Fun fact", "value": profile_data.fun_fact if profile_data and profile_data.fun_fact else "Not collected yet"},
        {
            "label": "Preferred instructor",
            "value": ", ".join(favorite_instructors[:2]) if favorite_instructors else "Still learning",
        },
        {
            "label": "Preferred format",
            "value": ", ".join(favorite_formats[:2]) if favorite_formats else "Still learning",
        },
        {
            "label": "Membership",
            "value": active_membership_name or "No active membership",
        },
        {"label": "Churn risk", "value": f"{churn_level.title()} · {current_reason}"},
    ]
    if profile_data and profile_data.pregnant_status:
        details.insert(2, {"label": "Pregnant", "value": profile_data.pregnant_status})
    if profile_data and profile_data.pregnancy_due_date:
        details.insert(3, {"label": "Pregnancy due date", "value": profile_data.pregnancy_due_date.isoformat()})
    return details[:8]


def _history_bookings(client: Client, now: datetime) -> list[Booking]:
    history: list[Booking] = []
    for booking in client.bookings:
        starts_at = _as_utc(booking.starts_at)
        if starts_at is None or starts_at >= now:
            continue
        if (booking.status or "").lower() != "checked_in":
            continue
        history.append(booking)
    return history


def _top_count_lines(counter: Counter[str], *, limit: int = 3) -> list[str]:
    if not counter:
        return ["Still learning"]
    return [f"{label} ({count})" for label, count in counter.most_common(limit)]


def _normalize_preference_label(value: str | None) -> str | None:
    if not value:
        return None
    collapsed = " ".join(value.split()).strip()
    return collapsed or None


def _membership_fit_summary(client: Client) -> dict[str, str]:
    membership_name = _active_membership_label(client)
    if not membership_name:
        return {
            "title": "Membership fit",
            "value": "No active membership on file",
            "note": "Use recent attendance to suggest the best next option.",
        }

    visits_last_30, _ = _canonical_visit_windows(client)
    normalized = membership_name.lower()
    expiration = _membership_expiration_context(client)

    if "unlimited" in normalized:
        if visits_last_30 >= 8:
            note = f"{visits_last_30} visits in the last 30 days. Unlimited looks like a strong fit."
        elif visits_last_30 >= 4:
            note = f"{visits_last_30} visits in the last 30 days. Still reasonable, but worth monitoring."
        else:
            note = f"{visits_last_30} visits in the last 30 days. A smaller recurring option or pack may fit better."
        if expiration:
            note = f"{note} {expiration}."
        return {"title": membership_name, "value": "Current plan fit", "note": note}

    expected_classes = None
    patterns = [
        r"(\d+)\s*x\s*month",
        r"(\d+)\s*class\s*pack",
        r"(\d+)\s*class",
        r"single\s*class",
    ]
    for pattern in patterns:
        match = re.search(pattern, normalized)
        if not match:
            continue
        if pattern == r"single\s*class":
            expected_classes = 1
        else:
            expected_classes = int(match.group(1))
        break

    if expected_classes is not None:
        if visits_last_30 >= expected_classes:
            note = f"{visits_last_30} visits in the last 30 days against a {expected_classes}-class plan. They are at or above plan pace."
        elif visits_last_30 >= max(expected_classes - 2, 1):
            note = f"{visits_last_30} visits in the last 30 days against a {expected_classes}-class plan. This looks like a healthy fit."
        else:
            note = f"{visits_last_30} visits in the last 30 days against a {expected_classes}-class plan. They may be under-using this membership."
        if expiration:
            note = f"{note} {expiration}."
        return {"title": membership_name, "value": "Current plan fit", "note": note}

    if "pack" in normalized:
        if visits_last_30 >= 8:
            note = f"{visits_last_30} visits in the last 30 days. A monthly membership may be a better fit than a pack."
        else:
            note = f"{visits_last_30} visits in the last 30 days. Pack usage still looks reasonable."
        if expiration:
            note = f"{note} {expiration}."
        return {"title": membership_name, "value": "Current plan fit", "note": note}

    return {
        "title": membership_name,
        "value": "Current plan fit",
        "note": f"{visits_last_30} visits in the last 30 days. Good candidate for a manual membership-fit review." + (f" {expiration}." if expiration else ""),
    }


def _membership_history_lines(client: Client) -> list[str]:
    memberships = sorted(
        client.memberships,
        key=_membership_sort_key,
        reverse=True,
    )
    if not memberships:
        active_name = _active_membership_label(client)
        return [active_name] if active_name else ["No membership history loaded yet"]

    lines: list[str] = []
    for membership in memberships[:6]:
        label = membership.membership_name or "Membership"
        status = (membership.status or "unknown").replace("_", " ")
        dates = []
        if membership.started_at:
            dates.append(_booking_as_local(membership.started_at).strftime("%b %-d, %Y"))
        if membership.ended_at:
            dates.append(_booking_as_local(membership.ended_at).strftime("%b %-d, %Y"))
        date_label = " to ".join(dates) if dates else "Dates not available"
        lines.append(f"{label} · {status.title()} · {date_label}")
    return lines


def _visit_breakdowns(client: Client, now: datetime) -> list[dict[str, Any]]:
    history = _history_bookings(client, now)
    instructor_counts: Counter[str] = Counter()
    weekday_counts: Counter[str] = Counter()
    format_counts: Counter[str] = Counter()

    for booking in history:
        instructor_name = _normalize_preference_label(booking.instructor_name)
        class_name = _normalize_preference_label(booking.class_name)
        if instructor_name:
            instructor_counts[instructor_name] += 1
        if class_name:
            format_counts[class_name] += 1
        starts_local = _booking_as_local(booking.starts_at)
        if starts_local is not None:
            weekday_counts[starts_local.strftime("%A")] += 1

    membership_fit = _membership_fit_summary(client)
    return [
        {
            "title": "Visits by instructor",
            "items": _top_count_lines(instructor_counts),
        },
        {
            "title": "Visits by weekday",
            "items": _top_count_lines(weekday_counts),
        },
        {
            "title": "Visits by format",
            "items": _top_count_lines(format_counts),
        },
        {
            "title": "Membership history",
            "items": _membership_history_lines(client),
        },
        {
            "title": membership_fit["title"],
            "items": [membership_fit["value"], membership_fit["note"]],
        },
    ]


def _client_to_demo_person(client: Client) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    flags_summary = build_flag_summary(client, now)
    churn_reason, churn_rule = _churn_reason(client, flags_summary)
    chips = _profile_chips(client, flags_summary)
    notes = [note.note_text for note in client.notes[:3]] or ["Warm personal acknowledgment is the main service move here."]
    return {
        "id": _slug_client(client),
        "name": _full_name(client),
        "membership": _active_membership_label(client) or "No active membership",
        "funFact": client.profile_data.fun_fact if client.profile_data and client.profile_data.fun_fact else "No fun fact collected yet",
        "churnRisk": {
            "level": flags_summary.churn_risk or ("new" if flags_summary.new_client else "low"),
            "reason": churn_reason,
            "rule": churn_rule,
        },
        "profile": {
            "firstName": client.first_name or _full_name(client).split(" ")[0],
            "fullName": _full_name(client),
            "subtext": notes[0],
            "details": _profile_details(client, flags_summary.churn_risk or "new", churn_reason),
            "chips": chips,
            "notes": notes,
        },
    }


def _client_to_frontdesk_item(client: Client, booking: Booking | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    flags_summary = build_flag_summary(client, now)
    milestones = build_milestones(client, now)
    booking_milestone = _booking_milestone_label(client, booking, now)
    display_milestone = next((item for item in milestones if item.type != "visit_count"), None)
    class_number_today = _booking_class_number_today(client, booking, now)
    booking_time = booking.starts_at if booking is not None else (client.activity.next_booking_at if client.activity else None)
    arrival = _format_booking_label(booking_time)
    if arrival == "Recently active":
        arrival = f"Recently active · Last seen {_format_date_label(client.activity.last_checkin_at if client.activity else None)}"
    notes = []
    if booking and booking.class_name:
        notes.append(booking.class_name)
    if booking_milestone:
        notes.append(booking_milestone)
    elif class_number_today is not None:
        notes.append(f"{_ordinal_label(class_number_today)} class today")
    elif display_milestone:
        notes.append(display_milestone.value or display_milestone.type)
    if flags_summary.birthday_this_week:
        notes.append("Birthday this week")
    if flags_summary.welcome_back:
        notes.append("First visit back after a gap")
    if not notes:
        notes = ["Active client", "Warm check-in opportunity"]
    lifetime_visits = _canonical_client_lifetime_visits(client, now)
    current_30, previous_30 = _canonical_visit_windows(client, now)
    return {
        "id": _slug_client(client),
        "arrival": arrival,
        "location": booking.location_name if booking is not None else None,
        "bookingId": booking.momence_booking_id if booking is not None else None,
        "checkedIn": booking.status == "checked_in" if booking is not None else False,
        "notes": notes[:3],
        "metrics": [
            {"label": "Lifetime", "value": str(lifetime_visits)},
            {"label": "Last 30", "value": str(current_30)},
            {"label": "Prev 30", "value": str(previous_30)},
            {"label": "Last seen", "value": _format_date_label(client.activity.last_checkin_at if client.activity else None)},
        ],
        "badges": _build_badges(client, flags_summary),
    }


def _client_to_roster_item(client: Client, booking: Booking | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    flags_summary = build_flag_summary(client, now)
    milestones = build_milestones(client, now)
    booking_milestone = _booking_milestone_label(client, booking, now)
    class_number_today = _booking_class_number_today(client, booking, now)
    display_milestone = next((item for item in milestones if item.type != "visit_count"), None)
    visible_highlights: list[dict[str, str]] = []
    if booking_milestone:
        visible_highlights.append({"label": "Milestone", "value": booking_milestone})
    elif class_number_today == 1:
        visible_highlights.append({"label": "Today", "value": "1st class today. Make the first visit feel personal and calm."})
    elif display_milestone:
        visible_highlights.append({"label": "Milestone", "value": display_milestone.value or display_milestone.type})
    if flags_summary.welcome_back:
        visible_highlights.append({"label": "Return marker", "value": "Welcome-back moment after time away."})
    if flags_summary.birthday_this_week:
        visible_highlights.append({"label": "Birthday", "value": "Birthday this week. A small personal acknowledgment will land well."})
    if client.notes:
        visible_highlights.append({"label": "Assumption", "value": client.notes[0].note_text})
    if not visible_highlights:
        visible_highlights.append({"label": "Assumption", "value": "Warm, personal coaching is likely to be the highest-impact move."})
    favorite_time = client.preferences.favorite_time_of_day if client.preferences and client.preferences.favorite_time_of_day else "Still learning"
    favorite_instructors = (
        [item for item in (client.preferences.favorite_instructors or "").split("|") if item]
        if client.preferences
        else []
    )
    lifetime_visits = _canonical_client_lifetime_visits(client, now)
    current_30, previous_30 = _canonical_visit_windows(client, now)
    return {
        "personId": _slug_client(client),
        "bookingId": booking.momence_booking_id if booking is not None else None,
        "checkedIn": booking.status == "checked_in" if booking is not None else False,
        "badges": (
            [{"label": booking_milestone, "tone": "birthday"}] + _build_badges(client, flags_summary)
            if booking_milestone
            else _build_badges(client, flags_summary)
        )[:3],
        "visibleHighlights": visible_highlights[:2],
        "stats": [
            {"label": "Lifetime", "value": str(lifetime_visits)},
            {"label": "Last 30", "value": str(current_30)},
            {"label": "Prev 30", "value": str(previous_30)},
            {"label": "Risk", "value": (flags_summary.churn_risk or ("new" if flags_summary.new_client else "low")).title()},
        ],
        "expand": {
            "assumption": client.notes[0].note_text if client.notes else "Use concise encouragement and contextual warmth.",
            "service": _profile_chips(client, flags_summary)[0],
            "breakdowns": _visit_breakdowns(client, now),
            "notes": [
                item
                for item in [
                    f"Today's class number: {_ordinal_label(class_number_today)}"
                    if class_number_today is not None
                    else None,
                ]
                if item
            ]
            or ["Profile details will deepen as booking history is connected."],
        },
    }


def _roster_sort_key(client: Client, now: datetime) -> tuple[int, str]:
    flags_summary = build_flag_summary(client, now)
    has_milestone = any(item.type != "visit_count" for item in build_milestones(client, now))
    is_featured = (
        flags_summary.new_client
        or has_milestone
        or flags_summary.birthday_this_week
        or flags_summary.churn_risk == "high"
    )
    return (0 if is_featured else 1, _full_name(client).lower())


def build_demo_payload(db: Session, day: date | None = None) -> dict[str, Any]:
    current_day = _resolve_demo_day(db, day)
    now = datetime.now(timezone.utc)
    current_day_label = datetime.combine(current_day, datetime.min.time(), tzinfo=LOCAL_TZ).strftime("%A, %B %-d")

    active_stmt = (
        select(Client)
        .join(ClientFlag, ClientFlag.client_id == Client.id)
        .outerjoin(ClientActivity, ClientActivity.client_id == Client.id)
        .where(ClientFlag.is_active_180d.is_(True))
        .options(
            selectinload(Client.activity),
            selectinload(Client.notes),
            selectinload(Client.milestones),
            selectinload(Client.profile_data),
            selectinload(Client.preferences),
            selectinload(Client.memberships),
            selectinload(Client.bookings),
            selectinload(Client.flags),
        )
        .order_by(
            desc(ClientFlag.birthday_this_week),
            desc(ClientFlag.welcome_back_flag),
            desc(ClientFlag.new_client_flag),
            desc(
                case(
                    (ClientFlag.churn_risk == "high", 3),
                    (ClientFlag.churn_risk == "medium", 2),
                    (ClientFlag.churn_risk == "low", 1),
                    else_=0,
                )
            ),
            desc(func.coalesce(ClientActivity.total_visits, 0)),
        )
        .limit(18)
    )
    active_clients = db.scalars(active_stmt).all()

    people = {_slug_client(client): _client_to_demo_person(client) for client in active_clients}

    start_dt, end_dt = _local_day_bounds(current_day)
    bookings_stmt = (
        select(Booking)
        .where(Booking.starts_at >= start_dt, Booking.starts_at < end_dt)
        .order_by(Booking.starts_at, Booking.class_name)
    )
    bookings = db.scalars(bookings_stmt).all()
    bookings = _prefer_official_bookings(bookings)
    bookings = _filter_relevant_bookings(bookings, current_day)
    client_by_id = {client.id: client for client in active_clients}
    if bookings:
        missing_ids = {booking.client_id for booking in bookings if booking.client_id not in client_by_id}
        if missing_ids:
            extra_stmt = (
                select(Client)
                .where(Client.id.in_(missing_ids))
                .options(
                    selectinload(Client.activity),
                    selectinload(Client.notes),
                    selectinload(Client.milestones),
                    selectinload(Client.profile_data),
                    selectinload(Client.preferences),
                    selectinload(Client.memberships),
                    selectinload(Client.bookings),
                    selectinload(Client.flags),
                )
            )
            extra_clients = db.scalars(extra_stmt).all()
            for client in extra_clients:
                client_by_id[client.id] = client
                people[_slug_client(client)] = _client_to_demo_person(client)

    grouped_bookings: dict[str, list[Booking]] = defaultdict(list)
    for booking in bookings:
        grouped_bookings[booking.momence_session_id].append(booking)

    frontdesk_pairs: list[tuple[Client, Booking | None]] = []
    if bookings:
        seen_client_ids: set[str] = set()
        for booking in bookings:
            if booking.client_id in seen_client_ids:
                continue
            client = client_by_id.get(booking.client_id)
            if client is None:
                continue
            seen_client_ids.add(booking.client_id)
            frontdesk_pairs.append((client, booking))
            if len(frontdesk_pairs) >= 6:
                break
    if not frontdesk_pairs:
        frontdesk_pairs = [(client, None) for client in active_clients[:6]]
    frontdesk = [_client_to_frontdesk_item(client, booking) for client, booking in frontdesk_pairs]
    frontdesk_clients = [client for client, _ in frontdesk_pairs]

    sessions = []
    for session_id, session_bookings in grouped_bookings.items():
        first = session_bookings[0]
        ordered_bookings = sorted(
            session_bookings,
            key=lambda booking: _roster_sort_key(client_by_id.get(booking.client_id), now)
            if client_by_id.get(booking.client_id) is not None
            else (1, ""),
        )
        roster = []
        special_returns = 0
        birthdays = 0
        milestones_count = 0
        for booking in ordered_bookings:
            client = client_by_id.get(booking.client_id)
            if client is None:
                continue
            flags_summary = build_flag_summary(client, now)
            if flags_summary.welcome_back:
                special_returns += 1
            if flags_summary.birthday_this_week:
                birthdays += 1
            current_milestone = _booking_milestone_label(client, booking, now)
            if current_milestone:
                milestones_count += 1
            roster.append(_client_to_roster_item(client, booking))

        sessions.append(
            {
                "id": session_id,
                "title": first.class_name or "Session",
                "time": _booking_as_local(first.starts_at).strftime("%-I:%M %p"),
                "instructor": first.instructor_name or "TBD",
                "location": first.location_name or "Studio",
                "summary": [
                    {"label": "In class", "value": str(len(roster))},
                    {"label": "Milestones", "value": str(milestones_count)},
                    {"label": "Birthdays", "value": str(birthdays)},
                    {"label": "Special returns", "value": str(special_returns)},
                ],
                "highlights": [
                    item
                    for item in [
                        "Milestone moments" if milestones_count else None,
                        "Birthday week" if birthdays else None,
                        "Welcome-back clients" if special_returns else None,
                    ]
                    if item
                ],
                "roster": roster,
            }
        )

    birthdays_count = db.scalar(
        select(func.count()).select_from(ClientFlag).where(ClientFlag.is_active_180d.is_(True), ClientFlag.birthday_this_week.is_(True))
    ) or 0
    milestone_count = 0
    seen_milestone_clients: set[str] = set()
    for booking in bookings:
        client = client_by_id.get(booking.client_id)
        if client is None:
            continue
        if client.momence_member_id in seen_milestone_clients:
            continue
        if _booking_milestone_label(client, booking, now):
            milestone_count += 1
            seen_milestone_clients.add(client.momence_member_id)
    special_alerts = sum(
        1
        for client in frontdesk_clients
        if (client.flags and client.flags.churn_risk in {"high", "medium"}) or (client.flags and client.flags.welcome_back_flag)
    )

    celebration_clients = active_clients[:8]
    celebrations = []
    for client in celebration_clients:
        flags_summary = build_flag_summary(client, now)
        client_bookings = [booking for booking in bookings if booking.client_id == client.id]
        booking_milestone = None
        for booking in sorted(client_bookings, key=lambda item: _as_utc(item.starts_at) or now):
            booking_milestone = _booking_milestone_label(client, booking, now)
            if booking_milestone:
                break
        milestones = build_milestones(client, now)
        display_milestone = next((item for item in milestones if item.type != "visit_count"), None)
        if booking_milestone:
            celebrations.append(f"{_full_name(client)} · {booking_milestone}")
        elif display_milestone:
            celebrations.append(f"{_full_name(client)} · {display_milestone.value or display_milestone.type}")
        elif flags_summary.birthday_this_week:
            celebrations.append(f"{_full_name(client)} · Birthday this week")
        elif flags_summary.welcome_back:
            celebrations.append(f"{_full_name(client)} · First class back after baby")
    celebrations = celebrations[:6]

    selected_profile_id = frontdesk[0]["id"] if frontdesk else (next(iter(people.keys()), None))
    return {
        "meta": {
            "liveProfiles": True,
            "liveBookings": bool(bookings),
            "day": current_day.isoformat(),
            "dayLabel": current_day_label,
            "selectedProfileId": selected_profile_id,
            "selectedSessionId": sessions[0]["id"] if sessions else None,
        },
        "summary": [
            {"label": "Class live now", "value": len(sessions)},
            {"label": "People in room", "value": len(bookings)},
            {"label": "Milestones today", "value": milestone_count},
            {"label": "Special alerts", "value": special_alerts},
        ],
        "freshness": [
            {
                "domain": item["domain"].replace("_", " "),
                "status": item["status"],
                "note": "Updates at 10:00 AM" if item["domain"] == "bookings" and item["is_stale"] else (
                    _as_local(item["last_successful_at"]).strftime("updated at %-I:%M %p")
                    if item["last_successful_at"]
                    else "waiting for first sync"
                ),
            }
            for item in get_freshness_map(db, now).values()
        ],
        "celebrations": celebrations,
        "people": people,
        "frontdesk": frontdesk,
        "sessions": sessions,
    }
