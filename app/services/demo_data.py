from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import case, desc, func, or_, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import Booking, Client, ClientActivity, ClientFlag, Milestone
from app.services.client_intelligence import (
    VISIT_MILESTONES,
    as_utc as _as_utc,
    attended_bookings as _attended_bookings,
    booking_as_local as _booking_as_local,
    canonical_client_lifetime_visits as _canonical_client_lifetime_visits,
    canonical_visit_windows as _canonical_visit_windows,
    filter_relevant_bookings as _filter_relevant_bookings,
    normalize_format_label as _normalize_format_label,
    normalize_instructor_key as _normalize_instructor_key,
    normalize_text_label as _normalize_preference_label,
    prefer_official_bookings as _prefer_official_bookings,
)
from app.services.client_context import (
    active_membership_label as _active_membership_label,
    booking_milestone_label as _booking_milestone_label,
    build_badges as _build_badges,
    build_enriched_client_context,
    booking_snapshot,
    churn_reason as _churn_reason,
    join_date_label as _join_date_label,
    membership_expiration_context as _membership_expiration_context,
    membership_fit_summary as _membership_fit_summary,
    profile_chips as _profile_chips,
    visit_breakdowns as _visit_breakdowns,
)
from app.services.domain import (
    build_client_profile,
    build_flag_summary,
    build_milestones,
    get_front_desk_view,
    get_instructor_view,
)
from app.services.sync_state import get_freshness_map

LOCAL_TZ = ZoneInfo("America/New_York")


def _as_local(value: datetime | None) -> datetime | None:
    current = _as_utc(value)
    if current is None:
        return None
    return current.astimezone(LOCAL_TZ)


def _risk_rank(level: str | None) -> int:
    if level == "high":
        return 3
    if level == "medium":
        return 2
    if level == "low":
        return 1
    return 0

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


def _current_membership_record(client: Client):
    memberships = _active_memberships(client)
    if memberships:
        return memberships[0]
    if client.memberships:
        memberships = sorted(client.memberships, key=_membership_sort_key, reverse=True)
        return memberships[0]
    return None


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


def _resolve_demo_day(db: Session, requested_day: date | None, now_local: datetime | None = None) -> date:
    if requested_day is not None:
        return requested_day

    current_local = now_local or datetime.now(LOCAL_TZ)
    current_day = current_local.date()
    time_probe = current_local.timetz().replace(tzinfo=LOCAL_TZ)

    def day_bookings(day: date) -> list[Booking]:
        start_dt, end_dt = _local_day_bounds(day)
        return _prefer_official_bookings(
            db.scalars(
                select(Booking)
                .where(Booking.starts_at >= start_dt, Booking.starts_at < end_dt)
                .order_by(Booking.starts_at)
            ).all()
        )

    def has_remaining_bookings(day: date) -> bool:
        bookings = day_bookings(day)
        if not bookings:
            return False
        effective_now = datetime.combine(day, time_probe)
        return bool(_filter_relevant_bookings(bookings, day, now_local=effective_now))

    if has_remaining_bookings(current_day):
        return current_day

    next_start = db.scalar(select(Booking.starts_at).where(Booking.starts_at >= _local_day_bounds(current_day)[0]).order_by(Booking.starts_at).limit(1))
    if next_start is not None:
        candidate_day = _as_local(next_start).date()
        if has_remaining_bookings(candidate_day):
            return candidate_day
        candidate_end = _local_day_bounds(candidate_day)[1]
        following_start = db.scalar(
            select(Booking.starts_at)
            .where(Booking.starts_at >= candidate_end)
            .order_by(Booking.starts_at)
            .limit(1)
        )
        if following_start is not None:
            return _as_local(following_start).date()
        return candidate_day

    latest_start = db.scalar(select(Booking.starts_at).order_by(Booking.starts_at.desc()).limit(1))
    if latest_start is not None:
        return _as_local(latest_start).date()

    return current_day


def _profile_details(client: Client, churn_level: str, current_reason: str) -> list[dict[str, str]]:
    activity = client.activity
    profile_data = client.profile_data
    preferences = client.preferences
    favorite_instructors = (
        [item for item in (preferences.favorite_instructors or "").split("|") if item] if preferences else []
    )
    favorite_formats = [_normalize_format_label(item) for item in (preferences.favorite_formats or "").split("|")] if preferences else []
    favorite_formats = [item for item in favorite_formats if item]
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

def _celebration_spotlight(client: Client, booking: Booking | None, now: datetime) -> dict[str, str]:
    flags_summary = build_flag_summary(client, now)
    booking_milestone = _booking_milestone_label(client, booking, now)
    class_number_today = _booking_class_number_today(client, booking, now)
    current_lifetime = _canonical_client_lifetime_visits(client, now)

    if booking_milestone:
        return {
            "title": "Celebration",
            "value": booking_milestone,
            "note": "Call this out clearly and make sure the front desk team celebrates it.",
        }
    if flags_summary.birthday_this_week:
        return {
            "title": "Celebration",
            "value": "Birthday week",
            "note": "A warm birthday acknowledgment or small prize moment would land well.",
        }
    if class_number_today == 1:
        return {
            "title": "Celebration",
            "value": "1st class today",
            "note": "Treat this like a welcome moment and make the first visit feel calm, clear, and personal.",
        }
    if flags_summary.welcome_back:
        return {
            "title": "Celebration",
            "value": "Welcome-back visit",
            "note": "Acknowledge the return and make re-entry feel easy and encouraging.",
        }
    next_milestone = next((value for value in sorted(VISIT_MILESTONES) if value > current_lifetime), None)
    if next_milestone is not None:
        gap = next_milestone - current_lifetime
        if gap <= 3:
            return {
                "title": "Celebration",
                "value": f"{gap} away from {next_milestone}",
                "note": "Not a prize moment today, but this client is close enough to the next milestone that staff should keep it in mind.",
            }
    return {
        "title": "Celebration",
        "value": "No active celebration",
        "note": "Use the personal context and service cues instead of a prize or milestone callout.",
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


def _client_to_demo_person(client: Client) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    context = build_enriched_client_context(client, now)
    notes = [note.text for note in context.notes[:3]] or ["Warm personal acknowledgment is the main service move here."]
    return {
        "id": _slug_client(client),
        "name": context.full_name,
        "membership": context.active_membership_name or "No active membership",
        "funFact": context.profile_data.fun_fact or "No fun fact collected yet",
        "churnRisk": {
            "level": context.flags.churn_risk or ("new" if context.flags.new_client else "low"),
            "reason": context.churn_reason,
            "rule": context.churn_rule,
        },
        "profile": {
            "firstName": client.first_name or context.full_name.split(" ")[0],
            "fullName": context.full_name,
            "subtext": notes[0],
            "details": _profile_details(client, context.flags.churn_risk or "new", context.churn_reason),
            "chips": context.concierge_chips,
            "notes": notes,
        },
    }


def _client_to_frontdesk_item(client: Client, booking: Booking | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    context = build_enriched_client_context(client, now)
    snapshot = booking_snapshot(context, booking)
    display_milestone = next((item for item in context.milestones if item.type != "visit_count"), None)
    booking_time = booking.starts_at if booking is not None else (client.activity.next_booking_at if client.activity else None)
    arrival = _format_booking_label(booking_time)
    if arrival == "Recently active":
        arrival = f"Recently active · Last seen {_format_date_label(client.activity.last_checkin_at if client.activity else None)}"
    notes = []
    if booking and booking.class_name:
        notes.append(booking.class_name)
    if snapshot["booking_milestone"]:
        notes.append(snapshot["booking_milestone"])
    elif snapshot["class_number_label"] is not None:
        notes.append(f"{snapshot['class_number_label']} class today")
    elif display_milestone:
        notes.append(display_milestone.value or display_milestone.type)
    if context.flags.birthday_this_week:
        notes.append("Birthday this week")
    if context.flags.welcome_back:
        notes.append("First visit back after a gap")
    if not notes:
        notes = ["Active client", "Warm check-in opportunity"]
    return {
        "id": _slug_client(client),
        "arrival": arrival,
        "location": snapshot["location"],
        "bookingId": snapshot["booking_id"],
        "checkedIn": snapshot["checked_in"],
        "notes": notes[:3],
        "metrics": [
            {"label": "Lifetime", "value": str(context.activity.total_visits)},
            {"label": "Last 30", "value": str(context.activity.visits_last_30d)},
            {"label": "Prev 30", "value": str(context.activity.visits_previous_30d)},
            {"label": "Last seen", "value": _format_date_label(client.activity.last_checkin_at if client.activity else None)},
        ],
        "badges": _build_badges(client, context.flags, now),
    }


def _frontdesk_item_from_ops(arrival, client: Client | None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    flags_summary = build_flag_summary(client, now) if client else arrival.flags
    notes: list[str] = []
    if arrival.class_name:
        notes.append(arrival.class_name)
    if arrival.booking_milestone:
        notes.append(arrival.booking_milestone)
    elif arrival.class_number_label:
        notes.append(f"{arrival.class_number_label} class today")
    elif arrival.milestones:
        lead = arrival.milestones[0]
        notes.append(lead.value or lead.type)
    if arrival.flags.birthday_this_week:
        notes.append("Birthday this week")
    if arrival.flags.welcome_back:
        notes.append("First visit back after a gap")
    if not notes:
        notes = ["Active client", "Warm check-in opportunity"]
    return {
        "id": client.momence_member_id if client else arrival.member_id,
        "arrival": arrival.arrival_label or "Recently active",
        "location": arrival.location_name,
        "bookingId": arrival.booking_id,
        "checkedIn": arrival.checked_in,
        "notes": notes[:3],
        "metrics": [
            {"label": "Lifetime", "value": str(arrival.activity.total_visits)},
            {"label": "Last 30", "value": str(arrival.activity.visits_last_30d)},
            {"label": "Prev 30", "value": str(arrival.activity.visits_previous_30d)},
            {"label": "Last seen", "value": _format_date_label(arrival.activity.last_checkin_at)},
        ],
        "badges": _build_badges(client, flags_summary, now) if client else [],
    }


def _client_to_roster_item(client: Client, booking: Booking | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    context = build_enriched_client_context(client, now)
    snapshot = booking_snapshot(context, booking)
    display_milestone = next((item for item in context.milestones if item.type != "visit_count"), None)
    visible_highlights: list[dict[str, str]] = []
    if snapshot["booking_milestone"]:
        visible_highlights.append({"label": "Milestone", "value": snapshot["booking_milestone"]})
    elif snapshot["class_number_today"] == 1:
        visible_highlights.append({"label": "Today", "value": "1st class today. Make the first visit feel personal and calm."})
    elif display_milestone:
        visible_highlights.append({"label": "Milestone", "value": display_milestone.value or display_milestone.type})
    if context.flags.welcome_back:
        visible_highlights.append({"label": "Return marker", "value": "Welcome-back moment after time away."})
    if context.flags.birthday_this_week:
        visible_highlights.append({"label": "Birthday", "value": "Birthday this week. A small personal acknowledgment will land well."})
    if context.notes:
        visible_highlights.append({"label": "Assumption", "value": context.notes[0].text})
    if not visible_highlights:
        visible_highlights.append({"label": "Assumption", "value": "Warm, personal coaching is likely to be the highest-impact move."})
    return {
        "personId": _slug_client(client),
        "bookingId": snapshot["booking_id"],
        "checkedIn": snapshot["checked_in"],
        "classNumberToday": snapshot["class_number_today"],
        "bookingMilestone": snapshot["booking_milestone"],
        "birthdayToday": context.flags.birthday_today,
        "badges": (
            [{"label": snapshot["booking_milestone"], "tone": "birthday"}] + _build_badges(client, context.flags, now)
            if snapshot["booking_milestone"]
            else _build_badges(client, context.flags, now)
        )[:3],
        "visibleHighlights": visible_highlights[:2],
        "stats": [
            {"label": "Lifetime", "value": str(context.activity.total_visits)},
            {"label": "Last 30", "value": str(context.activity.visits_last_30d)},
            {"label": "Prev 30", "value": str(context.activity.visits_previous_30d)},
            {"label": "Risk", "value": (context.flags.churn_risk or ("new" if context.flags.new_client else "low")).title()},
        ],
        "expand": {
            "assumption": context.notes[0].text if context.notes else "Use concise encouragement and contextual warmth.",
            "service": context.concierge_chips[0],
            "celebrationSpotlight": _celebration_spotlight(client, booking, now),
            "membershipSpotlight": context.membership_spotlight,
            "breakdowns": _visit_breakdowns(client, now),
            "notes": [
                item
                for item in [
                    f"Today's class number: {snapshot['class_number_label']}"
                    if snapshot["class_number_label"] is not None
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


def _session_time_label(starts_at: datetime | None) -> str:
    if starts_at is None:
        return "TBD"
    now_utc = datetime.now(timezone.utc)
    starts_utc = _as_utc(starts_at)
    time_str = _booking_as_local(starts_at).strftime("%-I:%M %p")
    if starts_utc is None:
        return time_str
    ends_utc = starts_utc + timedelta(minutes=60)
    if starts_utc <= now_utc <= ends_utc:
        return f"Now · {time_str}"
    if starts_utc > now_utc:
        return f"Up next · {time_str}"
    return f"Earlier · {time_str}"


def _build_session_card(
    session_id: str,
    title: str | None,
    starts_at: datetime | None,
    instructor_name: str | None,
    location_name: str | None,
    roster: list[dict[str, Any]],
    *,
    birthdays: int,
    milestones_count: int,
    special_returns: int,
) -> dict[str, Any]:
    return {
        "id": session_id,
        "title": title or "Session",
        "time": _session_time_label(starts_at),
        "instructor": instructor_name or "TBD",
        "location": location_name or "Studio",
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


def build_demo_payload(db: Session, day: date | None = None) -> dict[str, Any]:
    current_day = _resolve_demo_day(db, day)
    now = datetime.now(timezone.utc)
    current_day_label = datetime.combine(current_day, datetime.min.time(), tzinfo=LOCAL_TZ).strftime("%A, %B %-d")
    front_desk_view = get_front_desk_view(db, current_day, None)
    instructor_view = get_instructor_view(db, current_day, None, None)

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
    missing_ids = {booking.client_id for booking in bookings if booking.client_id not in client_by_id}
    needed_member_ids = {
        arrival.member_id for arrival in front_desk_view.arrivals if arrival.member_id
    } | {
        roster_item.member_id
        for session_view in instructor_view.sessions
        for roster_item in session_view.roster
        if roster_item.member_id
    }
    missing_member_ids = {
        member_id
        for member_id in needed_member_ids
        if member_id not in {client.momence_member_id for client in client_by_id.values()}
    }
    if missing_ids or missing_member_ids:
        filters = []
        if missing_ids:
            filters.append(Client.id.in_(missing_ids))
        if missing_member_ids:
            filters.append(Client.momence_member_id.in_(missing_member_ids))
        extra_stmt = (
            select(Client)
            .where(or_(*filters))
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
    client_by_member_id = {client.momence_member_id: client for client in client_by_id.values()}
    frontdesk = [
        _frontdesk_item_from_ops(arrival, client_by_member_id.get(arrival.member_id))
        for arrival in front_desk_view.arrivals[:6]
    ]
    if not frontdesk:
        frontdesk = [_client_to_frontdesk_item(client, None) for client in active_clients[:6]]

    sessions = []
    for session_view in instructor_view.sessions:
        session_bookings = grouped_bookings.get(session_view.session_id, [])
        bookings_by_id = {booking.momence_booking_id: booking for booking in session_bookings}
        bookings_by_member = {
            client_by_id[booking.client_id].momence_member_id: booking
            for booking in session_bookings
            if booking.client_id in client_by_id
        }
        roster = []
        special_returns = 0
        birthdays = 0
        milestones_count = 0
        for roster_item in session_view.roster:
            client = client_by_member_id.get(roster_item.member_id)
            if client is None:
                continue
            booking = bookings_by_id.get(roster_item.booking_id) if getattr(roster_item, "booking_id", None) else None
            if booking is None:
                booking = bookings_by_member.get(roster_item.member_id)
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
            _build_session_card(
                session_view.session_id,
                session_view.class_name,
                session_view.starts_at,
                session_view.instructor_name,
                session_view.location_name,
                roster,
                birthdays=birthdays,
                milestones_count=milestones_count,
                special_returns=special_returns,
            )
        )

    if not sessions and grouped_bookings:
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
                _build_session_card(
                    session_id,
                    first.class_name,
                    first.starts_at,
                    first.instructor_name,
                    first.location_name,
                    roster,
                    birthdays=birthdays,
                    milestones_count=milestones_count,
                    special_returns=special_returns,
                )
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
    checked_in_count = len(
        {
            booking.client_id
            for booking in bookings
            if booking.status == "checked_in"
        }
    )
    new_clients_today_count = len(
        {
            booking.client_id
            for booking in bookings
            if (client := client_by_id.get(booking.client_id)) is not None
            and _booking_class_number_today(client, booking, now) == 1
        }
    )
    birthdays_today_count = len(
        {
            booking.client_id
            for booking in bookings
            if (client := client_by_id.get(booking.client_id)) is not None
            and build_flag_summary(client, now).birthday_today
        }
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
            celebrations.append(f"{_full_name(client)} · First visit back after a gap")
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
            {"label": "People checked in", "value": checked_in_count},
            {"label": "New clients today", "value": new_clients_today_count},
            {"label": "Milestones today", "value": milestone_count},
            {"label": "Birthdays today", "value": birthdays_today_count},
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
