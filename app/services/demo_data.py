from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import case, desc, func, select
from sqlalchemy.orm import Session, selectinload

from app.db.models import Booking, Client, ClientActivity, ClientFlag, Milestone
from app.services.domain import build_client_profile, build_flag_summary, build_milestones
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
    current_30 = activity.visits_last_30d if activity else 0
    previous_30 = activity.visits_previous_30d if activity else 0
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
    if milestones:
        chips.append(f"Celebrate {milestones[0].value or milestones[0].type}")
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
    if milestones:
        badges.append({"label": milestones[0].value or milestones[0].type, "tone": "birthday"})
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
    lifetime_visits = _canonical_lifetime_visits(activity)
    details: list[dict[str, str]] = [
        {"label": "Lifetime classes", "value": str(lifetime_visits)},
        {"label": "Last 30 days", "value": str(activity.visits_last_30d) if activity else "0"},
        {"label": "Previous 30 days", "value": str(activity.visits_previous_30d) if activity else "0"},
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
            "value": activity.active_membership_name if activity and activity.active_membership_name else "No active membership",
        },
        {"label": "Churn risk", "value": f"{churn_level.title()} · {current_reason}"},
    ]
    if profile_data and profile_data.pregnant_status:
        details.insert(2, {"label": "Pregnant", "value": profile_data.pregnant_status})
    if profile_data and profile_data.pregnancy_due_date:
        details.insert(3, {"label": "Pregnancy due date", "value": profile_data.pregnancy_due_date.isoformat()})
    return details[:8]


def _client_to_demo_person(client: Client) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    flags_summary = build_flag_summary(client, now)
    churn_reason, churn_rule = _churn_reason(client, flags_summary)
    chips = _profile_chips(client, flags_summary)
    notes = [note.note_text for note in client.notes[:3]] or ["Warm personal acknowledgment is the main service move here."]
    return {
        "id": _slug_client(client),
        "name": _full_name(client),
        "membership": client.activity.active_membership_name if client.activity and client.activity.active_membership_name else "No active membership",
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
    booking_time = booking.starts_at if booking is not None else (client.activity.next_booking_at if client.activity else None)
    arrival = _format_booking_label(booking_time)
    if arrival == "Recently active":
        arrival = f"Recently active · Last seen {_format_date_label(client.activity.last_checkin_at if client.activity else None)}"
    notes = []
    if booking and booking.class_name:
        notes.append(booking.class_name)
    if milestones:
        notes.append(milestones[0].value or milestones[0].type)
    if flags_summary.birthday_this_week:
        notes.append("Birthday this week")
    if flags_summary.welcome_back:
        notes.append("First visit back after a gap")
    if not notes:
        notes = ["Active client", "Warm check-in opportunity"]
    lifetime_visits = _canonical_lifetime_visits(client.activity)
    return {
        "id": _slug_client(client),
        "arrival": arrival,
        "location": booking.location_name if booking is not None else None,
        "bookingId": booking.momence_booking_id if booking is not None else None,
        "checkedIn": booking.status == "checked_in" if booking is not None else False,
        "notes": notes[:3],
        "metrics": [
            {"label": "Lifetime", "value": str(lifetime_visits)},
            {"label": "Last 30", "value": str(client.activity.visits_last_30d if client.activity else 0)},
            {"label": "Prev 30", "value": str(client.activity.visits_previous_30d if client.activity else 0)},
            {"label": "Last seen", "value": _format_date_label(client.activity.last_checkin_at if client.activity else None)},
        ],
        "badges": _build_badges(client, flags_summary),
    }


def _client_to_roster_item(client: Client, booking: Booking | None = None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    flags_summary = build_flag_summary(client, now)
    milestones = build_milestones(client, now)
    visible_highlights: list[dict[str, str]] = []
    if milestones:
        visible_highlights.append({"label": "Milestone", "value": milestones[0].value or milestones[0].type})
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
    lifetime_visits = _canonical_lifetime_visits(client.activity)
    return {
        "personId": _slug_client(client),
        "bookingId": booking.momence_booking_id if booking is not None else None,
        "checkedIn": booking.status == "checked_in" if booking is not None else False,
        "badges": _build_badges(client, flags_summary),
        "visibleHighlights": visible_highlights[:2],
        "stats": [
            {"label": "Lifetime", "value": str(lifetime_visits)},
            {"label": "Last 30", "value": str(client.activity.visits_last_30d if client.activity else 0)},
            {"label": "Prev 30", "value": str(client.activity.visits_previous_30d if client.activity else 0)},
            {"label": "Risk", "value": (flags_summary.churn_risk or ("new" if flags_summary.new_client else "low")).title()},
        ],
        "expand": {
            "assumption": client.notes[0].note_text if client.notes else "Use concise encouragement and contextual warmth.",
            "service": _profile_chips(client, flags_summary)[0],
            "notes": [
                item
                for item in [
                    f"How heard about us: {client.profile_data.heard_about_us}" if client.profile_data and client.profile_data.heard_about_us else None,
                    f"Fun fact: {client.profile_data.fun_fact}" if client.profile_data and client.profile_data.fun_fact else None,
                    f"Preferred instructor: {', '.join(favorite_instructors[:2])}"
                    if favorite_instructors
                    else None,
                ]
                if item
            ]
            or ["Profile details will deepen as booking history is connected."],
        },
    }


def _roster_sort_key(client: Client, now: datetime) -> tuple[int, str]:
    flags_summary = build_flag_summary(client, now)
    has_milestone = bool(build_milestones(client, now))
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
            current_milestones = build_milestones(client, now)
            if current_milestones:
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
    milestone_count = db.scalar(
        select(func.count()).select_from(Milestone).where(Milestone.is_current.is_(True))
    ) or 0
    special_alerts = sum(
        1
        for client in frontdesk_clients
        if (client.flags and client.flags.churn_risk in {"high", "medium"}) or (client.flags and client.flags.welcome_back_flag)
    )

    celebration_clients = active_clients[:8]
    celebrations = []
    for client in celebration_clients:
        flags_summary = build_flag_summary(client, now)
        milestones = build_milestones(client, now)
        if milestones:
            celebrations.append(f"{_full_name(client)} · {milestones[0].value or milestones[0].type}")
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
