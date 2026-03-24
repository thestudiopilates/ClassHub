from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.models import Booking, Client, ClientActivity, ClientFlag, Milestone
from app.schemas import (
    ActivitySummary,
    ClientListItem,
    ClientProfileResponse,
    FlagsSummary,
    FrontDeskResponse,
    InstructorResponse,
    MembershipSummary,
    MilestoneSummary,
    NoteSummary,
    PreferencesSummary,
    ProfileDataSummary,
    SessionRosterItem,
    SessionView,
    WeekAheadResponse,
)
from app.services.client_intelligence import (
    VISIT_MILESTONES,
    as_utc as _as_utc,
    canonical_client_lifetime_visits,
    canonical_visit_windows,
    filter_relevant_bookings,
    normalize_format_label,
    normalize_text_label,
    prefer_official_bookings,
)
from app.services.client_context import (
    celebration_spotlight,
    build_activity_summary,
    build_badges,
    build_flag_summary,
    build_milestones,
    build_notes,
    build_preferences_summary,
    build_profile_data_summary,
    build_profile_response_from_context,
    build_enriched_client_context,
    booking_snapshot,
)
from app.services.sync_state import get_freshness_map

LOCAL_TZ = ZoneInfo("America/New_York")


def _local_day_bounds(day: date) -> tuple[datetime, datetime]:
    start_local = datetime.combine(day, datetime.min.time(), tzinfo=LOCAL_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def _visit_counts(client: Client, now: datetime) -> tuple[int, int, int]:
    lifetime = canonical_client_lifetime_visits(client, now)
    current, previous = canonical_visit_windows(client, now)
    return lifetime, current, previous


def compute_is_active_180d(activity: ClientActivity | None, now: datetime) -> bool:
    if activity is None:
        return False
    cutoff = now - timedelta(days=180)
    activity_dates = [
        _as_utc(activity.last_checkin_at),
        _as_utc(activity.last_booking_at),
        _as_utc(activity.last_purchase_at),
    ]
    return any(dt is not None and dt >= cutoff for dt in activity_dates) or bool(
        activity.has_active_membership or (_as_utc(activity.next_booking_at) and _as_utc(activity.next_booking_at) >= now)
    )


def build_client_profile(client: Client, now: datetime) -> ClientProfileResponse:
    return build_profile_response_from_context(build_enriched_client_context(client, now))


def get_client_profile(db: Session, momence_member_id: str) -> ClientProfileResponse | None:
    stmt = (
        select(Client)
        .where(Client.momence_member_id == momence_member_id)
        .options(
            selectinload(Client.activity),
            selectinload(Client.notes),
            selectinload(Client.milestones),
            selectinload(Client.bookings),
            selectinload(Client.profile_data),
            selectinload(Client.preferences),
        )
    )
    client = db.scalar(stmt)
    if client is None:
        return None
    now = datetime.now(timezone.utc)
    profile = build_client_profile(client, now)
    profile.freshness = get_freshness_map(db, now)
    return profile


def _get_clients_for_bookings(db: Session, booking_ids: list[str]) -> dict[str, Client]:
    if not booking_ids:
        return {}
    stmt = (
        select(Client)
        .join(Booking, Booking.client_id == Client.id)
        .where(Booking.momence_booking_id.in_(booking_ids))
        .options(
            selectinload(Client.activity),
            selectinload(Client.notes),
            selectinload(Client.milestones),
        )
    )
    clients = db.scalars(stmt).all()
    return {client.id.hex: client for client in clients}


def get_front_desk_view(db: Session, day: date, location_name: str | None) -> FrontDeskResponse:
    start_dt, end_dt = _local_day_bounds(day)
    stmt = select(Booking).where(Booking.starts_at >= start_dt, Booking.starts_at < end_dt)
    if location_name:
        stmt = stmt.where(Booking.location_name == location_name)
    bookings = prefer_official_bookings(db.scalars(stmt.order_by(Booking.starts_at)).all())
    bookings = filter_relevant_bookings(bookings, day)

    client_ids = {booking.client_id for booking in bookings}
    clients_stmt = (
        select(Client)
        .where(Client.id.in_(client_ids))
        .options(
            selectinload(Client.activity),
            selectinload(Client.notes),
            selectinload(Client.milestones),
            selectinload(Client.bookings),
        )
    )
    clients = {client.id: client for client in db.scalars(clients_stmt).all()}
    now = datetime.now(timezone.utc)

    arrivals = []
    for booking in bookings:
        client = clients.get(booking.client_id)
        if client is None:
            continue
        context = build_enriched_client_context(client, now)
        arrivals.append(
            ClientListItem(
                member_id=context.client.momence_member_id,
                name=context.full_name,
                arrival_label=(_as_utc(booking.starts_at).astimezone(LOCAL_TZ).strftime("%b %-d · %-I:%M %p") if _as_utc(booking.starts_at) else None),
                location_name=booking.location_name,
                class_name=booking.class_name,
                booking_id=booking.momence_booking_id,
                checked_in=booking.status == "checked_in",
                membership=context.membership,
                activity=context.activity,
                flags=context.flags,
                profile_data=context.profile_data,
                preferences=context.preferences,
                milestones=context.milestones,
                notes=context.notes,
                join_date_label=context.join_date_label,
                class_number_today=booking_snapshot(context, booking)["class_number_today"],
                class_number_label=booking_snapshot(context, booking)["class_number_label"],
                booking_milestone=booking_snapshot(context, booking)["booking_milestone"],
                churn_reason=context.churn_reason,
                membership_spotlight=context.membership_spotlight,
                celebration_spotlight=celebration_spotlight(context.client, booking, now),
            )
        )
    return FrontDeskResponse(date=day, arrivals=arrivals, freshness=get_freshness_map(db, now))


def get_instructor_view(
    db: Session, day: date, session_id: str | None, instructor_name: str | None
) -> InstructorResponse:
    start_dt, end_dt = _local_day_bounds(day)
    stmt = select(Booking).where(Booking.starts_at >= start_dt, Booking.starts_at < end_dt)
    if session_id:
        stmt = stmt.where(Booking.momence_session_id == session_id)
    if instructor_name:
        stmt = stmt.where(Booking.instructor_name == instructor_name)
    bookings = prefer_official_bookings(db.scalars(stmt.order_by(Booking.starts_at)).all())

    client_ids = {booking.client_id for booking in bookings}
    clients_stmt = (
        select(Client)
        .where(Client.id.in_(client_ids))
        .options(
            selectinload(Client.activity),
            selectinload(Client.notes),
            selectinload(Client.milestones),
            selectinload(Client.bookings),
        )
    )
    clients = {client.id: client for client in db.scalars(clients_stmt).all()}
    now = datetime.now(timezone.utc)

    grouped: dict[str, list[Booking]] = defaultdict(list)
    for booking in bookings:
        grouped[booking.momence_session_id].append(booking)

    sessions: list[SessionView] = []
    for current_session_id, session_bookings in grouped.items():
        first = session_bookings[0]
        roster = []
        for booking in session_bookings:
            client = clients.get(booking.client_id)
            if client is None:
                continue
            context = build_enriched_client_context(client, now)
            snapshot = booking_snapshot(context, booking)
            roster.append(
                SessionRosterItem(
                    member_id=context.client.momence_member_id,
                    name=context.full_name,
                    booking_id=booking.momence_booking_id,
                    checked_in=booking.status == "checked_in",
                    birthday_this_week=context.flags.birthday_this_week,
                    milestones=[m.value or m.type for m in context.milestones],
                    injury_flag=context.flags.injury,
                    instructor_notes=[note.text for note in context.notes if note.type in {"instructor", "injury"}]
                    or [note.note_text for note in client.notes if note.is_instructor_flag or note.is_injury_flag],
                    new_client=context.flags.new_client,
                    welcome_back=context.flags.welcome_back,
                    total_visits=context.activity.total_visits or 0,
                    fun_fact=context.profile_data.fun_fact,
                    class_number_today=snapshot["class_number_today"],
                    class_number_label=snapshot["class_number_label"],
                    booking_milestone=snapshot["booking_milestone"],
                    churn_risk=context.flags.churn_risk,
                    membership_name=context.active_membership_name,
                    membership_spotlight=context.membership_spotlight,
                    celebration_spotlight=celebration_spotlight(context.client, booking, now),
                )
            )
        sessions.append(
            SessionView(
                session_id=current_session_id,
                class_name=first.class_name,
                starts_at=first.starts_at,
                location_name=first.location_name,
                instructor_name=first.instructor_name,
                roster=roster,
            )
        )
    return InstructorResponse(date=day, sessions=sessions, freshness=get_freshness_map(db, now))


def get_week_ahead_view(db: Session, start: date, days: int) -> WeekAheadResponse:
    sessions = []
    birthdays: list[SessionRosterItem] = []
    milestone_people: list[SessionRosterItem] = []
    now = datetime.now(timezone.utc)
    for offset in range(days):
        view = get_instructor_view(db, start + timedelta(days=offset), None, None)
        sessions.extend(view.sessions)
        for session in view.sessions:
            for roster_item in session.roster:
                if roster_item.birthday_this_week:
                    birthdays.append(roster_item)
                if roster_item.milestones:
                    milestone_people.append(roster_item)
    return WeekAheadResponse(
        start=start,
        end=start + timedelta(days=days - 1),
        sessions=sessions,
        birthdays=birthdays,
        milestones=milestone_people,
        freshness=get_freshness_map(db, now),
    )


def recompute_client_flags(db: Session, client: Client, now: datetime) -> None:
    summary = build_flag_summary(client, now)
    existing = client.flags or ClientFlag(client_id=client.id)
    existing.is_active_180d = compute_is_active_180d(client.activity, now)
    existing.birthday_today = summary.birthday_today
    existing.birthday_this_week = summary.birthday_this_week
    existing.churn_risk = summary.churn_risk
    existing.new_client_flag = summary.new_client
    existing.welcome_back_flag = summary.welcome_back
    existing.injury_flag = summary.injury
    existing.computed_at = now
    db.add(existing)


def refresh_all_flags(db: Session) -> int:
    stmt = select(Client).options(selectinload(Client.activity), selectinload(Client.notes), selectinload(Client.flags))
    clients = db.scalars(stmt).all()
    now = datetime.now(timezone.utc)
    recompute_visit_window_counts(db, now)
    for client in clients:
        recompute_client_flags(db, client, now)
    db.commit()
    return len(clients)


def recompute_visit_window_counts(db: Session, now: datetime) -> None:
    current_start = now - timedelta(days=30)
    previous_start = now - timedelta(days=60)
    bookings_stmt = select(Booking).where(
        Booking.starts_at >= previous_start,
        Booking.starts_at < now,
        Booking.status == "checked_in",
    )
    bookings = db.scalars(bookings_stmt).all()
    counts: dict[object, dict[str, int]] = defaultdict(lambda: {"current": 0, "previous": 0})

    for booking in bookings:
        starts_at = _as_utc(booking.starts_at)
        if starts_at is None:
            continue
        if starts_at >= current_start:
            counts[booking.client_id]["current"] += 1
        elif starts_at >= previous_start:
            counts[booking.client_id]["previous"] += 1

    activities = db.scalars(select(ClientActivity)).all()
    for activity in activities:
        client_counts = counts.get(activity.client_id, {"current": 0, "previous": 0})
        # Preserve stronger imported rollups when hosted booking history is still partial.
        activity.visits_last_30d = max(activity.visits_last_30d or 0, client_counts["current"])
        activity.visits_previous_30d = max(activity.visits_previous_30d or 0, client_counts["previous"])
        db.add(activity)
