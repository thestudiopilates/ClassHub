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


def compute_birthday_flags(birthday: date | None, day: date) -> tuple[bool, bool]:
    if birthday is None:
        return False, False
    today_match = (birthday.month, birthday.day) == (day.month, day.day)
    week_end = day + timedelta(days=7)
    def birthday_for_year(year: int) -> date:
        try:
            return date(year, birthday.month, birthday.day)
        except ValueError:
            if birthday.month == 2 and birthday.day == 29:
                return date(year, 2, 28)
            raise

    this_year = birthday_for_year(day.year)
    next_date = this_year if this_year >= day else birthday_for_year(day.year + 1)
    return today_match, day <= next_date <= week_end


def compute_churn_risk(activity: ClientActivity | None, now: datetime) -> str | None:
    if activity is None or not activity.has_active_membership:
        return None
    next_booking_at = _as_utc(activity.next_booking_at)
    if next_booking_at and next_booking_at >= now:
        return "low"
    current_30 = activity.visits_last_30d or 0
    previous_30 = activity.visits_previous_30d or 0
    if previous_30 > 0:
        change_ratio = current_30 / previous_30
        if change_ratio <= 0.5:
            return "high"
        if change_ratio < 0.8:
            return "medium"
        return "low"
    reference = _as_utc(activity.last_checkin_at) or _as_utc(activity.last_booking_at)
    if reference is None:
        return "high"
    days_since = (now - reference).days
    if days_since >= 21:
        return "high"
    if days_since >= 14:
        return "medium"
    return "low"


def build_flag_summary(client: Client, now: datetime) -> FlagsSummary:
    today = now.date()
    birthday_today, birthday_this_week = compute_birthday_flags(client.birthday, today)
    activity = client.activity
    injury = any(note.is_injury_flag for note in client.notes)
    total_visits, current_30, previous_30 = _visit_counts(client, now)
    new_client = bool(activity and total_visits <= 1)
    welcome_back = False
    last_checkin_at = _as_utc(activity.last_checkin_at) if activity else None
    next_booking_at = _as_utc(activity.next_booking_at) if activity else None
    if last_checkin_at and next_booking_at:
        welcome_back = (next_booking_at - last_checkin_at).days >= 30

    return FlagsSummary(
        birthday_today=birthday_today,
        birthday_this_week=birthday_this_week,
        injury=injury,
        welcome_back=welcome_back,
        new_client=new_client,
        churn_risk=(
            "low"
            if activity and (_as_utc(activity.next_booking_at) and _as_utc(activity.next_booking_at) >= now)
            else (
                "high"
                if previous_30 > 0 and current_30 / previous_30 <= 0.5
                else "medium"
                if previous_30 > 0 and current_30 / previous_30 < 0.8
                else compute_churn_risk(activity, now)
            )
        ),
    )


def build_membership_summary(client: Client) -> MembershipSummary:
    activity = client.activity
    return MembershipSummary(
        active=bool(activity and activity.has_active_membership),
        name=activity.active_membership_name if activity else None,
    )


def build_activity_summary(client: Client) -> ActivitySummary:
    activity = client.activity
    if activity is None and not getattr(client, "bookings", None):
        return ActivitySummary()
    now = datetime.now(timezone.utc)
    total_visits, current_30, previous_30 = _visit_counts(client, now)
    return ActivitySummary(
        last_checkin_at=activity.last_checkin_at if activity else None,
        last_booking_at=activity.last_booking_at if activity else None,
        next_booking_at=activity.next_booking_at if activity else None,
        total_visits=total_visits,
        lifetime_visits=total_visits,
        visits_last_30d=current_30,
        visits_previous_30d=previous_30,
    )


def build_milestones(client: Client, now: datetime) -> list[MilestoneSummary]:
    milestones: list[MilestoneSummary] = [
        MilestoneSummary(type=item.milestone_type, value=item.milestone_value, date=item.milestone_date)
        for item in client.milestones
        if item.is_current
    ]
    total_visits, _, _ = _visit_counts(client, now)
    if total_visits in VISIT_MILESTONES:
        milestones.append(
            MilestoneSummary(
                type="visit_count",
                value=f"{total_visits}th class",
                date=now.date(),
            )
        )
    return milestones


def build_notes(client: Client) -> list[NoteSummary]:
    return [NoteSummary(type=note.note_type, text=note.note_text) for note in client.notes]


def build_profile_data_summary(client: Client) -> ProfileDataSummary:
    profile_data = client.profile_data
    if profile_data is None:
        return ProfileDataSummary()
    return ProfileDataSummary(
        fun_fact=profile_data.fun_fact,
        pregnant_status=profile_data.pregnant_status,
        pregnancy_due_date=profile_data.pregnancy_due_date,
        heard_about_us=profile_data.heard_about_us,
    )


def build_preferences_summary(client: Client) -> PreferencesSummary:
    preferences = client.preferences
    if preferences is None:
        return PreferencesSummary()
    return PreferencesSummary(
        favorite_time_of_day=preferences.favorite_time_of_day,
        favorite_weekdays=[item for item in (preferences.favorite_weekdays or "").split("|") if item],
        favorite_instructors=[
            item for raw in (preferences.favorite_instructors or "").split("|") if (item := normalize_text_label(raw))
        ],
        favorite_formats=[
            item for raw in (preferences.favorite_formats or "").split("|") if (item := normalize_format_label(raw))
        ],
        preference_basis=preferences.preference_basis,
    )


def build_client_profile(client: Client, now: datetime) -> ClientProfileResponse:
    full_name = client.full_name or " ".join(part for part in [client.first_name, client.last_name] if part).strip()
    return ClientProfileResponse(
        member_id=client.momence_member_id,
        first_name=client.first_name,
        last_name=client.last_name,
        full_name=full_name,
        email=client.email,
        phone=client.phone,
        birthday=client.birthday,
        membership=build_membership_summary(client),
        activity=build_activity_summary(client),
        flags=build_flag_summary(client, now),
        profile_data=build_profile_data_summary(client),
        preferences=build_preferences_summary(client),
        milestones=build_milestones(client, now),
        notes=build_notes(client),
        freshness={},
    )


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

    arrivals = [
        ClientListItem(
            member_id=client.momence_member_id,
            name=client.full_name or " ".join(filter(None, [client.first_name, client.last_name])),
            membership=build_membership_summary(client),
            activity=build_activity_summary(client),
            flags=build_flag_summary(client, now),
            profile_data=build_profile_data_summary(client),
            preferences=build_preferences_summary(client),
            milestones=build_milestones(client, now),
            notes=build_notes(client),
        )
        for booking in bookings
        if (client := clients.get(booking.client_id)) is not None
    ]
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
    bookings = _prefer_official_bookings(db.scalars(stmt.order_by(Booking.starts_at)).all())

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
            flags = build_flag_summary(client, now)
            roster.append(
                SessionRosterItem(
                    member_id=client.momence_member_id,
                    name=client.full_name or " ".join(filter(None, [client.first_name, client.last_name])),
                    birthday_this_week=flags.birthday_this_week,
                    milestones=[m.value or m.type for m in build_milestones(client, now)],
                    injury_flag=flags.injury,
                    instructor_notes=[note.note_text for note in client.notes if note.is_instructor_flag or note.is_injury_flag],
                    new_client=flags.new_client,
                    welcome_back=flags.welcome_back,
                    total_visits=build_activity_summary(client).total_visits or 0,
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
