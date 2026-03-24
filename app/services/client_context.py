from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.db.models import Booking, Client
from app.schemas import (
    ActivitySummary,
    ClientProfileResponse,
    FlagsSummary,
    MembershipSummary,
    MilestoneSummary,
    NoteSummary,
    PreferencesSummary,
    ProfileDataSummary,
)
from app.services.client_intelligence import (
    VISIT_MILESTONES,
    as_utc,
    attended_bookings,
    booking_as_local,
    canonical_client_lifetime_visits,
    canonical_visit_windows,
    normalize_format_label,
    normalize_instructor_key,
    normalize_text_label,
)


def _ordinal_label(value: int | None) -> str | None:
    if value is None:
        return None
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _format_date_label(value: datetime | None) -> str:
    if value is None:
        return "Never"
    current = as_utc(value)
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


def compute_birthday_flags(birthday, day) -> tuple[bool, bool]:
    if birthday is None:
        return False, False
    from datetime import date, timedelta

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


def compute_churn_risk(client: Client, now: datetime) -> str | None:
    activity = client.activity
    if activity is None or not activity.has_active_membership:
        return None
    next_booking_at = as_utc(activity.next_booking_at)
    if next_booking_at and next_booking_at >= now:
        return "low"
    current_30, previous_30 = canonical_visit_windows(client, now)
    if previous_30 > 0:
        change_ratio = current_30 / previous_30
        if change_ratio <= 0.5:
            return "high"
        if change_ratio < 0.8:
            return "medium"
        return "low"
    reference = as_utc(activity.last_checkin_at) or as_utc(activity.last_booking_at)
    if reference is None:
        return "high"
    days_since = (now - reference).days
    if days_since >= 21:
        return "high"
    if days_since >= 14:
        return "medium"
    return "low"


def _membership_sort_key(value) -> datetime:
    fallback = datetime.min.replace(tzinfo=timezone.utc)
    candidate = value.started_at or value.ended_at or value.source_updated_at
    if candidate is None:
        return fallback
    current = as_utc(candidate)
    return current or fallback


def _active_memberships(client: Client) -> list:
    now = datetime.now(timezone.utc)
    active: list = []
    for membership in client.memberships:
        status = (membership.status or "").lower()
        ended_at = as_utc(membership.ended_at)
        if status == "active" or ended_at is None or ended_at >= now:
            active.append(membership)
    active.sort(key=_membership_sort_key, reverse=True)
    return active


def active_membership_label(client: Client) -> str | None:
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


def build_membership_summary(client: Client) -> MembershipSummary:
    activity = client.activity
    return MembershipSummary(
        active=bool(activity and activity.has_active_membership),
        name=active_membership_label(client) or (activity.active_membership_name if activity else None),
    )


def build_activity_summary(client: Client, now: datetime | None = None) -> ActivitySummary:
    activity = client.activity
    if activity is None and not getattr(client, "bookings", None):
        return ActivitySummary()
    current = now or datetime.now(timezone.utc)
    total_visits = canonical_client_lifetime_visits(client, current)
    current_30, previous_30 = canonical_visit_windows(client, current)
    return ActivitySummary(
        last_checkin_at=activity.last_checkin_at if activity else None,
        last_booking_at=activity.last_booking_at if activity else None,
        next_booking_at=activity.next_booking_at if activity else None,
        total_visits=total_visits,
        lifetime_visits=total_visits,
        visits_last_30d=current_30,
        visits_previous_30d=previous_30,
    )


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


def build_notes(client: Client) -> list[NoteSummary]:
    return [NoteSummary(type=note.note_type, text=note.note_text) for note in client.notes]


def build_milestones(client: Client, now: datetime) -> list[MilestoneSummary]:
    milestones: list[MilestoneSummary] = [
        MilestoneSummary(type=item.milestone_type, value=item.milestone_value, date=item.milestone_date)
        for item in client.milestones
        if item.is_current
    ]
    total_visits = canonical_client_lifetime_visits(client, now)
    if total_visits in VISIT_MILESTONES:
        milestones.append(
            MilestoneSummary(
                type="visit_count",
                value=f"{total_visits}th class",
                date=now.date(),
            )
        )
    return milestones


def build_flag_summary(client: Client, now: datetime) -> FlagsSummary:
    today = now.date()
    birthday_today, birthday_this_week = compute_birthday_flags(client.birthday, today)
    activity = client.activity
    injury = any(note.is_injury_flag for note in client.notes)
    total_visits = canonical_client_lifetime_visits(client, now)
    current_30, previous_30 = canonical_visit_windows(client, now)
    new_client = bool(activity and total_visits <= 1)
    welcome_back = False
    last_checkin_at = as_utc(activity.last_checkin_at) if activity else None
    next_booking_at = as_utc(activity.next_booking_at) if activity else None
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
            if activity and (as_utc(activity.next_booking_at) and as_utc(activity.next_booking_at) >= now)
            else (
                "high"
                if previous_30 > 0 and current_30 / previous_30 <= 0.5
                else "medium"
                if previous_30 > 0 and current_30 / previous_30 < 0.8
                else compute_churn_risk(client, now)
            )
        ),
    )


def current_membership_record(client: Client):
    memberships = _active_memberships(client)
    if memberships:
        return memberships[0]
    if client.memberships:
        memberships = sorted(client.memberships, key=_membership_sort_key, reverse=True)
        return memberships[0]
    return None


def membership_expiration_context(client: Client) -> str | None:
    membership = current_membership_record(client)
    if membership is None:
        return None
    bits: list[str] = []
    if membership.is_frozen:
        bits.append("Frozen")
    if membership.classes_left is not None:
        bits.append(f"{membership.classes_left} classes left")
    if membership.ended_at is not None:
        end_label = booking_as_local(membership.ended_at).strftime("%b %-d")
        if membership.renewal_cancelled:
            bits.append(f"Ends {end_label}")
        else:
            name = (membership.membership_name or "").lower()
            if "pack" in name or "credit" in name or membership.classes_left is not None:
                bits.append(f"Expires {end_label}")
            else:
                bits.append(f"Through {end_label}")
    return " · ".join(bits) if bits else None


def join_date_label(client: Client) -> str:
    attended = attended_bookings(client)
    if attended:
        return booking_as_local(attended[0].starts_at).strftime("%b %-d, %Y")
    activity = client.activity
    if activity is None or activity.first_visit_at is None:
        return "Join date still loading"
    return booking_as_local(activity.first_visit_at).strftime("%b %-d, %Y")


def booking_class_number_today(client: Client, booking: Booking | None, now: datetime) -> int | None:
    if booking is None:
        return None
    base_lifetime = canonical_client_lifetime_visits(client, now)
    booking_day = booking_as_local(booking.starts_at).date() if booking.starts_at else None
    if booking_day is None:
        return None
    day_bookings = [
        item
        for item in client.bookings
        if item.status != "cancelled"
        and item.starts_at is not None
        and booking_as_local(item.starts_at).date() == booking_day
    ]
    day_bookings.sort(key=lambda item: (as_utc(item.starts_at) or now, item.momence_booking_id))
    already_counted_today = sum(
        1
        for item in day_bookings
        if item.status == "checked_in" and (as_utc(item.starts_at) or now) <= now
    )
    base_before_day = max(base_lifetime - already_counted_today, 0)
    for index, item in enumerate(day_bookings, start=1):
        if item.momence_booking_id == booking.momence_booking_id:
            return base_before_day + index
    return None


def booking_milestone_label(client: Client, booking: Booking | None, now: datetime) -> str | None:
    class_number = booking_class_number_today(client, booking, now)
    if class_number is None:
        return None
    if class_number in VISIT_MILESTONES:
        return f"{class_number}th class today"
    return None


def membership_fit_summary(client: Client) -> dict[str, str]:
    membership_name = active_membership_label(client)
    if not membership_name:
        return {
            "title": "Membership fit",
            "value": "No active membership on file",
            "note": "Use recent attendance to suggest the best next option.",
        }

    visits_last_30, _ = canonical_visit_windows(client)
    normalized = membership_name.lower()
    expiration = membership_expiration_context(client)

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
    import re

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
        "note": f"{visits_last_30} visits in the last 30 days. Good candidate for a manual membership-fit review."
        + (f" {expiration}." if expiration else ""),
    }


def churn_reason(client: Client, flags_summary: FlagsSummary) -> tuple[str, str]:
    activity = client.activity
    current_30, previous_30 = canonical_visit_windows(client)
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


def profile_chips(client: Client, flags_summary: FlagsSummary) -> list[str]:
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


def build_badges(client: Client, flags_summary: FlagsSummary, now: datetime) -> list[dict[str, str]]:
    badges: list[dict[str, str]] = []
    milestones = build_milestones(client, now)
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


def visit_breakdowns(client: Client, now: datetime) -> list[dict[str, Any]]:
    history = [
        booking
        for booking in client.bookings
        if (as_utc(booking.starts_at) or now) < now and (booking.status or "").lower() == "checked_in"
    ]
    instructor_counts: Counter[str] = Counter()
    instructor_labels: dict[str, Counter[str]] = defaultdict(Counter)
    weekday_counts: Counter[str] = Counter()
    format_counts: Counter[str] = Counter()

    for booking in history:
        instructor_name = normalize_text_label(booking.instructor_name)
        class_name = normalize_format_label(booking.class_name)
        if instructor_name:
            instructor_key = normalize_instructor_key(instructor_name)
            if instructor_key:
                instructor_counts[instructor_key] += 1
                instructor_labels[instructor_key][instructor_name] += 1
        if class_name:
            format_counts[class_name] += 1
        starts_local = booking_as_local(booking.starts_at)
        if starts_local is not None:
            weekday_counts[starts_local.strftime("%A")] += 1

    def top_count_lines(counter: Counter[str], *, limit: int = 3) -> list[str]:
        if not counter:
            return ["Still learning"]
        return [f"{label} ({count})" for label, count in counter.most_common(limit)]

    instructor_lines = (
        [f"{instructor_labels[key].most_common(1)[0][0]} ({count})" for key, count in instructor_counts.most_common(3)]
        if instructor_counts
        else ["Still learning"]
    )
    membership_fit = membership_fit_summary(client)
    membership_lines = []
    memberships = sorted(client.memberships, key=_membership_sort_key, reverse=True)
    if memberships:
        for membership in memberships[:6]:
            label = membership.membership_name or "Membership"
            status = (membership.status or "unknown").replace("_", " ")
            dates = []
            if membership.started_at:
                dates.append(booking_as_local(membership.started_at).strftime("%b %-d, %Y"))
            if membership.ended_at:
                dates.append(booking_as_local(membership.ended_at).strftime("%b %-d, %Y"))
            membership_lines.append(f"{label} · {status.title()} · {' to '.join(dates) if dates else 'Dates not available'}")
    else:
        membership_lines = [active_membership_label(client)] if active_membership_label(client) else ["No membership history loaded yet"]

    return [
        {"title": "Visits by instructor", "items": instructor_lines},
        {"title": "Visits by weekday", "items": top_count_lines(weekday_counts)},
        {"title": "Visits by format", "items": top_count_lines(format_counts)},
        {"title": "Membership history", "items": membership_lines},
        {"title": membership_fit["title"], "items": [membership_fit["value"], membership_fit["note"]]},
    ]


@dataclass
class EnrichedClientContext:
    client: Client
    now: datetime
    full_name: str
    membership: MembershipSummary
    activity: ActivitySummary
    flags: FlagsSummary
    profile_data: ProfileDataSummary
    preferences: PreferencesSummary
    milestones: list[MilestoneSummary]
    notes: list[NoteSummary]
    active_membership_name: str | None
    join_date_label: str
    churn_reason: str
    churn_rule: str
    concierge_chips: list[str]
    membership_spotlight: dict[str, str]


def build_enriched_client_context(client: Client, now: datetime | None = None) -> EnrichedClientContext:
    current = now or datetime.now(timezone.utc)
    flags = build_flag_summary(client, current)
    return EnrichedClientContext(
        client=client,
        now=current,
        full_name=client.full_name or " ".join(part for part in [client.first_name, client.last_name] if part).strip(),
        membership=build_membership_summary(client),
        activity=build_activity_summary(client),
        flags=flags,
        profile_data=build_profile_data_summary(client),
        preferences=build_preferences_summary(client),
        milestones=build_milestones(client, current),
        notes=build_notes(client),
        active_membership_name=active_membership_label(client),
        join_date_label=join_date_label(client),
        churn_reason=churn_reason(client, flags)[0],
        churn_rule=churn_reason(client, flags)[1],
        concierge_chips=profile_chips(client, flags),
        membership_spotlight=membership_fit_summary(client),
    )


def booking_snapshot(context: EnrichedClientContext, booking: Booking | None) -> dict[str, Any]:
    class_number_today = booking_class_number_today(context.client, booking, context.now)
    booking_milestone = booking_milestone_label(context.client, booking, context.now)
    return {
        "booking_id": booking.momence_booking_id if booking is not None else None,
        "checked_in": booking.status == "checked_in" if booking is not None else False,
        "location": booking.location_name if booking is not None else None,
        "class_name": booking.class_name if booking is not None else None,
        "class_number_today": class_number_today,
        "class_number_label": _ordinal_label(class_number_today),
        "booking_milestone": booking_milestone,
    }


def build_profile_response_from_context(context: EnrichedClientContext) -> ClientProfileResponse:
    return ClientProfileResponse(
        member_id=context.client.momence_member_id,
        first_name=context.client.first_name,
        last_name=context.client.last_name,
        full_name=context.full_name,
        email=context.client.email,
        phone=context.client.phone,
        birthday=context.client.birthday,
        membership=context.membership,
        activity=context.activity,
        flags=context.flags,
        profile_data=context.profile_data,
        preferences=context.preferences,
        milestones=context.milestones,
        notes=context.notes,
        freshness={},
    )
