from __future__ import annotations

from datetime import date as date_type, datetime
from typing import Dict, List, Optional

from pydantic import BaseModel


class MembershipSummary(BaseModel):
    active: bool
    name: Optional[str] = None


class ActivitySummary(BaseModel):
    last_checkin_at: Optional[datetime] = None
    last_booking_at: Optional[datetime] = None
    next_booking_at: Optional[datetime] = None
    total_visits: int = 0
    lifetime_visits: int = 0
    visits_last_30d: int = 0
    visits_previous_30d: int = 0


class FlagsSummary(BaseModel):
    birthday_today: bool = False
    birthday_this_week: bool = False
    injury: bool = False
    welcome_back: bool = False
    new_client: bool = False
    churn_risk: Optional[str] = None


class PreferencesSummary(BaseModel):
    favorite_time_of_day: Optional[str] = None
    favorite_weekdays: List[str] = []
    favorite_instructors: List[str] = []
    favorite_formats: List[str] = []
    preference_basis: Optional[str] = None


class ProfileDataSummary(BaseModel):
    fun_fact: Optional[str] = None
    pregnant_status: Optional[str] = None
    pregnancy_due_date: Optional[date_type] = None
    heard_about_us: Optional[str] = None


class FreshnessItem(BaseModel):
    domain: str
    last_synced_at: Optional[datetime] = None
    last_successful_at: Optional[datetime] = None
    status: str = "unknown"
    records_processed: int = 0
    error_text: Optional[str] = None
    stale_after_hours: int
    is_stale: bool = True


class NoteSummary(BaseModel):
    type: Optional[str] = None
    text: str


class MilestoneSummary(BaseModel):
    type: str
    value: Optional[str] = None
    date: Optional[date_type] = None


class ClientListItem(BaseModel):
    member_id: str
    name: str
    membership: MembershipSummary
    activity: ActivitySummary
    flags: FlagsSummary
    profile_data: ProfileDataSummary
    preferences: PreferencesSummary
    milestones: List[MilestoneSummary]
    notes: List[NoteSummary]


class SessionRosterItem(BaseModel):
    member_id: str
    name: str
    birthday_this_week: bool = False
    milestones: List[str]
    injury_flag: bool = False
    instructor_notes: List[str]
    new_client: bool = False
    welcome_back: bool = False
    total_visits: int = 0


class SessionView(BaseModel):
    session_id: str
    class_name: Optional[str] = None
    starts_at: datetime
    location_name: Optional[str] = None
    instructor_name: Optional[str] = None
    roster: List[SessionRosterItem]


class FrontDeskResponse(BaseModel):
    date: date_type
    arrivals: List[ClientListItem]
    freshness: Dict[str, FreshnessItem]


class InstructorResponse(BaseModel):
    date: date_type
    sessions: List[SessionView]
    freshness: Dict[str, FreshnessItem]


class WeekAheadResponse(BaseModel):
    start: date_type
    end: date_type
    sessions: List[SessionView]
    birthdays: List[SessionRosterItem]
    milestones: List[SessionRosterItem]
    freshness: Dict[str, FreshnessItem]


class ClientProfileResponse(BaseModel):
    member_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    birthday: Optional[date_type] = None
    membership: MembershipSummary
    activity: ActivitySummary
    flags: FlagsSummary
    profile_data: ProfileDataSummary
    preferences: PreferencesSummary
    milestones: List[MilestoneSummary]
    notes: List[NoteSummary]
    freshness: Dict[str, FreshnessItem]


class SyncRunResponse(BaseModel):
    job_name: str
    status: str
    records_processed: int
    started_at: datetime
    finished_at: Optional[datetime] = None
    error_text: Optional[str] = None


class BookingHistoryProgressResponse(BaseModel):
    window_start: datetime
    window_end: datetime
    cursor: datetime
    chunk_days: int
    complete: bool = False
    records_processed_last_chunk: int = 0
    status: str = "unknown"
    error_text: Optional[str] = None


class BookingHistoryRunRequest(BaseModel):
    max_chunks: int = 1


class RosterHistoryRunRequest(BaseModel):
    day: Optional[date_type] = None
    max_clients: int = 25
    offset: int = 0


class TargetedRefreshRequest(BaseModel):
    member_ids: List[str]


class BookingCheckInResponse(BaseModel):
    booking_id: str
    success: bool
    provider: str = "momence"
    checked_in: bool = True
    response: Dict[str, object] = {}


class MomenceTokenImportRequest(BaseModel):
    payload: Dict[str, object]


class SeedActivityPayload(BaseModel):
    last_checkin_at: Optional[datetime] = None
    last_booking_at: Optional[datetime] = None
    next_booking_at: Optional[datetime] = None
    first_visit_at: Optional[datetime] = None
    total_visits: int = 0
    lifetime_visits_baseline: int = 0
    lifetime_visits_increment: int = 0
    lifetime_visits_baseline_as_of: Optional[datetime] = None
    visits_last_30d: int = 0
    visits_previous_30d: int = 0
    has_active_membership: bool = False
    active_membership_name: Optional[str] = None


class SeedProfileDataPayload(BaseModel):
    fun_fact: Optional[str] = None
    pregnant_status: Optional[str] = None
    pregnancy_due_date: Optional[date_type] = None
    heard_about_us: Optional[str] = None


class SeedPreferencesPayload(BaseModel):
    favorite_time_of_day: Optional[str] = None
    favorite_weekdays: Optional[str] = None
    favorite_instructors: Optional[str] = None
    favorite_formats: Optional[str] = None
    preference_basis: Optional[str] = None


class SeedNotePayload(BaseModel):
    type: Optional[str] = None
    text: str
    is_injury_flag: bool = False
    is_front_desk_flag: bool = False
    is_instructor_flag: bool = False
    source_updated_at: Optional[datetime] = None


class SeedClientPayload(BaseModel):
    member_id: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    birthday: Optional[date_type] = None
    source_updated_at: Optional[datetime] = None
    activity: Optional[SeedActivityPayload] = None
    profile_data: Optional[SeedProfileDataPayload] = None
    preferences: Optional[SeedPreferencesPayload] = None
    notes: List[SeedNotePayload] = []


class SeedImportRequest(BaseModel):
    clients: List[SeedClientPayload]
    recompute_flags: bool = False


class SeedImportResponse(BaseModel):
    imported_clients: int
    imported_notes: int
    recomputed_flags: bool = False
