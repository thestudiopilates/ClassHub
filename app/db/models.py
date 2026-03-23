from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Optional

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Client(Base):
    __tablename__ = "clients"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    momence_member_id: Mapped[str] = mapped_column(Text, unique=True, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    full_name: Mapped[Optional[str]] = mapped_column(Text)
    email: Mapped[Optional[str]] = mapped_column(Text)
    phone: Mapped[Optional[str]] = mapped_column(Text)
    birthday: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )
    source_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    activity: Mapped[Optional["ClientActivity"]] = relationship(back_populates="client", uselist=False)
    profile_data: Mapped[Optional["ClientProfileData"]] = relationship(back_populates="client", uselist=False)
    preferences: Mapped[Optional["ClientPreference"]] = relationship(back_populates="client", uselist=False)
    flags: Mapped[Optional["ClientFlag"]] = relationship(back_populates="client", uselist=False)
    notes: Mapped[list[ClientNote]] = relationship(back_populates="client")
    milestones: Mapped[list[Milestone]] = relationship(back_populates="client")
    bookings: Mapped[list[Booking]] = relationship(back_populates="client")


class ClientActivity(Base):
    __tablename__ = "client_activity"

    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), primary_key=True)
    last_checkin_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_booking_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_purchase_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    next_booking_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    first_visit_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    total_visits: Mapped[int] = mapped_column(Integer, default=0)
    lifetime_visits_baseline: Mapped[int] = mapped_column(Integer, default=0)
    lifetime_visits_increment: Mapped[int] = mapped_column(Integer, default=0)
    lifetime_visits_baseline_as_of: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    visits_last_30d: Mapped[int] = mapped_column(Integer, default=0)
    visits_previous_30d: Mapped[int] = mapped_column(Integer, default=0)
    has_active_membership: Mapped[bool] = mapped_column(Boolean, default=False)
    active_membership_name: Mapped[Optional[str]] = mapped_column(Text)
    activity_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="activity")


class Booking(Base):
    __tablename__ = "bookings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    momence_booking_id: Mapped[str] = mapped_column(Text, unique=True)
    momence_session_id: Mapped[str] = mapped_column(Text, index=True)
    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), index=True)
    class_name: Mapped[Optional[str]] = mapped_column(Text)
    location_name: Mapped[Optional[str]] = mapped_column(Text)
    instructor_name: Mapped[Optional[str]] = mapped_column(Text, index=True)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[Optional[str]] = mapped_column(Text)
    is_waitlist: Mapped[bool] = mapped_column(Boolean, default=False)
    synced_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="bookings")


class ClientNote(Base):
    __tablename__ = "client_notes"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), index=True)
    note_type: Mapped[Optional[str]] = mapped_column(Text)
    note_text: Mapped[str] = mapped_column(Text)
    is_injury_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    is_front_desk_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    is_instructor_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    source_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    client: Mapped[Client] = relationship(back_populates="notes")


class ClientFlag(Base):
    __tablename__ = "client_flags"

    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), primary_key=True)
    is_active_180d: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    birthday_this_week: Mapped[bool] = mapped_column(Boolean, index=True, default=False)
    birthday_today: Mapped[bool] = mapped_column(Boolean, default=False)
    churn_risk: Mapped[Optional[str]] = mapped_column(Text, index=True)
    vip_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    new_client_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    welcome_back_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    injury_flag: Mapped[bool] = mapped_column(Boolean, default=False)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="flags")


class ClientProfileData(Base):
    __tablename__ = "client_profile_data"

    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), primary_key=True)
    fun_fact: Mapped[Optional[str]] = mapped_column(Text)
    pregnant_status: Mapped[Optional[str]] = mapped_column(Text)
    pregnancy_due_date: Mapped[Optional[date]] = mapped_column(Date)
    heard_about_us: Mapped[Optional[str]] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="profile_data")


class ClientPreference(Base):
    __tablename__ = "client_preferences"

    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), primary_key=True)
    favorite_time_of_day: Mapped[Optional[str]] = mapped_column(Text)
    favorite_weekdays: Mapped[Optional[str]] = mapped_column(Text)
    favorite_instructors: Mapped[Optional[str]] = mapped_column(Text)
    favorite_formats: Mapped[Optional[str]] = mapped_column(Text)
    preference_basis: Mapped[Optional[str]] = mapped_column(Text)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="preferences")


class Milestone(Base):
    __tablename__ = "milestones"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    client_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("clients.id"), index=True)
    milestone_type: Mapped[str] = mapped_column(Text)
    milestone_value: Mapped[Optional[str]] = mapped_column(Text)
    milestone_date: Mapped[Optional[date]] = mapped_column(Date)
    starts_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    is_current: Mapped[bool] = mapped_column(Boolean, default=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client: Mapped[Client] = relationship(back_populates="milestones")


class SyncRun(Base):
    __tablename__ = "sync_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_name: Mapped[str] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text)
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    error_text: Mapped[Optional[str]] = mapped_column(Text)


class SyncState(Base):
    __tablename__ = "sync_states"

    domain: Mapped[str] = mapped_column(Text, primary_key=True)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_successful_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text, default="unknown")
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    error_text: Mapped[Optional[str]] = mapped_column(Text)
