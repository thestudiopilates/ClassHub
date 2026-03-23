from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.db.models import Booking, ClientActivity
from app.db.session import SessionLocal
from app.schemas import BookingCheckInResponse
from app.services.momence.client import MomenceClient

router = APIRouter()


@router.post("/{booking_id}/check-in", response_model=BookingCheckInResponse)
async def check_in_booking(booking_id: str) -> BookingCheckInResponse:
    if not settings.momence_enable_check_in_write:
        raise HTTPException(
            status_code=403,
            detail="Momence check-in writes are disabled. Set MOMENCE_ENABLE_CHECK_IN_WRITE=true to enable.",
        )

    client = MomenceClient()
    try:
        payload = await client.check_in_session_booking(booking_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Momence check-in failed: {exc}") from exc

    db = SessionLocal()
    try:
        booking = db.scalar(select(Booking).where(Booking.momence_booking_id == booking_id))
        if booking is not None:
            booking.status = "checked_in"
            activity = db.get(ClientActivity, booking.client_id)
            if activity is None:
                activity = ClientActivity(client_id=booking.client_id)
            activity.lifetime_visits_increment = (activity.lifetime_visits_increment or 0) + 1
            activity.total_visits = (activity.lifetime_visits_baseline or 0) + (activity.lifetime_visits_increment or 0)
            activity.last_checkin_at = booking.starts_at or datetime.now(timezone.utc)
            activity.activity_updated_at = datetime.now(timezone.utc)
            db.add(booking)
            db.add(activity)
            db.commit()
    finally:
        db.close()

    return BookingCheckInResponse(
        booking_id=booking_id,
        success=True,
        checked_in=True,
        response=payload or {"status": "checked-in"},
    )


@router.delete("/{booking_id}/check-in", response_model=BookingCheckInResponse)
async def undo_check_in_booking(booking_id: str) -> BookingCheckInResponse:
    if not settings.momence_enable_check_in_write:
        raise HTTPException(
            status_code=403,
            detail="Momence check-in writes are disabled. Set MOMENCE_ENABLE_CHECK_IN_WRITE=true to enable.",
        )

    client = MomenceClient()
    try:
        payload = await client.undo_check_in_session_booking(booking_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Momence undo check-in failed: {exc}") from exc

    db = SessionLocal()
    try:
        booking = db.scalar(select(Booking).where(Booking.momence_booking_id == booking_id))
        if booking is not None:
            booking.status = "booked"
            activity = db.get(ClientActivity, booking.client_id)
            if activity is not None and (activity.lifetime_visits_increment or 0) > 0:
                activity.lifetime_visits_increment = max((activity.lifetime_visits_increment or 0) - 1, 0)
                activity.total_visits = (activity.lifetime_visits_baseline or 0) + (activity.lifetime_visits_increment or 0)
                activity.activity_updated_at = datetime.now(timezone.utc)
                db.add(activity)
            db.add(booking)
            db.commit()
    finally:
        db.close()

    return BookingCheckInResponse(
        booking_id=booking_id,
        success=True,
        checked_in=False,
        response=payload or {"status": "not-checked-in"},
    )
