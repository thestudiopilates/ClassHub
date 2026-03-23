from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas import ClientProfileResponse, FrontDeskResponse, InstructorResponse, WeekAheadResponse
from app.services.domain import (
    get_client_profile,
    get_front_desk_view,
    get_instructor_view,
    get_week_ahead_view,
)

router = APIRouter()


@router.get("/front-desk", response_model=FrontDeskResponse)
def front_desk_view(day: date, location_name: Optional[str] = None, db: Session = Depends(get_db)) -> FrontDeskResponse:
    return get_front_desk_view(db, day, location_name)


@router.get("/instructor", response_model=InstructorResponse)
def instructor_view(
    day: date,
    session_id: Optional[str] = None,
    instructor_name: Optional[str] = None,
    db: Session = Depends(get_db),
) -> InstructorResponse:
    return get_instructor_view(db, day, session_id, instructor_name)


@router.get("/week-ahead", response_model=WeekAheadResponse)
def week_ahead_view(start: date, days: int = 7, db: Session = Depends(get_db)) -> WeekAheadResponse:
    return get_week_ahead_view(db, start, days)


@router.get("/birthdays", response_model=List[ClientProfileResponse])
def birthdays(start: date, end: date, db: Session = Depends(get_db)) -> List[ClientProfileResponse]:
    profiles = get_week_ahead_view(db, start, (end - start).days + 1).birthdays
    member_ids = {item.member_id for item in profiles}
    return [profile for member_id in member_ids if (profile := get_client_profile(db, member_id)) is not None]
