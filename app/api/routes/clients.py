from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas import ClientProfileResponse
from app.services.domain import get_client_profile
from app.services.sync.jobs import refresh_client_by_member_id

router = APIRouter()


@router.get("/{momence_member_id}", response_model=ClientProfileResponse)
def get_client(
    momence_member_id: str,
    refresh_context: bool = Query(default=False, description="Refresh memberships and notes before returning"),
    db: Session = Depends(get_db),
) -> ClientProfileResponse:
    if refresh_context:
        refresh_client_by_member_id(db, momence_member_id)
    profile = get_client_profile(db, momence_member_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="Client not found")
    return profile
