from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.demo_data import build_demo_payload
from app.services.automation import trigger_auto_warm_if_needed


router = APIRouter()

STATIC_ROOT = Path(__file__).resolve().parents[2] / "static" / "demo"


@router.get("/demo")
def demo_ui() -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")


@router.get("/demo/data")
def demo_data(day: Optional[date] = None, db: Session = Depends(get_db)) -> dict:
    trigger_auto_warm_if_needed(day)
    return build_demo_payload(db, day)
