from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.services.demo_data import build_demo_payload
from app.services.automation import trigger_auto_warm_if_needed


router = APIRouter()

STATIC_ROOT = Path(__file__).resolve().parents[2] / "static" / "demo"

# In-memory cache: avoids rebuilding the full payload on every page refresh.
# Invalidated after TTL or when day rolls over.
_demo_cache: dict[str, Any] = {"payload": None, "day": None, "expires_at": 0.0}
_CACHE_TTL_SECONDS = 60


@router.get("/demo")
def demo_ui() -> FileResponse:
    return FileResponse(STATIC_ROOT / "index.html")


@router.get("/demo/data")
def demo_data(day: Optional[date] = None, db: Session = Depends(get_db)) -> dict:
    trigger_auto_warm_if_needed(day)

    now = datetime.now(timezone.utc).timestamp()
    cache_day = day or date.today()

    if (
        _demo_cache["payload"] is not None
        and _demo_cache["day"] == cache_day
        and now < _demo_cache["expires_at"]
    ):
        return _demo_cache["payload"]

    payload = build_demo_payload(db, day)
    _demo_cache["payload"] = payload
    _demo_cache["day"] = cache_day
    _demo_cache["expires_at"] = now + _CACHE_TTL_SECONDS
    return payload
