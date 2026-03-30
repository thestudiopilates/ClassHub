from datetime import date, datetime, timezone
from typing import Optional
import unittest
from unittest.mock import patch

from app.schemas import SyncRunResponse
from app.services import automation


def _response(job_name: str, records: int = 0, *, error_text: Optional[str] = None) -> SyncRunResponse:
    now = datetime.now(timezone.utc)
    return SyncRunResponse(
        job_name=job_name,
        status="completed",
        records_processed=records,
        started_at=now,
        finished_at=now,
        error_text=error_text,
    )


class AutomationModeTests(unittest.TestCase):
    def test_intraday_sync_skips_roster_history_when_fresh(self) -> None:
        fake_db = object()
        with patch.object(automation, "sync_upcoming_bookings", return_value=_response("bookings", 12)) as bookings_mock:
            with patch.object(automation, "roster_history_is_fresh_for_day", return_value=True) as fresh_mock:
                with patch.object(automation, "sync_roster_client_history_full") as roster_mock:
                    result = automation.run_intraday_ops_sync(fake_db, day=date(2026, 3, 30))

        bookings_mock.assert_called_once_with(fake_db)
        fresh_mock.assert_called_once()
        roster_mock.assert_not_called()
        self.assertEqual(result["bookings"].records_processed, 12)
        self.assertEqual(result["roster_history"].records_processed, 0)
        self.assertIn("Skipped roster history", result["roster_history"].error_text or "")

    def test_intraday_sync_backfills_when_roster_history_is_stale(self) -> None:
        fake_db = object()
        with patch.object(automation, "sync_upcoming_bookings", return_value=_response("bookings", 8)) as bookings_mock:
            with patch.object(automation, "roster_history_is_fresh_for_day", return_value=False) as fresh_mock:
                with patch.object(
                    automation,
                    "sync_roster_client_history_full",
                    return_value=_response("roster_history", 15),
                ) as roster_mock:
                    result = automation.run_intraday_ops_sync(fake_db, day=date(2026, 3, 30))

        bookings_mock.assert_called_once_with(fake_db)
        fresh_mock.assert_called_once()
        roster_mock.assert_called_once()
        self.assertEqual(result["bookings"].records_processed, 8)
        self.assertEqual(result["roster_history"].records_processed, 15)


if __name__ == "__main__":
    unittest.main()
