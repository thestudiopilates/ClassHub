from datetime import datetime, timedelta, timezone
import unittest

from app.db.models import Booking, Client, ClientActivity, ClientMembership
from app.services.client_context import build_membership_summary
from app.services.client_intelligence import canonical_client_lifetime_visits


class ClientLogicTests(unittest.TestCase):
    def test_lifetime_visits_prefers_canonical_max_over_partial_checked_in_history(self) -> None:
        now = datetime.now(timezone.utc)
        client = Client(momence_member_id="member-1", bookings=[])
        client.activity = ClientActivity(
            lifetime_visits_baseline=50,
            lifetime_visits_increment=1,
            total_visits=51,
            visits_last_30d=10,
            visits_previous_30d=8,
        )
        client.bookings = [
            Booking(
                momence_booking_id="booking-1",
                momence_session_id="session-1",
                client_id=client.id,
                starts_at=now - timedelta(days=2),
                status="checked_in",
            ),
            Booking(
                momence_booking_id="booking-2",
                momence_session_id="session-2",
                client_id=client.id,
                starts_at=now - timedelta(days=1),
                status="checked_in",
            ),
        ]

        self.assertEqual(canonical_client_lifetime_visits(client, now), 51)

    def test_membership_summary_marks_future_dated_membership_as_active(self) -> None:
        now = datetime.now(timezone.utc)
        client = Client(momence_member_id="member-2", memberships=[])
        client.activity = ClientActivity(has_active_membership=False, active_membership_name=None)
        client.memberships = [
            ClientMembership(
                membership_name="12x Month",
                status="paused",
                ended_at=now + timedelta(days=10),
            )
        ]

        summary = build_membership_summary(client)

        self.assertTrue(summary.active)
        self.assertEqual(summary.name, "12x Month")


if __name__ == "__main__":
    unittest.main()
