import unittest
from unittest.mock import AsyncMock, patch

from app.services.momence.client import MomenceClient


class MomenceClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_access_token_falls_back_to_stored_token_when_refresh_fails(self) -> None:
        client = MomenceClient()
        stored = {
            "access_token": "stored-access-token",
            "refresh_token": "stored-refresh-token",
            "expires_at": "2000-01-01T00:00:00+00:00",
        }

        with patch("app.services.momence.client.load_tokens", return_value=stored):
            with patch("app.services.momence.client.access_token_is_fresh", return_value=False):
                with patch.object(
                    client,
                    "refresh_access_token",
                    AsyncMock(side_effect=RuntimeError("refresh failed")),
                ) as refresh_mock:
                    token = await client.get_access_token()

        refresh_mock.assert_awaited_once_with("stored-refresh-token")
        self.assertEqual(token, "stored-access-token")

    async def test_get_access_token_uses_stored_token_when_credentials_missing(self) -> None:
        client = MomenceClient()
        client.client_id = ""
        client.client_secret = ""
        client.username = ""
        client.password = ""
        stored = {
            "access_token": "stored-access-token",
            "expires_at": "2000-01-01T00:00:00+00:00",
        }

        with patch("app.services.momence.client.load_tokens", return_value=stored):
            with patch("app.services.momence.client.access_token_is_fresh", return_value=False):
                token = await client.get_access_token()

        self.assertEqual(token, "stored-access-token")


if __name__ == "__main__":
    unittest.main()
