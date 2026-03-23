from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import cached_property
from urllib.parse import urlencode
import secrets

import httpx

from app.core.config import settings
from app.services.momence.token_store import access_token_is_fresh, load_tokens, save_tokens


class MomenceClient:
    def __init__(self) -> None:
        self.base_url = settings.momence_base_url.rstrip("/")
        self.client_id = settings.momence_client_id
        self.client_secret = settings.momence_client_secret
        self.username = settings.momence_username
        self.password = settings.momence_password

    @cached_property
    def _basic_auth(self) -> tuple[str, str]:
        return (self.client_id, self.client_secret)

    def get_authorization_url(self, state: str) -> str:
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirectUri": settings.momence_redirect_uri,
            "state": state,
        }
        if settings.momence_oauth_scopes.strip():
            params["scope"] = settings.momence_oauth_scopes.strip()
        return f"{self.base_url}/api/v2/auth/authorize?{urlencode(params)}"

    @staticmethod
    def generate_oauth_state() -> str:
        return secrets.token_urlsafe(32)

    async def exchange_authorization_code(self, code: str) -> dict:
        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            attempts = [
                {
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirectUri": settings.momence_redirect_uri,
                },
                {
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": settings.momence_redirect_uri,
                },
                {
                    "grant_type": "authorization_code",
                    "code": code,
                },
            ]
            last_response: httpx.Response | None = None
            for payload in attempts:
                response = await client.post(
                    "/api/v2/auth/token",
                    auth=self._basic_auth,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=payload,
                )
                if response.is_success:
                    return save_tokens(response.json())
                last_response = response
            if last_response is not None:
                last_response.raise_for_status()
            raise RuntimeError("Momence authorization code exchange failed.")

    async def refresh_access_token(self, refresh_token: str) -> dict:
        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            attempts = [
                {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                {
                    "grant_type": "refresh_token",
                    "refreshToken": refresh_token,
                },
            ]
            last_response: httpx.Response | None = None
            for payload in attempts:
                response = await client.post(
                    "/api/v2/auth/token",
                    auth=self._basic_auth,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=payload,
                )
                if response.is_success:
                    return save_tokens(response.json())
                last_response = response
            if last_response is not None:
                last_response.raise_for_status()
            raise RuntimeError("Momence refresh token exchange failed.")

    async def get_access_token(self) -> str:
        stored = load_tokens()
        if access_token_is_fresh(stored):
            return stored["access_token"]

        if stored and stored.get("refresh_token"):
            refreshed = await self.refresh_access_token(stored["refresh_token"])
            if refreshed.get("access_token"):
                return refreshed["access_token"]

        if not all([self.client_id, self.client_secret, self.username, self.password]):
            missing = [
                name
                for name, value in [
                    ("MOMENCE_CLIENT_ID", self.client_id),
                    ("MOMENCE_CLIENT_SECRET", self.client_secret),
                    ("MOMENCE_USERNAME", self.username),
                    ("MOMENCE_PASSWORD", self.password),
                ]
                if not value
            ]
            raise RuntimeError(f"Missing Momence credentials: {', '.join(missing)}")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0) as client:
            response = await client.post(
                "/api/v2/auth/token",
                auth=self._basic_auth,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data={
                    "grant_type": "password",
                    "username": self.username,
                    "password": self.password,
                },
            )
            response.raise_for_status()
            payload = response.json()
            saved = save_tokens(payload)
            return saved.get("access_token") or saved.get("accessToken") or ""

    async def _authorized_client(self) -> httpx.AsyncClient:
        token = await self.get_access_token()
        return httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers={"Authorization": f"Bearer {token}"},
        )

    async def _get_paginated(self, path: str, *, params: dict | None = None) -> list[dict]:
        page = 0
        payload: list[dict] = []
        base_params = params.copy() if params else {}
        async with await self._authorized_client() as client:
            while True:
                query = {"page": page, "pageSize": 100, **base_params}
                response = await client.get(path, params=query)
                response.raise_for_status()
                body = response.json()
                items = body.get("payload", [])
                payload.extend(items)
                if len(items) < query["pageSize"]:
                    break
                page += 1
        return payload

    @staticmethod
    def _session_window_param_attempts(start: date | datetime, end: date | datetime) -> list[dict]:
        if isinstance(start, datetime):
            start_dt = start.astimezone(timezone.utc) if start.tzinfo else start.replace(tzinfo=timezone.utc)
        else:
            start_dt = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        if isinstance(end, datetime):
            end_dt = end.astimezone(timezone.utc) if end.tzinfo else end.replace(tzinfo=timezone.utc)
        else:
            end_dt = datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc)

        start_day = start_dt.date().isoformat()
        end_day = end_dt.date().isoformat()
        start_iso = start_dt.isoformat()
        end_iso = end_dt.isoformat()
        next_day_iso = (end_dt + timedelta(days=1)).date().isoformat()

        attempts = [
            {
                "sortBy": "startsAt",
                "sortOrder": "ASC",
                "startAfter": start_day,
                "startBefore": next_day_iso,
                "includeCancelled": False,
            },
            {
                "sortBy": "startsAt",
                "sortOrder": "ASC",
                "startAfter": start_day,
                "startBefore": end_day,
                "includeCancelled": False,
            },
            {
                "sortBy": "startsAt",
                "sortOrder": "ASC",
                "startAfter": start_iso,
                "startBefore": end_iso,
                "includeCancelled": False,
            },
            {
                "sortBy": "startsAt",
                "sortOrder": "ASC",
                "startAfter": start_day,
                "endBefore": end_day,
                "includeCancelled": False,
            },
        ]
        return attempts

    async def fetch_session_bookings_between(self, start: date, end: date) -> list[dict]:
        sessions: list[dict] = []
        for params in self._session_window_param_attempts(start, end):
            sessions = await self._get_paginated("/api/v2/host/sessions", params=params)
            if sessions:
                break
        bookings: list[dict] = []
        async with await self._authorized_client() as client:
            for session in sessions:
                session_id = session["id"]
                page = 0
                page_size = 100
                while True:
                    response = await client.get(
                        f"/api/v2/host/sessions/{session_id}/bookings",
                        params={
                            "page": page,
                            "pageSize": page_size,
                            "sortBy": "createdAt",
                            "sortOrder": "ASC",
                            "includeCancelled": False,
                        },
                    )
                    try:
                        response.raise_for_status()
                    except httpx.HTTPStatusError:
                        if response.status_code >= 500:
                            break
                        raise
                    body = response.json()
                    items = body.get("payload", [])
                    for booking in items:
                        bookings.append(
                            {
                                "session": session,
                                "booking": booking,
                            }
                        )
                    if len(items) < page_size:
                        break
                    page += 1
        return bookings

    async def fetch_upcoming_bookings(self, start: date, end: date) -> list[dict]:
        return await self.fetch_session_bookings_between(start, end)

    async def fetch_member_profile(self, momence_member_id: str) -> dict:
        async with await self._authorized_client() as client:
            response = await client.get(f"/api/v2/host/members/{momence_member_id}")
            response.raise_for_status()
            return response.json()

    async def fetch_member_notes(self, momence_member_id: str) -> list[dict]:
        return await self._get_paginated(
            f"/api/v2/host/members/{momence_member_id}/notes",
            params={"sortBy": "modifiedAt", "sortOrder": "DESC"},
        )

    async def fetch_member_memberships(self, momence_member_id: str) -> list[dict]:
        return await self._get_paginated(
            f"/api/v2/host/members/{momence_member_id}/bought-memberships/active",
            params={"includeFrozen": "false"},
        )

    async def fetch_member_session_bookings(self, momence_member_id: str) -> list[dict]:
        page = 0
        page_size = 100
        rows: list[dict] = []
        async with await self._authorized_client() as client:
            while True:
                response = await client.get(
                    f"/api/v2/host/members/{momence_member_id}/sessions",
                    params={
                        "page": page,
                        "pageSize": page_size,
                        "sortBy": "startsAt",
                        "sortOrder": "DESC",
                        "includeCancelled": "false",
                    },
                )
                response.raise_for_status()
                body = response.json()
                items = body.get("payload", [])
                rows.extend(items)
                if len(items) < page_size:
                    break
                page += 1
        return rows

    async def check_in_session_booking(self, booking_id: str) -> dict:
        async with await self._authorized_client() as client:
            response = await client.post(f"/api/v2/host/session-bookings/{booking_id}/check-in")
            response.raise_for_status()
            return response.json() if response.content else {"status": "ok"}

    async def undo_check_in_session_booking(self, booking_id: str) -> dict:
        async with await self._authorized_client() as client:
            response = await client.delete(f"/api/v2/host/session-bookings/{booking_id}/check-in")
            response.raise_for_status()
            return response.json() if response.content else {"status": "ok"}
