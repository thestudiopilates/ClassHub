from __future__ import annotations

import csv
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from playwright.sync_api import BrowserContext, Page, sync_playwright

from app.core.config import settings


class MomenceBrowserClient:
    def __init__(self) -> None:
        self.host_id = settings.momence_host_id
        self.profile_dir = Path(settings.momence_browser_profile_dir).expanduser()
        self.birthdays_report_url = settings.momence_birthdays_report_url

    def _ensure_profile(self) -> None:
        if not self.profile_dir.exists():
            raise RuntimeError(
                f"Momence browser profile not found at {self.profile_dir}. "
                "Reuse your saved Playwright session or refresh it before syncing."
            )

    @contextmanager
    def browser_context(self) -> BrowserContext:
        self._ensure_profile()
        with sync_playwright() as playwright:
            context = playwright.chromium.launch_persistent_context(
                user_data_dir=str(self.profile_dir),
                headless=True,
                accept_downloads=True,
            )
            try:
                yield context
            finally:
                context.close()

    def _open_dashboard_page(self, context: BrowserContext, path: str) -> Page:
        page = context.new_page()
        page.goto(f"https://momence.com{path}", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        if "/sign-in" in page.url:
            raise RuntimeError("Momence browser session is no longer authenticated.")
        return page

    def fetch_active_customers(self, active_days: int = 180, page_size: int = 200) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=active_days)
        payload: list[dict] = []
        with self.browser_context() as context:
            page = self._open_dashboard_page(context, f"/dashboard/{self.host_id}/crm")
            current_page = 0
            while True:
                batch = page.evaluate(
                    """async ({ hostId, currentPage, pageSize }) => {
                      const res = await fetch(
                        `/_api/primary/host/${hostId}/customers?query=&page=${currentPage}&pageSize=${pageSize}`,
                        { credentials: 'include' }
                      );
                      return await res.json();
                    }""",
                    {"hostId": self.host_id, "currentPage": current_page, "pageSize": page_size},
                )
                customers = batch.get("payload", [])
                pagination = batch.get("pagination", {})
                payload.extend(customers)
                total_pages = pagination.get("pageCount") or pagination.get("totalPages")
                if not customers:
                    break
                if total_pages is not None and current_page + 1 >= total_pages:
                    break
                if total_pages is None and len(customers) < page_size:
                    break
                current_page += 1

        active_customers = []
        for customer in payload:
            last_seen = customer.get("lastSeen")
            if not last_seen:
                continue
            seen_at = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
            if seen_at >= cutoff:
                active_customers.append(customer)
        return active_customers

    def fetch_member_contexts(self, member_ids: list[str], batch_size: int = 10) -> dict[str, dict]:
        if not member_ids:
            return {}

        results: dict[str, dict] = {}
        with self.browser_context() as context:
            page = self._open_dashboard_page(context, f"/dashboard/{self.host_id}/crm")
            for index in range(0, len(member_ids), batch_size):
                batch_ids = member_ids[index : index + batch_size]
                batch_results = page.evaluate(
                    """async ({ hostId, memberIds }) => {
                      const output = await Promise.all(
                        memberIds.map(async (memberId) => {
                          const [notesRes, membershipsRes] = await Promise.all([
                            fetch(`/_api/primary/host/${hostId}/customer-notes?memberId=${memberId}`, {
                              credentials: 'include'
                            }),
                            fetch(`/_api/primary/host/${hostId}/customers/${memberId}/memberships`, {
                              credentials: 'include'
                            })
                          ]);
                          const notes = await notesRes.json();
                          const memberships = await membershipsRes.json();
                          return { memberId, notes, memberships };
                        })
                      );
                      return output;
                    }""",
                    {"hostId": self.host_id, "memberIds": batch_ids},
                )
                for item in batch_results:
                    results[str(item["memberId"])] = {
                        "notes": item.get("notes") or [],
                        "memberships": item.get("memberships") or {},
                    }
        return results

    def get_authenticated_cookies(self) -> dict[str, str]:
        with self.browser_context() as context:
            self._open_dashboard_page(context, f"/dashboard/{self.host_id}/crm")
            return {
                item["name"]: item["value"]
                for item in context.cookies()
                if "momence.com" in item.get("domain", "")
            }

    def download_birthdays_csv(self) -> list[dict[str, str]]:
        if not self.birthdays_report_url:
            raise RuntimeError("MOMENCE_BIRTHDAYS_REPORT_URL is not configured.")
        return self.download_report_csv(self.birthdays_report_url)

    def download_report_csv(self, report_url: str, timeout_ms: int = 30000) -> list[dict[str, str]]:
        with self.browser_context() as context:
            page = context.new_page()
            try:
                page.goto(report_url, wait_until="domcontentloaded", timeout=60000)
            except Exception:
                # Some Momence reports abort while the SPA swaps routes but still land on the right page.
                pass
            page.wait_for_timeout(8000)
            if "/sign-in" in page.url:
                raise RuntimeError("Momence browser session is no longer authenticated.")
            with page.expect_download(timeout=timeout_ms) as pending:
                page.get_by_role("button", name="Download summary").click()
            download = pending.value
            with tempfile.TemporaryDirectory() as temp_dir:
                target = Path(temp_dir) / download.suggested_filename
                download.save_as(str(target))
                with target.open(encoding="utf-8-sig", newline="") as handle:
                    return list(csv.DictReader(handle))
