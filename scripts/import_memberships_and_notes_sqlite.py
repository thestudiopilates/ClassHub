from __future__ import annotations

import sqlite3
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import re

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from app.db.base import Base
from app.services.domain import refresh_all_flags
from app.db.session import SessionLocal, engine
from app.services.momence.browser import MomenceBrowserClient
from app.services.sync_state import record_sync_state


INJURY_PATTERNS = (
    r"\binjur(?:y|ed)\b",
    r"\bsurger(?:y|ies)\b",
    r"\bpain\b",
    r"\bshoulder\b",
    r"\bknee\b",
    r"\bback\b",
    r"\bneck\b",
    r"\bhip\b",
    r"\bankle\b",
    r"\bwrist\b",
    r"\bspine\b",
    r"\bhernia\b",
    r"\bpregnan\w*\b",
    r"\bprenatal\b",
    r"\bpostpartum\b",
    r"\bmodif(?:y|ication|ications)\b",
    r"\blimited mobility\b",
)


def normalize_datetime(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    value = raw_value.strip()
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except ValueError:
        return None


def parse_end_date(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def is_subscription_active(item: dict, now: datetime) -> bool:
    if item.get("isVoided"):
        return False
    end_date = parse_end_date(item.get("endDate"))
    if end_date and end_date < now:
        return False
    return True


def is_credit_active(item: dict, now: datetime) -> bool:
    if item.get("isVoided"):
        return False
    end_date = parse_end_date(item.get("endDate"))
    if end_date and end_date < now:
        return False
    classes_left = item.get("classesLeft")
    money_left = item.get("moneyLeft")
    if classes_left is None and money_left is None:
        return bool(end_date is None or end_date >= now)
    try:
        if classes_left is not None and float(classes_left) > 0:
            return True
    except (TypeError, ValueError):
        pass
    try:
        if money_left is not None and float(money_left) > 0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def choose_active_membership_name(memberships: dict, now: datetime) -> tuple[bool, str | None]:
    subscriptions = memberships.get("subscriptions") or []
    credits = memberships.get("creditsAndEvents") or []
    netflix = memberships.get("netflixSubscriptions") or []

    active_subscriptions = [item for item in subscriptions if is_subscription_active(item, now)]
    active_subscriptions.sort(key=lambda item: parse_end_date(item.get("endDate")) or datetime.max.replace(tzinfo=timezone.utc))
    if active_subscriptions:
        return True, active_subscriptions[0].get("membershipName")

    active_credits = [item for item in credits if is_credit_active(item, now)]
    active_credits.sort(key=lambda item: parse_end_date(item.get("endDate")) or datetime.max.replace(tzinfo=timezone.utc))
    if active_credits:
        return True, active_credits[0].get("membershipName")

    active_netflix = [item for item in netflix if is_subscription_active(item, now)]
    active_netflix.sort(key=lambda item: parse_end_date(item.get("endDate")) or datetime.max.replace(tzinfo=timezone.utc))
    if active_netflix:
        return True, active_netflix[0].get("membershipName")

    return False, None


def is_injury_note(note_text: str) -> bool:
    lowered = note_text.lower()
    return any(re.search(pattern, lowered) for pattern in INJURY_PATTERNS)


def chunked(values: list[tuple[str, str]], chunk_size: int) -> list[list[tuple[str, str]]]:
    return [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]


def fetch_member_context(http: httpx.Client, host_id: int, member_id: str) -> tuple[str, list[dict], dict]:
    notes_response = http.get(f"/_api/primary/host/{host_id}/customer-notes", params={"memberId": member_id})
    memberships_response = http.get(f"/_api/primary/host/{host_id}/customers/{member_id}/memberships")

    notes: list[dict] = []
    memberships: dict = {}

    if notes_response.status_code == 200 and "application/json" in notes_response.headers.get("content-type", ""):
        notes = notes_response.json()
    if memberships_response.status_code == 200 and "application/json" in memberships_response.headers.get(
        "content-type", ""
    ):
        memberships = memberships_response.json()

    return member_id, notes, memberships


def main() -> None:
    Base.metadata.create_all(bind=engine)
    if not settings.momence_allow_broad_context_sync:
        raise RuntimeError(
            "Broad memberships/notes sync is disabled. Use targeted client refreshes or set "
            "MOMENCE_ALLOW_BROAD_CONTEXT_SYNC=true only for a one-off approved run."
        )
    database_url = settings.database_url
    if not database_url.startswith("sqlite"):
        raise RuntimeError(f"Expected a SQLite DATABASE_URL for this importer, got: {database_url}")

    db_path = database_url.split("///", 1)[-1]
    db_file = Path(db_path).resolve()
    if not db_file.exists():
        raise RuntimeError(f"SQLite database not found at {db_file}")

    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    active_clients = [
        (row["id"], row["momence_member_id"])
        for row in cursor.execute(
            """
            select c.id, c.momence_member_id
            from clients c
            join client_flags f on f.client_id = c.id
            where f.is_active_180d = 1
            """
        )
    ]
    print(f"Loaded {len(active_clients)} active clients")

    if not active_clients:
        raise RuntimeError("No active clients found. Run the active client and flag sync first.")

    active_client_ids = [client_id for client_id, _ in active_clients]
    for batch in chunked([(client_id, "") for client_id in active_client_ids], 500):
        ids = [client_id for client_id, _ in batch]
        placeholders = ",".join("?" for _ in ids)
        cursor.execute(
            f"delete from client_notes where client_id in ({placeholders})",
            ids,
        )
        cursor.execute(
            f"update client_activity set has_active_membership = 0, active_membership_name = null where client_id in ({placeholders})",
            ids,
        )
    conn.commit()

    browser = MomenceBrowserClient()
    cookies = browser.get_authenticated_cookies()
    now = datetime.now(timezone.utc)
    total_notes = 0
    members_with_notes = 0
    members_with_active_membership = 0

    member_lookup = {member_id: client_id for client_id, member_id in active_clients}
    member_ids = [member_id for _, member_id in active_clients]

    insert_note_sql = """
        insert into client_notes (
            id,
            client_id,
            note_type,
            note_text,
            is_injury_flag,
            is_front_desk_flag,
            is_instructor_flag,
            source_updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
    """
    update_activity_sql = """
        update client_activity
        set has_active_membership = ?, active_membership_name = ?, activity_updated_at = CURRENT_TIMESTAMP
        where client_id = ?
    """

    progress_interval = 250
    pending_note_rows: list[tuple[str, str, str | None, str, int, int, int, str | None]] = []
    pending_activity_rows: list[tuple[int, str | None, str]] = []
    processed = 0

    with httpx.Client(
        base_url="https://momence.com",
        cookies=cookies,
        timeout=30.0,
        follow_redirects=True,
        headers={"accept": "application/json, text/plain, */*"},
    ) as http:
        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {
                executor.submit(fetch_member_context, http, settings.momence_host_id, member_id): member_id
                for member_id in member_ids
            }
            for future in as_completed(futures):
                member_id, notes, memberships = future.result()
                client_id = member_lookup[member_id]

                has_active_membership, membership_name = choose_active_membership_name(memberships, now)
                pending_activity_rows.append((1 if has_active_membership else 0, membership_name, client_id))
                if has_active_membership:
                    members_with_active_membership += 1

                if notes:
                    members_with_notes += 1
                for note in notes:
                    note_text = (note.get("notePreview") or "").strip()
                    if not note_text:
                        continue
                    injury_flag = 1 if is_injury_note(note_text) else 0
                    pending_note_rows.append(
                        (
                            uuid.uuid4().hex,
                            client_id,
                            note.get("type") or note.get("assignmentType"),
                            note_text,
                            injury_flag,
                            1,
                            injury_flag,
                            normalize_datetime(note.get("modifiedAt") or note.get("createdAt")),
                        )
                    )

                processed += 1
                if processed % progress_interval == 0:
                    cursor.executemany(update_activity_sql, pending_activity_rows)
                    if pending_note_rows:
                        cursor.executemany(insert_note_sql, pending_note_rows)
                    conn.commit()
                    total_notes += len(pending_note_rows)
                    print(
                        f"Processed {processed}/{len(member_ids)} members, "
                        f"inserted {len(pending_note_rows)} notes in this window"
                    )
                    pending_activity_rows = []
                    pending_note_rows = []

    if pending_activity_rows:
        cursor.executemany(update_activity_sql, pending_activity_rows)
    if pending_note_rows:
        cursor.executemany(insert_note_sql, pending_note_rows)
    conn.commit()
    total_notes += len(pending_note_rows)

    conn.close()

    db = SessionLocal()
    try:
        refreshed = refresh_all_flags(db)
        record_sync_state(
            db,
            "memberships_notes",
            status="completed",
            records_processed=members_with_active_membership + members_with_notes,
        )
        record_sync_state(db, "flags", status="completed", records_processed=refreshed)
        db.commit()
    finally:
        db.close()

    verify_conn = sqlite3.connect(str(db_file))
    verify_cursor = verify_conn.cursor()
    notes_count = verify_cursor.execute("select count(*) from client_notes").fetchone()[0]
    membership_count = verify_cursor.execute(
        "select count(*) from client_activity where has_active_membership = 1"
    ).fetchone()[0]
    injury_count = verify_cursor.execute(
        "select count(*) from client_notes where is_injury_flag = 1"
    ).fetchone()[0]
    verify_conn.close()

    print(
        {
            "members_with_notes": members_with_notes,
            "members_with_active_membership": members_with_active_membership,
            "client_notes_rows": notes_count,
            "active_memberships_rows": membership_count,
            "injury_note_rows": injury_count,
            "flags_refreshed": refreshed,
        }
    )


if __name__ == "__main__":
    main()
