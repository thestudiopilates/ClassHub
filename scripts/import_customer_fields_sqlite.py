from __future__ import annotations

import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.core.config import settings
from app.db.base import Base
from app.db.session import SessionLocal, engine
from app.services.momence.browser import MomenceBrowserClient
from app.services.sync_state import record_sync_state


FUN_FACT_FIELD = "Tell us one fun fact about you!"
PREGNANT_FIELD = "Pregnant"
PREGNANCY_DUE_DATE_FIELD = "Pregnancy Due Date"
HEARD_ABOUT_US_FIELD = "How Did You Hear About Us?"


def parse_due_date(raw_value: str | None) -> str | None:
    if not raw_value:
        return None
    value = raw_value.strip()
    if not value:
        return None

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def normalize_value(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    value = raw_value.strip()
    return value or None


def main() -> None:
    Base.metadata.create_all(bind=engine)
    database_url = settings.database_url
    if not database_url.startswith("sqlite"):
        raise RuntimeError(f"Expected a SQLite DATABASE_URL for this importer, got: {database_url}")

    db_path = database_url.split("///", 1)[-1]
    db_file = Path(db_path).resolve()
    if not db_file.exists():
        raise RuntimeError(f"SQLite database not found at {db_file}")

    browser = MomenceBrowserClient()
    rows = browser.download_report_csv(settings.momence_customer_field_values_report_url, timeout_ms=120000)
    print(f"Downloaded {len(rows)} customer field rows")

    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    email_to_client_id = {
        row["email"].strip().lower(): row["id"]
        for row in cursor.execute("select id, email from clients where email is not null")
        if row["email"]
    }
    print(f"Loaded {len(email_to_client_id)} client emails from {db_file}")

    profile_by_client: dict[str, dict[str, str | None]] = defaultdict(dict)
    matched_rows = 0

    for row in rows:
        email = normalize_value(row.get("E-mail"))
        field_name = normalize_value(row.get("Field name"))
        field_value = normalize_value(row.get("Field value"))
        if not email or not field_name:
            continue

        client_id = email_to_client_id.get(email.lower())
        if not client_id:
            continue

        matched_rows += 1
        payload = profile_by_client[client_id]

        if field_name == FUN_FACT_FIELD and field_value:
            payload["fun_fact"] = field_value
        elif field_name == PREGNANT_FIELD and field_value:
            payload["pregnant_status"] = field_value
        elif field_name == PREGNANCY_DUE_DATE_FIELD:
            parsed_date = parse_due_date(field_value)
            if parsed_date:
                payload["pregnancy_due_date"] = parsed_date
        elif field_name == HEARD_ABOUT_US_FIELD and field_value:
            payload["heard_about_us"] = field_value

    print(f"Matched {matched_rows} report rows to known clients")
    print(f"Prepared profile payloads for {len(profile_by_client)} clients")

    upsert_sql = """
        insert into client_profile_data (
            client_id,
            fun_fact,
            pregnant_status,
            pregnancy_due_date,
            heard_about_us,
            updated_at
        ) values (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        on conflict(client_id) do update set
            fun_fact = coalesce(excluded.fun_fact, client_profile_data.fun_fact),
            pregnant_status = coalesce(excluded.pregnant_status, client_profile_data.pregnant_status),
            pregnancy_due_date = coalesce(excluded.pregnancy_due_date, client_profile_data.pregnancy_due_date),
            heard_about_us = coalesce(excluded.heard_about_us, client_profile_data.heard_about_us),
            updated_at = CURRENT_TIMESTAMP
    """

    to_upsert = [
        (
            client_id,
            payload.get("fun_fact"),
            payload.get("pregnant_status"),
            payload.get("pregnancy_due_date"),
            payload.get("heard_about_us"),
        )
        for client_id, payload in profile_by_client.items()
        if payload
    ]

    cursor.executemany(upsert_sql, to_upsert)
    conn.commit()
    print(f"Upserted {len(to_upsert)} client_profile_data rows")

    counts = {
        "total_rows": cursor.execute("select count(*) from client_profile_data").fetchone()[0],
        "fun_fact": cursor.execute(
            "select count(*) from client_profile_data where fun_fact is not null and trim(fun_fact) <> ''"
        ).fetchone()[0],
        "pregnant_status": cursor.execute(
            "select count(*) from client_profile_data where pregnant_status is not null and trim(pregnant_status) <> ''"
        ).fetchone()[0],
        "pregnancy_due_date": cursor.execute(
            "select count(*) from client_profile_data where pregnancy_due_date is not null"
        ).fetchone()[0],
        "heard_about_us": cursor.execute(
            "select count(*) from client_profile_data where heard_about_us is not null and trim(heard_about_us) <> ''"
        ).fetchone()[0],
    }
    print(counts)
    conn.close()

    db = SessionLocal()
    try:
        record_sync_state(db, "customer_fields", status="completed", records_processed=counts["total_rows"])
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    main()
