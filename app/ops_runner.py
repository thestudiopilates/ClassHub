from __future__ import annotations

import argparse
from datetime import date, datetime

from app.db.session import SessionLocal
from app.services.automation import run_preopen_ops_sync, sync_roster_client_history_full


def _parse_day(raw: str | None) -> date | None:
    if not raw or raw == "today":
        return datetime.now().date()
    return date.fromisoformat(raw)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run scheduled Momence ops sync jobs.")
    parser.add_argument("--mode", choices=["preopen", "roster-history"], default="preopen")
    parser.add_argument("--day", default="today")
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args()

    target_day = _parse_day(args.day)
    db = SessionLocal()
    try:
        if args.mode == "preopen":
            results = run_preopen_ops_sync(db, day=target_day)
            for name, result in results.items():
                print(f"{name}: status={result.status} records={result.records_processed}")
                if result.status != "completed":
                    print(result.error_text or "")
                    return 1
            return 0

        result = sync_roster_client_history_full(
            db,
            day=target_day,
            batch_size=args.batch_size,
            max_batches=args.max_batches,
        )
        print(f"{result.job_name}: status={result.status} records={result.records_processed}")
        if result.status != "completed":
            print(result.error_text or "")
            return 1
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
