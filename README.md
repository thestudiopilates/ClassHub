# Momence Ops API

Internal API service for:

- staff-facing instructor and front desk views
- birthdays and milestone tracking
- active-client eligibility based on a rolling 180-day window
- targeted syncs from Momence for upcoming bookings and impacted clients

## Stack

- FastAPI
- SQLAlchemy
- Postgres

## Quick start

1. Create a virtual environment and install dependencies.
2. Copy `.env.example` to `.env` and fill in your Momence and Postgres credentials.
3. Run the API:

```bash
uvicorn app.main:app --reload
```

4. Open the docs:

`http://127.0.0.1:8000/docs`

## What is included

- Postgres-ready schema for clients, bookings, notes, flags, milestones, and sync runs
- domain logic for birthdays, churn risk, welcome-back, and active status
- operational endpoints for front desk, instructor, weekly view, and client profile
- response freshness metadata for active customers, birthdays, customer fields, behavior, memberships/notes, bookings, and flags
- admin endpoints to trigger sync stubs
- Momence client scaffold for auth and future endpoint integration

## Notes

- The app creates tables on startup for local development.
- In production, switch to Alembic migrations instead of `Base.metadata.create_all`.
- Momence auth uses API client credentials plus a staff username/password via the documented OAuth password grant.
- The host endpoints currently wired for integration are sessions, session bookings, member detail, member notes, and active memberships.
- A browser-session fallback is now wired for Momence CRM and birthdays report sync using an existing persistent Playwright profile.
- `GET /v1/clients/{momence_member_id}?refresh_context=true` will refresh that client's memberships and notes before returning the profile.
- Broad memberships/notes sweeps are disabled by default. Use targeted refreshes and upcoming-booking driven syncs instead.
- `POST /v1/admin/sync/clients/context` accepts a bounded list of member ids and refuses requests above `MOMENCE_MAX_CONTEXT_REFRESH_BATCH`.
- Upcoming booking sync is designed to prefer a local/exported CSV via `MOMENCE_SESSION_BOOKINGS_CSV_PATH`. Browser report sync for bookings stays off by default.
- `POST /v1/bookings/{booking_id}/check-in` is wired for Momence host check-ins, but it stays disabled until `MOMENCE_ENABLE_CHECK_IN_WRITE=true`.

## Ops automation

The app now supports a production-shaped sync flow for the live board:

- `sync_upcoming_bookings` refreshes the next 7 days of sessions and rosters from the Momence Host API
- `sync_roster_client_history_full` walks the current roster in small batches and backfills only those clients' booking history
- `/v1/demo/data` can optionally trigger a background warm-up if today's roster history is stale

CLI entrypoint:

```bash
python -m app.ops_runner --mode preopen --day today
```

Roster-only history warm:

```bash
python -m app.ops_runner --mode roster-history --day 2026-03-23 --batch-size 15 --max-batches 4
```

Helpful env vars:

- `OPS_ROSTER_HISTORY_BATCH_SIZE=15`
- `OPS_ROSTER_HISTORY_PAUSE_SECONDS=0.3`
- `OPS_AUTO_WARM_ENABLED=true`
- `OPS_AUTO_WARM_MAX_BATCHES=4`
- `OPS_AUTO_WARM_DAY_OFFSET=0`

## Railway deployment

This repo is set up so Railway can run two services from the same codebase:

1. Web service
   Start command:

```bash
bash railway/run-web.sh
```

2. Scheduled ops sync service
   Start command:

```bash
bash railway/run-ops-sync.sh
```

Recommended cron schedule for the sync service:

- pre-open run each morning before classes begin
- intraday run every 2-4 hours during studio hours

Suggested env on the sync service:

- `OPS_AUTOMATION_MODE=preopen`
- `OPS_TARGET_DAY=today`

Full setup notes live in [DEPLOYMENT.md](/Users/kellyjackson/Documents/Codex/Customer%20Birthdays%20and%20Milestone%20/DEPLOYMENT.md).

## GitHub

GitHub Actions CI is included at [.github/workflows/ci.yml](/Users/kellyjackson/Documents/Codex/Customer%20Birthdays%20and%20Milestone%20/.github/workflows/ci.yml).

It currently validates:

- package install
- Python 3.11 compatibility
- `compileall` across `app`
