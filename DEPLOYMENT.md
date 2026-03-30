# Railway Deployment

## Services

Create 2 Railway services from the same GitHub repo:

1. `classhub-web`
2. `classhub-sync`

## Web Service

Repo:

- `thestudiopilates/ClassHub`

Runtime:

- `runtime.txt` pins Python 3.11 for Railway/Nixpacks
- `Dockerfile` is included and should be preferred for Railway deploys

Start command:

```bash
bash railway/run-web.sh
```

Healthcheck path:

```text
/health
```

## Sync Service

Repo:

- `thestudiopilates/ClassHub`

Build:

- if Railway asks, use the repo `Dockerfile`

Start command:

```bash
bash railway/run-ops-sync.sh
```

Recommended env:

```text
OPS_AUTOMATION_MODE=intraday
OPS_TARGET_DAY=today
```

Recommended schedule:

- one pre-open run before the first classes each day using `OPS_AUTOMATION_MODE=preopen`
- one intraday sync every 2-4 hours during studio hours using `OPS_AUTOMATION_MODE=intraday`

## Shared Environment Variables

Set these on both services:

```text
DATABASE_URL=
MOMENCE_BASE_URL=https://api.momence.com
MOMENCE_CLIENT_ID=
MOMENCE_CLIENT_SECRET=
MOMENCE_REDIRECT_URI=https://YOUR-WEB-DOMAIN/v1/auth/momence/callback
MOMENCE_HOST_ID=29863
MOMENCE_TOKEN_STORE_PATH=.momence_tokens.json
DEFAULT_TIMEZONE=America/New_York
MOMENCE_UPCOMING_BOOKING_DAYS=7
MOMENCE_HISTORY_BOOKING_DAYS=60
MOMENCE_HISTORY_BOOKING_CHUNK_DAYS=1
MOMENCE_ENABLE_CHECK_IN_WRITE=false
OPS_ROSTER_HISTORY_BATCH_SIZE=15
OPS_ROSTER_HISTORY_PAUSE_SECONDS=0.3
OPS_ROSTER_HISTORY_PREOPEN_MAX_BATCHES=4
OPS_ROSTER_HISTORY_INTRADAY_MAX_BATCHES=1
OPS_AUTO_WARM_ENABLED=true
OPS_AUTO_WARM_MAX_BATCHES=4
OPS_AUTO_WARM_DAY_OFFSET=0
```

## Supabase Postgres On Railway

This app talks to Postgres through `DATABASE_URL`. You do not need `@supabase/supabase-js`, Next.js middleware, or browser/client helpers unless you later add a separate frontend.

For a Supabase-backed Railway deploy:

1. copy the Supabase Postgres connection string
2. put it into `DATABASE_URL` on both `classhub-web` and `classhub-sync`
3. make sure the URL includes `sslmode=require`

Typical Supabase pooled format:

```text
DATABASE_URL=postgresql://postgres.<project-ref>:<password>@aws-0-us-east-1.pooler.supabase.com:6543/postgres?sslmode=require
```

Notes:

- Railway can use either `postgresql://...` or `postgres://...`; the app normalizes both to SQLAlchemy/psycopg format automatically
- use the same `DATABASE_URL` on both Railway services so the web app and sync worker point at the same Supabase database
- if Supabase gives you a direct connection string instead of the pooler, still keep `sslmode=require`

## Post-Deploy OAuth

After the web service gets a public Railway URL:

1. update your Momence API app redirect URI to:

```text
https://YOUR-WEB-DOMAIN/v1/auth/momence/callback
```

2. open:

```text
https://YOUR-WEB-DOMAIN/v1/auth/momence/login
```

3. complete authorization

4. verify:

```text
https://YOUR-WEB-DOMAIN/v1/auth/momence/status
```

## Useful Hosted Endpoints

- `GET /health`
- `GET /v1/demo`
- `GET /v1/demo/data`
- `POST /v1/admin/sync/preopen`
- `POST /v1/admin/sync/upcoming-bookings`
- `POST /v1/admin/sync/roster-history`
