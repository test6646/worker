# fyers-live-worker

Node worker doing **two jobs** in one process:

1. **Live prices** — one market-hours-only upstream Fyers Data Socket fanned out to N browsers (`/ws`).
2. **Symbol sync** — streaming ingest of Fyers symbol master CSVs → Postgres `instruments_staging` → atomic swap into live catalog (`POST /sync`).

Deploy target: Railway / Render / Fly.io / any Node 20+ host.

## Environment variables

Set these on the host (Railway → Variables, Render → Environment):

| Name | Required | Purpose |
|---|---|---|
| `SUPABASE_URL` | yes | Your Supabase project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | yes | For writing ticks/sessions |
| `FYERS_APP_ID` | live prices | Fyers app id (e.g. `ABC123-100`) |
| `FYERS_TICKET_SECRET` | live prices | HMAC secret for browser WS tickets |
| `ALLOWED_ORIGINS` | prod | Comma-separated origins allowed to open WS |
| `PORT` | no | HTTP port (default 3000) |
| `DATABASE_URL` | sync | Postgres pooler URL, e.g. `postgres://postgres.<proj>:<pwd>@aws-0-ap-south-1.pooler.supabase.com:6543/postgres` |

> **Which pooler?** Use the Supabase **Session Pooler** at `...pooler.supabase.com:5432` (username `postgres.<project-ref>`). It is IPv4-reachable — required on Railway/Render/Fly, whose containers cannot route to the IPv6-only `db.<ref>.supabase.co` direct host. Do **not** use the transaction pooler on `:6543`; it drops `SET LOCAL`, which `sync_finalize` depends on for its extended `statement_timeout`.

| `SYNC_SHARED_SECRET` | sync | Bearer token required on `POST /sync`. Must match the `SYNC_SHARED_SECRET` set in Supabase Edge Function secrets. |

## Endpoints

- `GET /health` — uptime + upstream state
- `GET /ws?ticket=…` — WebSocket for live ticks
- `POST /sync` — trigger symbol sync (Auth: `Bearer $SYNC_SHARED_SECRET`)
  - Returns `{ ok: true, started: true }` immediately; work runs in background.
  - Pass `?wait=1` to block until finished (returns full stats).

## Local dev

```bash
cd worker
cp .env.example .env  # fill values
npm install
npm run dev
```

## Deploy (Railway)

1. Create a new Railway project → Deploy from GitHub → point at this repo, root `/worker`.
2. Add every env var from the table above.
3. Railway auto-detects `npm start`. First deploy takes ~1 min.
4. Copy the public URL (e.g. `https://fyers-worker.up.railway.app`) into the Supabase secret `SYNC_WORKER_URL`.

## Deploy (Render)

1. New Web Service → connect repo → Root Directory `worker`.
2. Build command `npm install`, start command `npm start`.
3. Add env vars from the table.
4. Copy the public URL into `SYNC_WORKER_URL` in Supabase.

## Triggering sync

- **Automatic (daily 20:00 IST Mon–Fri):** already scheduled via `pg_cron` → calls `sync-dispatcher` → calls this worker.
- **Manual (admin UI):** existing "Sync symbols" button in Fyers admin page.
- **Manual (curl):**
  ```bash
  curl -X POST "https://yujcehcgqabktetaejrb.supabase.co/functions/v1/sync-dispatcher" \
    -H "apikey: <ANON_KEY>" \
    -H "Authorization: Bearer <ANON_KEY>" \
    -H "Content-Type: application/json" \
    -d '{}'
  ```

  Full `all`, `symbols`, and `eod` syncs are long-running worker jobs. The
  dispatcher starts them and returns immediately; watch `sync_jobs` /
  `eod_sync_runs` for progress. Use `?wait=1` only for short bounded jobs such
  as `{"job":"ipo"}` or `{"job":"mf"}`.

## Live-feed reliability model

- The worker does not open the Fyers DataSocket outside market hours; browsers render EOD/cached data then.
- Fyers SDK access goes through its singleton `getInstance()` contract, guarded by one reconnect state machine.
- Repeated vendor/socket failures open a cooldown circuit instead of hammering Fyers/Railway with thousands of reconnects.
- Symbol gaps are repaired with small per-symbol resubscribe batches during market hours only.
- **Watch progress:**
  ```sql
  select source, status, rows_written, duration_ms, error, heartbeat_at
  from sync_jobs
  where run_id = (select run_id from sync_jobs order by created_at desc limit 1)
  order by created_at;
  ```
