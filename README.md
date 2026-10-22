# Clinic Booking API

A booking API for a clinic, with a small web page on top: see open slots, book one, get a confirmation, look up or cancel your bookings.

**Stack:** Python, FastAPI, Postgres (Neon), deployed on Vercel.

## Endpoints

Every API request needs an `X-API-Key` header (missing or wrong key returns 401). Errors are always `{"error": "<message>"}`.

| Method | Path | Level | Notes |
|---|---|---|---|
| POST | `/reset` | 1 | Deletes every booking and replaces all slots |
| GET | `/slots` | 1 | Earliest first, optional `?provider=`, each slot has `available` |
| POST | `/bookings` | 1-5 | Validation, 409 on a taken slot, `Idempotency-Key`, 429 rate limit |
| GET | `/bookings?email=` | 4 | Cursor paging (`limit` 1-50, `cursor`), oldest first |
| GET | `/bookings/{id}` | 1 | |
| DELETE | `/bookings/{id}` | 3 | Cancels and frees the slot (409 if already cancelled) |

## How the hard parts work

- **No double booking (level 3).** A partial unique index (`UNIQUE (slot_id) WHERE status = 'confirmed'`) lets the database decide the winner when requests arrive at the same instant. The loser gets 409.
- **Retries are safe (level 4).** An `Idempotency-Key` is stored with a hash of the request body and the response. Same key and body replays the stored response; same key with a different body is 422. A transaction-scoped advisory lock on the key makes two simultaneous retries safe.
- **Rate limit (level 5).** Attempts are stored per email. A per-email advisory lock keeps the count exact under bursts, so the 6th attempt in a minute is 429 with a whole-second `Retry-After`. State lives in Postgres, so it holds across serverless instances.
- **The web page** calls `/web/*`, which runs the same handlers but adds no key requirement and does not expose `/reset`. The key-protected API is unchanged.

## Run locally

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
export DATABASE_URL=postgresql://user:pass@localhost:5432/clinic API_KEY=choose-a-key
.venv/bin/uvicorn main:app --port 3000
```

Check everything end to end (this calls `/reset`, so use a throwaway database):

```bash
BASE=http://localhost:3000 API_KEY=choose-a-key .venv/bin/python tests/check_api.py
```

Tables are created automatically on first start.

## Deploy

Set `API_KEY` and `DATABASE_URL` as environment variables on Vercel. Use Neon's pooled connection string; the app disables prepared statements so it works behind PgBouncer.
