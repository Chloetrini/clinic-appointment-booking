"""Clinic booking API (FastAPI + Postgres).

The rules that must never break live in the database, not in application code:
  * a partial unique index allows one confirmed booking per slot (level 3)
  * advisory locks serialise idempotency-key and per-email rate-limit checks (levels 4, 5)
"""
import base64
import hashlib
import hmac
import json
import math
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
from starlette.exceptions import HTTPException as StarletteHTTPException

API_KEY = os.environ.get("API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/clinic")
POOL_MAX = int(os.environ.get("PG_POOL_MAX", "5"))

RATE_LIMIT = 5
RATE_WINDOW_SECONDS = 60
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@.]+(\.[^\s@.]+)+$")
INDEX_HTML = Path(__file__).parent / "public" / "index.html"

SCHEMA = """
CREATE TABLE IF NOT EXISTS slots (
  id TEXT PRIMARY KEY,
  provider TEXT NOT NULL,
  start_raw TEXT NOT NULL,
  end_raw TEXT NOT NULL,
  start_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS bookings (
  seq BIGSERIAL UNIQUE,
  id TEXT PRIMARY KEY,
  slot_id TEXT NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS one_confirmed_per_slot ON bookings (slot_id) WHERE status = 'confirmed';
CREATE INDEX IF NOT EXISTS bookings_email_seq ON bookings (email, seq);
CREATE TABLE IF NOT EXISTS idempotency (
  key TEXT PRIMARY KEY,
  body_hash TEXT NOT NULL,
  status INT NOT NULL,
  response JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
  email TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS attempts_email_at ON attempts (email, at);
"""

# ---------- database ----------
_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def pool() -> ConnectionPool:
    """Open the pool and make sure the schema exists, once per process."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                p = ConnectionPool(
                    DATABASE_URL,
                    min_size=1,
                    max_size=POOL_MAX,
                    # pgbouncer (Neon's pooled endpoint) cannot keep server-side prepared statements
                    kwargs={"prepare_threshold": None, "row_factory": dict_row},
                    open=True,
                    timeout=20,
                )
                _ensure_schema(p)
                _pool = p
    return _pool


def _ensure_schema(p: ConnectionPool) -> None:
    # DDL takes table locks even when it is a no-op, so only run it when the schema is missing
    def has(conn) -> bool:
        return conn.execute("SELECT to_regclass('attempts_email_at') IS NOT NULL AS ok").fetchone()["ok"]

    with p.connection() as conn:
        if has(conn):
            return
        conn.execute("SELECT pg_advisory_xact_lock(727001)")
        if not has(conn):
            conn.execute(SCHEMA)


# ---------- helpers ----------
def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def booking_json(r: dict) -> dict:
    return {"id": r["id"], "slot_id": r["slot_id"], "name": r["name"], "email": r["email"],
            "status": r["status"], "created_at": iso(r["created_at"])}


def fail(status: int, message: str, headers: dict | None = None) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status, headers=headers)


def canonical(body: Any) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def parse_time(value: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def key_ok(given: str | None) -> bool:
    if not API_KEY or given is None:
        return False
    a = hashlib.sha256(given.encode()).digest()
    b = hashlib.sha256(API_KEY.encode()).digest()
    return hmac.compare_digest(a, b)


# ---------- app ----------
app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def require_key(request: Request, call_next):
    path = request.url.path
    # the page, health check and the page's own /web/* routes are public; everything else needs the key,
    # checked before the request body is even parsed
    if path in ("/", "/healthz") or path.startswith("/web/") or key_ok(request.headers.get("x-api-key")):
        return await call_next(request)
    return fail(401, "missing or invalid API key")


@app.exception_handler(StarletteHTTPException)
async def http_error(request: Request, exc: StarletteHTTPException):
    messages = {404: "not found", 405: "method not allowed"}
    return fail(exc.status_code, messages.get(exc.status_code, str(exc.detail)))


@app.exception_handler(RequestValidationError)
async def bad_body(request: Request, exc: RequestValidationError):
    return fail(400, "invalid JSON body")


@app.exception_handler(Exception)
async def crash(request: Request, exc: Exception):
    print("unhandled error:", repr(exc), flush=True)
    return fail(500, "internal error")


@app.get("/")
def home():
    return FileResponse(INDEX_HTML)


@app.get("/healthz")
def healthz():
    return {"ok": True}


api = APIRouter()   # needs the key
web = APIRouter()   # used by the page: same handlers, no /reset


def both(path: str, methods: list[str], fn) -> None:
    api.add_api_route(path, fn, methods=methods)
    web.add_api_route(path, fn, methods=methods)


# ---------- level 1: slots ----------
def list_slots(request: Request):
    provider = request.query_params.get("provider")
    sql = ("SELECT s.*, NOT EXISTS (SELECT 1 FROM bookings b WHERE b.slot_id = s.id AND b.status = 'confirmed') "
           "AS available FROM slots s {where} ORDER BY s.start_at, s.id")
    with pool().connection() as conn:
        if provider is not None:
            rows = conn.execute(sql.format(where="WHERE s.provider = %s"), (provider,)).fetchall()
        else:
            rows = conn.execute(sql.format(where="")).fetchall()
    return {"slots": [{"id": r["id"], "provider": r["provider"], "start": r["start_raw"],
                       "end": r["end_raw"], "available": r["available"]} for r in rows]}


def reset(body: Any = Body(default=None)):
    slots = body.get("slots") if isinstance(body, dict) else None
    if not isinstance(slots, list):
        return fail(400, "slots must be an array")
    seen, rows = set(), []
    for s in slots:
        if not isinstance(s, dict) or not all(isinstance(s.get(k), str) and s[k].strip()
                                             for k in ("id", "provider", "start", "end")):
            return fail(400, "each slot needs id, provider, start and end")
        start, end = parse_time(s["start"]), parse_time(s["end"])
        if start is None or end is None:
            return fail(400, "invalid slot date")
        if s["id"] in seen:
            return fail(400, "duplicate slot id: " + s["id"])
        seen.add(s["id"])
        rows.append((s["id"], s["provider"], s["start"], s["end"], start))
    with pool().connection() as conn:
        conn.execute("TRUNCATE bookings, slots, idempotency, attempts")
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO slots (id, provider, start_raw, end_raw, start_at) "
                            "VALUES (%s, %s, %s, %s, %s)", rows)
    return {"ok": True, "slots": len(rows)}


# ---------- levels 2-5: create a booking ----------
def place_booking(conn, body: dict) -> tuple[int, dict]:
    slot_id = body.get("slot_id")
    name = body["name"].strip() if isinstance(body.get("name"), str) else ""
    email = body["email"].strip().lower() if isinstance(body.get("email"), str) else ""
    if not isinstance(slot_id, str) or not slot_id.strip() or not name or not email:
        return 400, {"error": "slot_id, name and email are required"}
    if not EMAIL_RE.match(email):
        return 400, {"error": "email is not valid"}
    slot = conn.execute("SELECT id, start_at FROM slots WHERE id = %s", (slot_id,)).fetchone()
    if not slot:
        return 404, {"error": "slot not found"}
    if slot["start_at"].timestamp() <= time.time():
        return 400, {"error": "slot has already started"}
    try:
        with conn.transaction():  # savepoint, so a lost race does not poison the outer transaction
            row = conn.execute(
                "INSERT INTO bookings (id, slot_id, name, email, status) VALUES (%s, %s, %s, %s, 'confirmed') RETURNING *",
                (str(uuid.uuid4()), slot_id, name, email)).fetchone()
        return 201, booking_json(row)
    except UniqueViolation:
        return 409, {"error": "slot already booked"}


def create_booking(request: Request, body: Any = Body(default=None)):
    if not isinstance(body, dict):
        return fail(400, "body must be a JSON object")
    idem_key = request.headers.get("idempotency-key") or None
    raw_email = body.get("email")
    rate_email = raw_email.strip().lower() if isinstance(raw_email, str) and raw_email.strip() else None

    with pool().connection() as conn:
        if rate_email:  # level 5: at most 5 attempts per email per minute, failures included
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("rl:" + rate_email,))
            conn.execute("DELETE FROM attempts WHERE email = %s AND at < clock_timestamp() - make_interval(secs => %s)",
                         (rate_email, RATE_WINDOW_SECONDS))
            rows = conn.execute("SELECT at FROM attempts WHERE email = %s ORDER BY at", (rate_email,)).fetchall()
            if len(rows) >= RATE_LIMIT:
                wait = max(1, math.ceil(rows[0]["at"].timestamp() + RATE_WINDOW_SECONDS - time.time()))
                return fail(429, "too many booking attempts for this email, try again later",
                            {"Retry-After": str(wait)})
            conn.execute("INSERT INTO attempts (email) VALUES (%s)", (rate_email,))

        body_hash = None
        if idem_key:  # level 4: same key + same body replays the stored response
            conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("idem:" + idem_key,))
            body_hash = hashlib.sha256(canonical(body).encode()).hexdigest()
            prev = conn.execute("SELECT body_hash, status, response FROM idempotency WHERE key = %s",
                                (idem_key,)).fetchone()
            if prev:
                if prev["body_hash"] != body_hash:
                    return fail(422, "Idempotency-Key was already used with a different request body")
                return JSONResponse(prev["response"], status_code=prev["status"])

        status, payload = place_booking(conn, body)
        if idem_key:
            conn.execute("INSERT INTO idempotency (key, body_hash, status, response) VALUES (%s, %s, %s, %s)",
                         (idem_key, body_hash, status, Jsonb(payload)))
    return JSONResponse(payload, status_code=status)


# ---------- level 4: a person's bookings ----------
def list_bookings(request: Request):
    q = request.query_params
    email = q.get("email")
    if email is None or not email.strip():
        return fail(400, "email is required")
    limit = 10
    if "limit" in q:
        raw = q["limit"]
        if not re.fullmatch(r"\d+", raw) or not 1 <= int(raw) <= 50:
            return fail(400, "limit must be an integer from 1 to 50")
        limit = int(raw)
    after = 0
    if "cursor" in q:
        try:
            decoded = base64.urlsafe_b64decode(q["cursor"] + "=" * (-len(q["cursor"]) % 4)).decode()
        except Exception:
            decoded = ""
        if not re.fullmatch(r"\d+", decoded):
            return fail(400, "invalid cursor")
        after = int(decoded)
    with pool().connection() as conn:
        rows = conn.execute("SELECT * FROM bookings WHERE email = %s AND seq > %s ORDER BY seq LIMIT %s",
                            (email.strip().lower(), after, limit + 1)).fetchall()
    page = rows[:limit]
    nxt = base64.urlsafe_b64encode(str(page[-1]["seq"]).encode()).decode().rstrip("=") if len(rows) > limit else None
    return {"bookings": [booking_json(r) for r in page], "next_cursor": nxt}


# ---------- level 1 + 3: one booking, cancel ----------
def get_booking(id: str):
    with pool().connection() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id = %s", (id,)).fetchone()
    return booking_json(row) if row else fail(404, "booking not found")


def cancel_booking(id: str):
    with pool().connection() as conn:
        row = conn.execute("UPDATE bookings SET status = 'cancelled' WHERE id = %s AND status = 'confirmed' RETURNING *",
                           (id,)).fetchone()
        if row:
            return booking_json(row)
        exists = conn.execute("SELECT 1 FROM bookings WHERE id = %s", (id,)).fetchone()
    return fail(409, "booking is already cancelled") if exists else fail(404, "booking not found")


both("/slots", ["GET"], list_slots)
both("/bookings", ["POST"], create_booking)
both("/bookings", ["GET"], list_bookings)
both("/bookings/{id}", ["GET"], get_booking)
both("/bookings/{id}", ["DELETE"], cancel_booking)
api.add_api_route("/reset", reset, methods=["POST"])

app.include_router(api)
app.include_router(web, prefix="/web")
