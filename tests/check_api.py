"""End-to-end checks for all five levels.

    BASE=http://localhost:3000 API_KEY=... python tests/check_api.py

Needs only the standard library. It calls /reset, so point it at a throwaway or demo database.
"""
import json
import os
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

BASE = os.environ.get("BASE", "http://localhost:3000").rstrip("/")
KEY = os.environ.get("API_KEY", "testkey")
failures = 0


def call(method, path, body=None, headers=None, key=KEY):
    h = {"Content-Type": "application/json", **(headers or {})}
    if key is not None:
        h["X-API-Key"] = key
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or b"null"), r.headers
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else None), e.headers


def check(name, got, want):
    global failures
    ok = got == want
    failures += 0 if ok else 1
    print(("ok   " if ok else "FAIL ") + name + ("" if ok else f"  got {got!r} want {want!r}"))


def future(hours):
    return (datetime.now(timezone.utc) + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    slots = [{"id": "past", "provider": "Dr Ade", "start": "2020-01-01T09:00:00Z", "end": "2020-01-01T09:30:00Z"}]
    slots += [{"id": f"s{i}", "provider": "Dr Ade" if i % 2 else "Dr Bo",
               "start": future(24 + i), "end": future(24.5 + i)} for i in range(12)]

    # level 2: key
    check("401 no key", call("GET", "/slots", key=None)[0], 401)
    check("401 bad key", call("GET", "/slots", key="nope")[0], 401)
    check("401 reset no key", call("POST", "/reset", {"slots": []}, key=None)[0], 401)

    # level 1
    check("reset", call("POST", "/reset", {"slots": slots})[0], 200)
    _, j, _ = call("GET", "/slots")
    check("slots count + earliest first", (len(j["slots"]), j["slots"][0]["id"]), (13, "past"))
    _, j, _ = call("GET", "/slots?provider=Dr%20Bo")
    check("provider filter", all(s["provider"] == "Dr Bo" for s in j["slots"]), True)

    # level 2: validation
    check("blank name", call("POST", "/bookings", {"slot_id": "s0", "name": " ", "email": "a@b.co"})[0], 400)
    check("bad email", call("POST", "/bookings", {"slot_id": "s0", "name": "A", "email": "nope"})[0], 400)
    check("unknown slot", call("POST", "/bookings", {"slot_id": "zz", "name": "A", "email": "a@b.co"})[0], 404)
    check("past slot", call("POST", "/bookings", {"slot_id": "past", "name": "A", "email": "a@b.co"})[0], 400)
    s, j, _ = call("POST", "/bookings", {"slot_id": "s0", "name": "  Ada ", "email": " ADA@Example.com "})
    check("book", (s, j["name"], j["email"], j["status"]), (201, "Ada", "ada@example.com", "confirmed"))
    bid = j["id"]
    check("get booking", call("GET", f"/bookings/{bid}")[1]["id"], bid)
    check("get 404", call("GET", "/bookings/nope")[0], 404)
    check("slot unavailable", next(x for x in call("GET", "/slots")[1]["slots"] if x["id"] == "s0")["available"], False)

    # level 3
    check("taken -> 409", call("POST", "/bookings", {"slot_id": "s0", "name": "B", "email": "b@b.co"})[0], 409)
    check("cancel", call("DELETE", f"/bookings/{bid}")[1]["status"], "cancelled")
    check("cancel again -> 409", call("DELETE", f"/bookings/{bid}")[0], 409)
    check("cancel unknown -> 404", call("DELETE", "/bookings/nope")[0], 404)
    check("rebook after cancel", call("POST", "/bookings", {"slot_id": "s0", "name": "B", "email": "b@b.co"})[0], 201)

    with ThreadPoolExecutor(25) as ex:  # 25 different people, one slot, same moment
        codes = list(ex.map(lambda i: call("POST", "/bookings", {"slot_id": "s1", "name": f"P{i}", "email": f"race{i}@x.io"})[0], range(25)))
    check("race: exactly one 201", codes.count(201), 1)
    check("race: the rest 409", codes.count(409), 24)

    # level 4: idempotency
    body = {"slot_id": "s2", "name": "Ida", "email": "ida@x.io"}
    with ThreadPoolExecutor(2) as ex:
        a, b = list(ex.map(lambda _: call("POST", "/bookings", body, {"Idempotency-Key": "k1"}), range(2)))
    check("same key, same body", (a[0], b[0], a[1]["id"] == b[1]["id"]), (201, 201, True))
    check("same key, other body -> 422", call("POST", "/bookings", {**body, "name": "Other"}, {"Idempotency-Key": "k1"})[0], 422)
    check("failed response is replayed too", call("POST", "/bookings", {"slot_id": "s2", "name": "Z", "email": "z@x.io"}, {"Idempotency-Key": "k2"})[0], 409)

    # level 4: paging (5 bookings, 2 per page)
    for i in range(3, 8):
        call("POST", "/bookings", {"slot_id": f"s{i}", "name": "Pg", "email": "PAGE@x.io"})
    seen, cursor, pages = [], None, 0
    while True:
        _, j, _ = call("GET", "/bookings?email=page@X.io&limit=2" + (f"&cursor={cursor}" if cursor else ""))
        seen += j["bookings"]
        cursor, pages = j["next_cursor"], pages + 1
        if not cursor:
            break
    check("paging", (len(seen), pages, len({x["id"] for x in seen})), (5, 3, 5))
    for bad in ("0", "51", "abc"):
        check(f"limit={bad} -> 400", call("GET", f"/bookings?email=a@b.co&limit={bad}")[0], 400)

    # level 5: rate limit
    rl = [call("POST", "/bookings", {"slot_id": "zz", "name": "R", "email": "rl@x.io"}) for _ in range(6)]
    check("rate limit codes", [r[0] for r in rl], [404, 404, 404, 404, 404, 429])
    check("Retry-After is whole seconds", (rl[5][2].get("Retry-After") or "").isdigit(), True)
    check("other email unaffected", call("POST", "/bookings", {"slot_id": "zz", "name": "R", "email": "other@x.io"})[0], 404)
    with ThreadPoolExecutor(15) as ex:
        burst = list(ex.map(lambda _: call("POST", "/bookings", {"slot_id": "zz", "name": "B", "email": "burst@x.io"})[0], range(15)))
    check("burst of 15: 5 through, 10 blocked", (burst.count(404), burst.count(429)), (5, 10))

    # page route: works without a key, but cannot reset
    check("/web/slots needs no key", call("GET", "/web/slots", key=None)[0], 200)
    check("/web/reset is not exposed", call("POST", "/web/reset", {"slots": []}, key=None)[0], 404)

    print("\nall passed" if not failures else f"\n{failures} FAILED")
    sys.exit(1 if failures else 0)


main()
