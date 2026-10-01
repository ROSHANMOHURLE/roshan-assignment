"""Seat reservation service. All correctness decisions are made atomically inside Postgres."""
import asyncio, contextvars, hashlib, json, logging, os, random, re, time, uuid
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:pg@localhost:5432/seats")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "admin-secret")
POOL_MAX = int(os.getenv("POOL_MAX", "20"))
DEFAULT_LIMIT = 4

# ---------------------------------------------------------------- logging
rid_var: contextvars.ContextVar[str] = contextvars.ContextVar("rid", default="-")


class JsonFmt(logging.Formatter):
    def format(self, r):
        d = {"ts": self.formatTime(r, "%Y-%m-%dT%H:%M:%S"), "level": r.levelname,
             "msg": r.getMessage(), "request_id": rid_var.get()}
        d.update(getattr(r, "f", {}))
        return json.dumps(d)


_h = logging.StreamHandler()
_h.setFormatter(JsonFmt())
log = logging.getLogger("seats")
log.addHandler(_h)
log.setLevel(logging.INFO)
log.propagate = False

# ---------------------------------------------------------------- metrics
CONFIRMED = Counter("reservations_confirmed_total", "Reservations confirmed")
CANCELLED = Counter("reservations_cancelled_total", "Reservations cancelled")
DECLINED = Counter("reservations_declined_total", "Reservations declined", ["reason"])
for _r in ("seat_taken", "per_user_limit", "idempotent_replay", "idempotency_key_conflict", "invalid_seat"):
    DECLINED.labels(_r)
HTTP = Counter("http_requests_total", "HTTP requests", ["route", "method", "status"])
LAT = Histogram("http_request_duration_seconds", "Latency", ["route"],
                buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30))
INFLIGHT = Gauge("http_inflight_requests", "In-flight requests")

# ---------------------------------------------------------------- db
SCHEMA = """
CREATE TABLE IF NOT EXISTS shows(
  id uuid PRIMARY KEY, name text NOT NULL, price_paise bigint NOT NULL CHECK (price_paise >= 0),
  per_user_limit int NOT NULL CHECK (per_user_limit > 0), total_seats int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS reservations(
  id uuid PRIMARY KEY, show_id uuid NOT NULL REFERENCES shows(id), user_id text NOT NULL,
  seats text[] NOT NULL, amount_paise bigint NOT NULL,
  status text NOT NULL CHECK (status IN ('confirmed','cancelled')),
  idempotency_key text NOT NULL, request_hash text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (user_id, idempotency_key));
CREATE INDEX IF NOT EXISTS res_show_user ON reservations(show_id, user_id);
CREATE TABLE IF NOT EXISTS seats(
  show_id uuid NOT NULL REFERENCES shows(id), label text NOT NULL,
  status text NOT NULL DEFAULT 'available' CHECK (status IN ('available','held','confirmed')),
  reservation_id uuid, user_id text, updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (show_id, label),
  CHECK ((status='available') = (reservation_id IS NULL)));
"""

POOL = None
HPOOL = None  # dedicated 1-connection pool so health checks work even when the main pool is saturated
_pool_lock = asyncio.Lock()


async def get_pool():
    global POOL, HPOOL
    if POOL is None:
        async with _pool_lock:
            if POOL is None:
                p = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=POOL_MAX,
                                              command_timeout=60, timeout=5)
                async with p.acquire() as c:
                    await c.execute("SELECT pg_advisory_lock(7001)")
                    await c.execute(SCHEMA)
                    await c.execute("SELECT pg_advisory_unlock(7001)")
                HPOOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=1, timeout=3)
                POOL = p
    return POOL


@asynccontextmanager
async def lifespan(app):
    for _ in range(30):  # survive cold start where DB comes up after us
        try:
            await get_pool()
            break
        except Exception as e:
            log.warning("db not ready", extra={"f": {"err": str(e)}})
            await asyncio.sleep(1)
    yield
    if POOL:
        await POOL.close()


app = FastAPI(title="seat-reservation", lifespan=lifespan)


class Decline(Exception):
    def __init__(self, status, reason, detail=""):
        self.status, self.reason, self.detail = status, reason, detail


def err(status, reason, detail=""):
    return JSONResponse({"error": reason, "detail": detail, "request_id": rid_var.get()}, status_code=status)


@app.exception_handler(Decline)
async def _decline(_, e: Decline):
    return err(e.status, e.reason, e.detail)


@app.exception_handler(asyncpg.PostgresConnectionError)
@app.exception_handler(asyncpg.InterfaceError)
@app.exception_handler(asyncio.TimeoutError)
@app.exception_handler(OSError)
async def _dep_down(_, e):
    log.error("dependency unavailable", extra={"f": {"err": repr(e)}})
    return err(503, "dependency_unavailable")


@app.exception_handler(Exception)
async def _boom(_, e):
    log.exception("unhandled", extra={"f": {"err": repr(e)}})
    return err(500, "internal_error")


@app.middleware("http")
async def mw(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
    rid_var.set(rid)
    t0 = time.perf_counter()
    INFLIGHT.inc()
    try:
        resp = await call_next(request)
    finally:
        INFLIGHT.dec()
    dt = time.perf_counter() - t0
    route = getattr(request.scope.get("route"), "path", "unmatched")
    HTTP.labels(route, request.method, str(resp.status_code)).inc()
    LAT.labels(route).observe(dt)
    resp.headers["x-request-id"] = rid
    if route not in ("/metrics", "/healthz"):
        log.info("request", extra={"f": {"method": request.method, "path": request.url.path,
                                         "route": route, "status": resp.status_code,
                                         "ms": round(dt * 1000, 1), "user": getattr(request.state, "user", None)}})
    return resp


# ---------------------------------------------------------------- auth
USER_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")


def auth(authorization, request: Request, admin=False):
    """Token scheme: 'Bearer <ADMIN_TOKEN>' = admin; 'Bearer user:<id>' = user <id>.
    Identity is ALWAYS derived from here, never from the body."""
    if not authorization or not authorization.lower().startswith("bearer "):
        raise Decline(401, "unauthorized", "missing bearer token")
    tok = authorization[7:].strip()
    if tok == ADMIN_TOKEN:
        if not admin:
            raise Decline(403, "forbidden", "admin token cannot act as a user")
        return "admin"
    if admin:
        raise Decline(403, "forbidden", "admin only")
    if tok.startswith("user:") and USER_RE.match(tok[5:]):
        request.state.user = tok[5:]
        return tok[5:]
    raise Decline(401, "unauthorized", "invalid token")


def parse_uuid(s):
    try:
        return uuid.UUID(s)
    except ValueError:
        raise Decline(404, "not_found", "no such id")


async def with_retry(fn):
    """Retry only on Postgres-detected deadlock/serialization failures (safety net; lock ordering should prevent them)."""
    for i in range(6):
        try:
            return await fn()
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            if i == 5:
                raise
            await asyncio.sleep(random.random() * 0.02 * (i + 1))


# ---------------------------------------------------------------- models
class ShowIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    seats: list[str] = Field(min_length=1, max_length=100000)
    price_paise: int = Field(ge=0)
    per_user_limit: int = Field(default=DEFAULT_LIMIT, ge=1, le=100)


class ReserveIn(BaseModel):
    seats: list[str] = Field(min_length=1, max_length=50)
    idempotency_key: str | None = Field(default=None, max_length=200)
    # unknown fields (e.g. a spoofed user_id) are ignored: identity comes from the token only


# ---------------------------------------------------------------- endpoints
async def show_state(conn, sid):
    async with conn.transaction(isolation="repeatable_read", readonly=True):  # one snapshot => invariant exact
        show = await conn.fetchrow("SELECT * FROM shows WHERE id=$1", sid)
        if not show:
            raise Decline(404, "not_found", "no such show")
        rows = await conn.fetch("SELECT label, status FROM seats WHERE show_id=$1 ORDER BY label", sid)
    counts = {"available": 0, "held": 0, "confirmed": 0}
    for r in rows:
        counts[r["status"]] += 1
    return {"id": str(sid), "name": show["name"], "price_paise": show["price_paise"],
            "per_user_limit": show["per_user_limit"], "total_seats": show["total_seats"],
            "counts": counts, "seats": {r["label"]: r["status"] for r in rows}}


@app.post("/shows", status_code=201)
async def create_show(body: ShowIn, request: Request, authorization: str | None = Header(None)):
    auth(authorization, request, admin=True)
    if len(set(body.seats)) != len(body.seats) or any(not s.strip() for s in body.seats):
        raise Decline(422, "invalid_seats", "seat labels must be unique and non-empty")
    sid = uuid.uuid4()
    pool = await get_pool()
    async with pool.acquire(timeout=60) as conn:
        async with conn.transaction():
            await conn.execute("INSERT INTO shows(id,name,price_paise,per_user_limit,total_seats) VALUES($1,$2,$3,$4,$5)",
                               sid, body.name, body.price_paise, body.per_user_limit, len(body.seats))
            await conn.execute("INSERT INTO seats(show_id,label) SELECT $1, unnest($2::text[])", sid, body.seats)
        out = await show_state(conn, sid)
    log.info("show created", extra={"f": {"show_id": str(sid), "seats": len(body.seats)}})
    return out


@app.get("/shows/{show_id}")
async def get_show(show_id: str):
    sid = parse_uuid(show_id)
    pool = await get_pool()
    async with pool.acquire(timeout=60) as conn:
        return await show_state(conn, sid)


CLAIM_SQL = """
WITH c AS (
  SELECT label FROM seats
  WHERE show_id=$1 AND label = ANY($2::text[]) AND status='available'
  ORDER BY label FOR UPDATE)                       -- deterministic lock order => no deadlock
UPDATE seats s SET status='confirmed', reservation_id=$3, user_id=$4, updated_at=now()
FROM c WHERE s.show_id=$1 AND s.label=c.label
RETURNING s.label"""


def res_json(r):
    return {"reservation_id": str(r["id"]), "show_id": str(r["show_id"]), "user_id": r["user_id"],
            "seats": list(r["seats"]), "amount_paise": r["amount_paise"], "status": r["status"]}


async def _reserve_tx(conn, sid, user, seats, key, rhash):
    async with conn.transaction():
        # serialise concurrent retries of the SAME key (per user)
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", f"idem:{user}:{key}")
        prev = await conn.fetchrow("SELECT * FROM reservations WHERE user_id=$1 AND idempotency_key=$2", user, key)
        if prev:
            if prev["request_hash"] != rhash:
                raise Decline(409, "idempotency_key_conflict", "key already used with a different request")
            return "replay", prev
        show = await conn.fetchrow("SELECT price_paise, per_user_limit FROM shows WHERE id=$1", sid)
        if not show:
            raise Decline(404, "not_found", "no such show")
        # serialise all of one user's reservations on one show => per-user limit is race-free
        await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", f"quota:{sid}:{user}")
        held = await conn.fetchval("SELECT coalesce(sum(cardinality(seats)),0) FROM reservations "
                                   "WHERE show_id=$1 AND user_id=$2 AND status='confirmed'", sid, user)
        if held + len(seats) > show["per_user_limit"]:
            raise Decline(409, "per_user_limit", f"limit {show['per_user_limit']}, already holding {held}")
        rid = uuid.uuid4()
        amount = show["price_paise"] * len(seats)
        await conn.execute("INSERT INTO reservations(id,show_id,user_id,seats,amount_paise,status,idempotency_key,request_hash)"
                           " VALUES($1,$2,$3,$4,$5,'confirmed',$6,$7)", rid, sid, user, seats, amount, key, rhash)
        got = await conn.fetch(CLAIM_SQL, sid, seats, rid, user)  # THE atomic decision (all-or-nothing)
        if len(got) != len(seats):
            n = await conn.fetchval("SELECT count(*) FROM seats WHERE show_id=$1 AND label=ANY($2::text[])", sid, seats)
            if n != len(seats):
                raise Decline(422, "invalid_seat", "one or more seats do not exist in this show")
            raise Decline(409, "seat_taken", "one or more requested seats are not available")  # rolls back
        return "new", {"id": rid, "show_id": sid, "user_id": user, "seats": seats,
                       "amount_paise": amount, "status": "confirmed"}


@app.post("/shows/{show_id}/reserve", status_code=201)
async def reserve(show_id: str, body: ReserveIn, request: Request,
                  authorization: str | None = Header(None), idempotency_key: str | None = Header(None)):
    user = auth(authorization, request)
    sid = parse_uuid(show_id)
    key = idempotency_key or body.idempotency_key
    if not key:
        raise Decline(400, "missing_idempotency_key", "send Idempotency-Key header or idempotency_key in body")
    seats = sorted(body.seats)
    if len(set(seats)) != len(seats):
        raise Decline(422, "invalid_seat", "duplicate seats in request")
    rhash = hashlib.sha256(json.dumps([str(sid), seats]).encode()).hexdigest()
    pool = await get_pool()
    try:
        async with pool.acquire(timeout=60) as conn:
            # Lock-free fast path (an optimisation only; the authoritative decision is still _reserve_tx).
            # Reservation rows are immutable w.r.t. key/hash, and a non-available seat only becomes
            # available again via cancel, so declining on this read is a valid linearisation.
            prev = await conn.fetchrow("SELECT * FROM reservations WHERE user_id=$1 AND idempotency_key=$2", user, key)
            if prev:
                if prev["request_hash"] != rhash:
                    raise Decline(409, "idempotency_key_conflict", "key already used with a different request")
                kind, r = "replay", prev
            else:
                if await conn.fetchval("SELECT count(*) FROM seats WHERE show_id=$1 AND label=ANY($2::text[]) "
                                       "AND status<>'available'", sid, seats):
                    raise Decline(409, "seat_taken", "one or more requested seats are not available")
                kind, r = await with_retry(lambda: _reserve_tx(conn, sid, user, seats, key, rhash))
    except Decline as d:
        DECLINED.labels(d.reason).inc()
        raise
    if kind == "replay":
        DECLINED.labels("idempotent_replay").inc()
        return res_json(r)
    CONFIRMED.inc()
    log.info("reserved", extra={"f": {"show_id": str(sid), "reservation_id": str(r["id"]), "seats": seats}})
    return res_json(r)


async def _cancel_tx(conn, rid, user):
    async with conn.transaction():
        row = await conn.fetchrow("UPDATE reservations SET status='cancelled' WHERE id=$1 AND user_id=$2 "
                                  "AND status='confirmed' RETURNING *", rid, user)
        if row:
            await conn.fetch("""
              WITH c AS (SELECT label FROM seats WHERE show_id=$1 AND label=ANY($2::text[]) AND reservation_id=$3
                         ORDER BY label FOR UPDATE)     -- guard on reservation_id: can never free a seat now owned by someone else
              UPDATE seats s SET status='available', reservation_id=NULL, user_id=NULL, updated_at=now()
              FROM c WHERE s.show_id=$1 AND s.label=c.label RETURNING s.label""", row["show_id"], list(row["seats"]), rid)
            return True, row
        cur = await conn.fetchrow("SELECT * FROM reservations WHERE id=$1", rid)
        if not cur:
            raise Decline(404, "not_found", "no such reservation")
        if cur["user_id"] != user:
            raise Decline(403, "forbidden", "not your reservation")
        return False, cur  # already cancelled: idempotent


@app.post("/reservations/{reservation_id}/cancel")
async def cancel(reservation_id: str, request: Request, authorization: str | None = Header(None)):
    user = auth(authorization, request)
    rid = parse_uuid(reservation_id)
    pool = await get_pool()
    async with pool.acquire(timeout=60) as conn:
        changed, row = await with_retry(lambda: _cancel_tx(conn, rid, user))
    if changed:
        CANCELLED.inc()
        log.info("cancelled", extra={"f": {"reservation_id": str(rid)}})
    return res_json(row) | {"status": "cancelled"}


# ---------------------------------------------------------------- health & metrics
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    try:
        await get_pool()
        async with HPOOL.acquire(timeout=2) as c:
            await c.fetchval("SELECT 1", timeout=2)
        return {"status": "ready"}
    except Exception as e:  # fail closed
        return err(503, "not_ready", type(e).__name__)


@app.get("/metrics")
async def metrics():
    out = generate_latest().decode()
    try:
        pool = await get_pool()
        rows = await pool.fetch("SELECT show_id, status, count(*) n FROM seats WHERE show_id IN "
                                "(SELECT id FROM shows ORDER BY created_at DESC LIMIT 50) GROUP BY 1,2")
        per = {}
        for r in rows:
            per.setdefault(str(r["show_id"]), {})[r["status"]] = r["n"]
        for st in ("available", "held", "confirmed"):
            out += f"# HELP seats_{st} Seats currently {st} (read from DB at scrape time)\n# TYPE seats_{st} gauge\n"
            for sid, c in per.items():
                out += f'seats_{st}{{show_id="{sid}"}} {c.get(st, 0)}\n'
    except Exception as e:
        log.error("metrics db read failed", extra={"f": {"err": repr(e)}})
    return Response(out, media_type=CONTENT_TYPE_LATEST)
