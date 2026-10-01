# Seat Reservation at Scale

JSON API that sells assigned seats without double-selling, over-limit booking, or double-charging retries.
Stack: Python 3.12 · FastAPI · asyncpg · PostgreSQL 16 · Prometheus metrics.

**Live URL:** `https://<your-service>.onrender.com`  (fill in after deploy)
**Metrics:** `GET /metrics` · **Health:** `GET /healthz` (liveness), `GET /readyz` (readiness, checks DB, 503 when down)
**Logs:** Render dashboard → service → Logs (public log link / screen recording: `<add link>`). JSON lines with `request_id`.

## Run locally
```bash
docker compose up --build        # app on :8000, Postgres 16
python3 burst.py http://localhost:8000   # or: make burst
```

## Deploy (Render, free tier)
New → Blueprint → select this repo (`render.yaml` creates web service + Postgres and a generated `ADMIN_TOKEN`).
Read the generated token from the service's Environment tab, then:
```bash
ADMIN_TOKEN=<token> python3 burst.py https://<your-service>.onrender.com
```
(Free instances sleep; the app retries the DB on boot and `/healthz` is the platform health check.)

## Auth (dev scheme)
- Admin: `Authorization: Bearer <ADMIN_TOKEN>`
- User: `Authorization: Bearer user:<user_id>`  (identity comes only from the token; body `user_id` fields are ignored)

## API
| | |
|---|---|
| `POST /shows` (admin) `{"name","seats":[...],"price_paise":25000,"per_user_limit":4}` | 201 show, all seats `available` |
| `POST /shows/{id}/reserve` `{"seats":["A12"],"idempotency_key":"…"}` (or `Idempotency-Key` header) | 201 `{reservation_id, show_id, user_id, seats, amount_paise, status:"confirmed"}` |
| `POST /reservations/{id}/cancel` (owner only) | 200, seats return to `available` |
| `GET /shows/{id}` | `{counts:{available,held,confirmed}, total_seats, seats:{label:status}}` |

Declines (all 4xx JSON `{"error": <reason>}`): `409 seat_taken`, `409 per_user_limit`, `409 idempotency_key_conflict`,
`422 invalid_seat`, `400 missing_idempotency_key`, `401`, `403`, `404`.
Idempotent replay returns the original reservation with 201.

**Multi-seat policy: all-or-nothing.** `["A12","A13"]` with one taken → nothing is reserved, 409 `seat_taken`.
**Release model:** explicit cancel (owner only). The `held` state exists in the schema/invariant but reservations are confirmed immediately, so `held` is always 0.

## Metrics (Prometheus)
`reservations_confirmed_total`, `reservations_cancelled_total`, `reservations_declined_total{reason=seat_taken|per_user_limit|idempotent_replay|idempotency_key_conflict|invalid_seat}`,
`seats_available|held|confirmed{show_id}` (read from DB at scrape → always reconciles with `GET /shows/{id}`),
`http_requests_total{route,method,status}`, `http_request_duration_seconds`, `http_inflight_requests`.
Counters are per-process (reset on restart).

## Burst script
`python3 burst.py <BASE_URL>` (env: `ADMIN_TOKEN`, `N_REQUESTS=20000`, `SEATS=2000`, `HOT_SEATS=5`, `CONCURRENCY=500`).
Runs: (1) 500 users → one seat; (2) 22k-request stampede (70% on 5 hot seats, 10% same-key retries, heavy users);
(3) one user × 10 parallel at limit 4; (4) idempotency / spoofing / cancel checks; (5) reconciliation + metrics deltas.
Prints the outcome distribution and PASS/FAIL per invariant; exit code 1 on any failure.
Local result (1 vCPU sandbox, client+DB+app on the same core): 22,000 requests, 0 5xx, all checks passed.
