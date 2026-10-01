# Write-up

## The atomic decision
Postgres decides. For each reserve, one transaction (READ COMMITTED) runs
```sql
WITH c AS (SELECT label FROM seats WHERE show_id=$1 AND label = ANY($2) AND status='available'
           ORDER BY label FOR UPDATE)
UPDATE seats SET status='confirmed', reservation_id=$3, user_id=$4 FROM c ... RETURNING label
```
`FOR UPDATE` takes the row lock; a competing transaction blocks, and when it resumes Postgres re-evaluates
`status='available'` against the committed row, so the loser matches zero rows. If `RETURNING` yields fewer rows than
requested, the transaction raises and rolls back (all-or-nothing) → 409. The `CHECK ((status='available') = (reservation_id IS NULL))`
constraint is a backstop. There is no read-then-write in application code.
**Deadlock avoidance:** every multi-row lock (reserve and cancel) is taken in `ORDER BY label` order, so two transactions
can never wait on each other in a cycle. Advisory locks are always taken in the same order (idempotency key → user quota → seat rows).
`deadlock_detected` is retried as a safety net only.
A lock-free pre-read short-circuits obvious "seat already taken" declines to save work; it is an optimisation, not the decision.

## Per-user limit
Under `pg_advisory_xact_lock(hash(show,user))` the user's confirmed seat count is read and the new reservation inserted, so a user's
concurrent requests on one show execute serially; 10 parallel reserves at limit 4 → exactly 4 succeed.

## Idempotency
Stored in `reservations(user_id, idempotency_key)` with a `UNIQUE` constraint plus a SHA-256 of `(show_id, sorted seats)`.
Concurrent retries of one key serialise on an advisory lock; the first inserts, later ones find the row: same hash → return the original
reservation (201), different hash → 409 `idempotency_key_conflict`. The reservation insert and the seat claim commit together, so a
retry can never create a second reservation or charge. Keys are scoped per user (a user cannot collide with someone else's key).

## Holds & expiry
No timed holds. Reserve confirms immediately (payment is out of scope); release is explicit
`POST /reservations/{id}/cancel`, owner-only. Cancel updates the reservation `confirmed→cancelled` (row-locked, so double cancel is a no-op),
then frees only seats where `reservation_id = this reservation`, so it can never free or resurrect a seat now owned by someone else.

## Consistency vs availability
Single Postgres primary = single source of truth (CP). During a partition/DB outage the API fails closed: `/readyz` and writes return 503;
we never accept a reservation we can't durably record. Reads come from the primary too (no stale replica reads that could show phantom availability).
With more time: a replica for read-only state/metrics, with the primary still arbitrating writes.

## Observability / what would page me at 2am
- `/readyz` failing or `http_requests_total{status=~"5.."}` > 0 for 2 min (any 5xx is a bug or dependency failure).
- `http_inflight_requests` high and p99 `http_request_duration_seconds` > 2s (pool saturation / lock pile-up on a hot show).
- Invariant drift: `seats_available+seats_held+seats_confirmed != total` (should be impossible) — page immediately.
- Confirmed counter flat while declines climb on an on-sale (stuck lock / DB problem).
- Sudden `idempotency_key_conflict` spike (client bug or abuse).
Logs are JSON with `request_id` (echoed as `x-request-id`) for tracing a single request.

## AI usage  *(edit this to be accurate for your own work!)*
\Directed: I chose Postgres conditional update + ordered row locks, all-or-nothing multi-seat, explicit cancel, fail-closed readyz.
Decided by the AI: drafted FastAPI/asyncpg code, schema, burst.py, and first doc draft from my spec.
Corrected after review/testing: fixed metrics reconciliation to count only winners, added lock-free fast path after hot-seat slowness, re-ran 22k burst locally.
Be specific and honest here; graders read this alongside the commit history.

## What I'd do next
- Timed holds (`held` state + expiry sweeper using `FOR UPDATE SKIP LOCKED`) with a payment step between hold and confirm.
- Real auth (signed JWTs), rate limiting / admission queue in front of hot shows, per-show virtual waiting room.
- Hot-seat sharding of queues (reject early at the edge using a cached "sold" bitmap), PgBouncer, read replica for `GET /shows`.
- Multi-instance metrics via Prometheus scrape of each pod; load test on production-size instances; automated CI running `burst.py` against docker compose.
