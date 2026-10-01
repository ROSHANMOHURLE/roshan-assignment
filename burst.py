#!/usr/bin/env python3
"""On-sale stampede against a live URL.   usage: python3 burst.py <BASE_URL>
env: ADMIN_TOKEN, N_REQUESTS=20000, SEATS=2000, HOT_SEATS=5, CONCURRENCY=500, HOT_STORM_USERS=500"""
import asyncio, collections, os, random, re, sys, time, uuid
import httpx

BASE = (sys.argv[1] if len(sys.argv) > 1 else os.getenv("BASE_URL", "http://localhost:8000")).rstrip("/")
ADMIN = os.getenv("ADMIN_TOKEN", "admin-secret")
N = int(os.getenv("N_REQUESTS", 20000)); SEATS = int(os.getenv("SEATS", 2000))
HOT = int(os.getenv("HOT_SEATS", 5)); CONC = int(os.getenv("CONCURRENCY", 500))
STORM = int(os.getenv("HOT_STORM_USERS", 500))
fails = []


def check(name, ok, info=""):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name} {info}")
    if not ok:
        fails.append(name)


def outcome(r):
    if r is None: return "client_error/timeout"
    if r.status_code == 201: return "confirmed"
    if r.status_code >= 500: return f"5xx({r.status_code})"
    try: return f"{r.status_code}:{r.json().get('error')}"
    except Exception: return str(r.status_code)


async def mk_show(c, n, limit=4, price=25000):
    r = await c.post("/shows", headers={"Authorization": f"Bearer {ADMIN}"},
                     json={"name": "burst", "seats": [f"S{i}" for i in range(1, n + 1)], "price_paise": price, "per_user_limit": limit})
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def reserve(c, sem, show, user, seats, key=None, extra=None):
    body = {"seats": seats, "idempotency_key": key or uuid.uuid4().hex}
    body.update(extra or {})
    async with sem:
        try:
            return await c.post(f"/shows/{show}/reserve", headers={"Authorization": f"Bearer user:{user}"}, json=body)
        except Exception:
            return None


def scrape(txt, name, label=None):
    pat = rf'^{name}{{?[^ ]*{label if label else ""}[^ ]*}}? ([0-9.e+]+)$' if label else rf"^{name} ([0-9.e+]+)$"
    m = re.search(pat, txt, re.M)
    return int(float(m.group(1))) if m else 0


async def metrics(c):
    t = (await c.get("/metrics")).text
    return {"confirmed": scrape(t, "reservations_confirmed_total"),
            **{k: scrape(t, "reservations_declined_total", f'reason="{k}"') for k in ("seat_taken", "per_user_limit", "idempotent_replay")}}


async def main():
    lim = httpx.Limits(max_connections=CONC, max_keepalive_connections=CONC)
    async with httpx.AsyncClient(base_url=BASE, limits=lim, timeout=180) as c:
        sem = asyncio.Semaphore(CONC)
        print(f"target {BASE}")
        r = await c.get("/readyz"); check("readyz", r.status_code == 200)
        m0 = await metrics(c)
        created = {}  # reservation_id -> (show, seats, user)

        # ---- 1. hot-seat storm
        print(f"\n== 1. hot-seat storm: {STORM} users, ONE seat (S1)")
        show = await mk_show(c, 100)
        res = await asyncio.gather(*[reserve(c, sem, show, f"storm{i}", ["S1"]) for i in range(STORM)])
        dist = collections.Counter(outcome(r) for r in res); print("  ", dict(dist))
        check("exactly one winner", dist["confirmed"] == 1)
        check("rest are clean 409 seat_taken", dist["409:seat_taken"] == STORM - 1)
        check("zero 5xx", not any(k.startswith("5") for k in dist))
        for r in res:
            if r is not None and r.status_code == 201: created[r.json()["reservation_id"]] = (show, r.json()["seats"], "storm")

        # ---- 2. full stampede
        print(f"\n== 2. stampede: {N} requests (+10% same-key retries), 70% on {HOT} hot seats, heavy users hit the limit")
        show2 = await mk_show(c, SEATS)
        hot = [f"S{i}" for i in range(1, HOT + 1)]
        specs = []
        for i in range(N):
            u = f"heavy{random.randint(0, 49)}" if random.random() < 0.05 else f"u{random.randint(0, N // 2)}"
            s = random.choice(hot) if random.random() < 0.7 else f"S{random.randint(1, SEATS)}"
            specs.append((u, [s], uuid.uuid4().hex))
        specs += random.sample(specs, N // 10)  # retries: same user, same key, same body
        random.shuffle(specs)
        t0 = time.time()
        res = await asyncio.gather(*[reserve(c, sem, show2, u, s, k) for u, s, k in specs])
        dt = time.time() - t0
        dist = collections.Counter(outcome(r) for r in res)
        print(f"   {len(specs)} requests in {dt:.1f}s ({len(specs)/dt:.0f} req/s)")
        for k, v in sorted(dist.items()): print(f"   {k:32s} {v}")
        winners = collections.defaultdict(set)
        for (u, s, k), r in zip(specs, res):
            if r is not None and r.status_code == 201:
                j = r.json(); created[j["reservation_id"]] = (show2, j["seats"], j["user_id"])
                check_u = j["user_id"] == u
                if not check_u: check("identity matches token", False)
                for seat in j["seats"]: winners[seat].add(j["reservation_id"])
        check("zero 5xx / client errors", not any(k.startswith(("5", "client")) for k in dist))
        check("no seat confirmed to two reservations", all(len(v) == 1 for v in winners.values()))
        check("each hot seat has exactly one winner", all(len(winners[s]) == 1 for s in hot))
        per_user = collections.Counter(v[2] for v in created.values() if v[0] == show2)
        check("per-user limit (<=4) held", max(per_user.values()) <= 4, f"max={max(per_user.values())}")
        st = (await c.get(f"/shows/{show2}")).json(); k = st["counts"]
        check("reconciliation available+held+confirmed==total", sum(k.values()) == st["total_seats"], f"{k} total={st['total_seats']}")
        check("confirmed seats == distinct winning reservations' seats", k["confirmed"] == len(winners), f"{k['confirmed']} vs {len(winners)}")

        # ---- 3. per-user limit, 10 parallel
        print("\n== 3. one user, 10 parallel reserves, limit=4")
        show3 = await mk_show(c, 50)
        res = await asyncio.gather(*[reserve(c, sem, show3, "greedy", [f"S{i}"]) for i in range(1, 11)])
        dist = collections.Counter(outcome(r) for r in res); print("  ", dict(dist))
        check("exactly 4 confirmed", dist["confirmed"] == 4)
        check("rest 409 per_user_limit", dist["409:per_user_limit"] == 6)
        for r in res:
            if r is not None and r.status_code == 201: created[r.json()["reservation_id"]] = (show3, r.json()["seats"], "greedy")

        # ---- 4. idempotency, spoofing, cancel
        print("\n== 4. idempotency / spoofing / cancel")
        key = uuid.uuid4().hex
        a = await reserve(c, sem, show3, "alice", ["S20"], key)
        b = await reserve(c, sem, show3, "alice", ["S20"], key)
        d = await reserve(c, sem, show3, "alice", ["S21"], key)
        check("same key+body returns original", a.status_code == 201 and b.status_code == 201 and a.json() == b.json())
        check("same key, different seats -> 409", d.status_code == 409 and d.json()["error"] == "idempotency_key_conflict")
        sp = await reserve(c, sem, show3, "mallory", ["S30"], extra={"user_id": "victim", "user": "victim"})
        check("spoofed body user ignored", sp.status_code == 201 and sp.json()["user_id"] == "mallory")
        rid = sp.json()["reservation_id"]
        x = await c.post(f"/reservations/{rid}/cancel", headers={"Authorization": "Bearer user:victim"})
        check("non-owner cancel -> 403", x.status_code == 403)
        x = await c.post(f"/reservations/{rid}/cancel", headers={"Authorization": "Bearer user:mallory"})
        check("owner cancel -> 200", x.status_code == 200 and x.json()["status"] == "cancelled")
        y = await reserve(c, sem, show3, "bob", ["S30"])
        check("released seat re-bookable", y.status_code == 201)
        x = await c.post(f"/reservations/{rid}/cancel", headers={"Authorization": "Bearer user:mallory"})
        st = (await c.get(f"/shows/{show3}")).json()
        check("repeat cancel does not resurrect/free bob's seat", st["seats"]["S30"] == "confirmed")
        for r in (a, y): created[r.json()["reservation_id"]] = (show3, r.json()["seats"], "x")

        # ---- 5. reconciliation + metrics
        print("\n== 5. final reconciliation & metrics")
        st = (await c.get(f"/shows/{show3}")).json()
        check("show3 invariant", sum(st["counts"].values()) == st["total_seats"], str(st["counts"]))
        m1 = await metrics(c)
        new = len(created) + 1  # +1: mallory's (cancelled) reservation also counted when created
        print("   metrics delta:", {k: m1[k] - m0[k] for k in m1})
        check("metrics confirmed delta == reservations created", m1["confirmed"] - m0["confirmed"] == new,
              f"{m1['confirmed']-m0['confirmed']} vs {new}")
        r = await c.get("/metrics"); check("seats_available gauge exposed", "seats_available{" in r.text)
    print("\nRESULT:", "ALL CHECKS PASSED" if not fails else f"FAILED: {fails}")
    sys.exit(1 if fails else 0)

asyncio.run(main())
