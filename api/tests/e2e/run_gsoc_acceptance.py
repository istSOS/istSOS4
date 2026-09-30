#!/usr/bin/env python3
# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License").
#
# Full acceptance sweep for the GSoC authentication + authorization work.
#
# Every check below is a behaviour that does NOT exist in upstream/main, or
# one that upstream has and this branch had to keep working. It runs against
# a live stack -- nothing is mocked.
#
#   docker compose up -d --build      # .env: AUTHORIZATION=1 NETWORK=1
#                                     #       ANONYMOUS_VIEWER=0
#   python api/tests/e2e/run_gsoc_acceptance.py
#
# Exits non-zero if any check fails.

import os
import subprocess
import sys
import time
import uuid

import requests

BASE = os.getenv("ISTSOS_BASE", "http://localhost:8018/istsos4/v1.1")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
DB = os.getenv("DB_CONTAINER", "istsos4-database")

TAG = uuid.uuid4().hex[:6]
PW = "CorrectHorseBattery1!"

_results = []
_section = ""


def section(name):
    global _section
    _section = name
    print(f"\n\033[1m{name}\033[0m")


def check(name, cond, detail=""):
    _results.append((_section, name, bool(cond)))
    mark = "\033[32mPASS\033[0m" if cond else "\033[31mFAIL\033[0m"
    print(f"  [{mark}] {name}" + (f"   ({detail})" if detail else ""))
    return bool(cond)


def psql(sql):
    out = subprocess.run(
        ["docker", "exec", DB, "psql", "-U", "postgres", "-d", "istsos", "-tAc", sql],
        capture_output=True, text=True,
    )
    return out.stdout.strip()


def login(u, p):
    return requests.post(f"{BASE}/Login", data={"username": u, "password": p}, timeout=10)


def tok_of(resp):
    try:
        return resp.json()["access_token"]
    except Exception:  # noqa: BLE001
        return None


def H(t):
    return {"Authorization": f"Bearer {t}"}


def count(entity, t, extra=None):
    p = {"$count": "true", "$top": 1}
    if extra:
        p.update(extra)
    r = requests.get(f"{BASE}/{entity}", params=p, headers=H(t), timeout=30)
    if r.status_code != 200:
        return None
    return r.json().get("@iot.count")


def main():
    t0 = time.time()
    print(f"istSOS: {BASE}    run tag: {TAG}")

    # ==================================================================
    section("1. Anonymous access is denied  (ANONYMOUS_VIEWER=0)")
    # upstream: same contract -- this branch broke it, then restored it.
    # ==================================================================
    r = requests.get(f"{BASE}/Datastreams", timeout=10)
    check("GET /Datastreams without a token -> 401", r.status_code == 401,
          f"HTTP {r.status_code}")
    r = requests.get(f"{BASE}/Observations", timeout=10)
    check("GET /Observations without a token -> 401", r.status_code == 401,
          f"HTTP {r.status_code}")
    r = requests.get(f"{BASE}/Users", timeout=10)
    check("GET /Users without a token -> 401", r.status_code == 401,
          f"HTTP {r.status_code}")

    # ==================================================================
    section("2. Local authentication  (bcrypt in User.password -- NEW)")
    # ==================================================================
    r = login(ADMIN_USER, ADMIN_PASS)
    admin = tok_of(r)
    check("admin login -> 200 + JWT", r.status_code == 200 and admin,
          f"HTTP {r.status_code}")
    check("wrong password -> 401", login(ADMIN_USER, "nope").status_code == 401)
    check("unknown user -> 401", login(f"ghost{TAG}", PW).status_code == 401)

    admin_ct = psql("SELECT count(*) FROM sensorthings.\"User\" "
                    "WHERE password IS NOT NULL AND password LIKE '$2%'")
    check("passwords stored as bcrypt hashes, not PG roles", admin_ct != "0",
          f"{admin_ct} bcrypt row(s)")
    # The app-layer pivot: the only PostgreSQL roles that may exist are the
    # service account + the fixed group roles. No role is ever created per
    # istSOS user.
    extra_roles = psql(
        "SELECT coalesce(string_agg(rolname, ','), '-') FROM pg_roles "
        "WHERE rolname NOT LIKE 'pg\\_%' AND rolname NOT IN "
        "('postgres','admin','administrator','guest','user','sensor','qc')")
    check("istSOS users are NOT PostgreSQL roles (no per-user CREATE ROLE)",
          extra_roles == "-", f"unexpected pg_roles: {extra_roles}")

    # ==================================================================
    section("3. GET /Users payload  (7 new columns, no secret leak)")
    # ==================================================================
    r = requests.get(f"{BASE}/Users", headers=H(admin), timeout=10)
    users = r.json().get("value", [])
    row = users[0] if users else {}
    check("GET /Users -> 200", r.status_code == 200)
    for col in ("auth_provider", "external_sub_id", "status", "dataset_id",
                "requested_role", "possible_duplicate_of"):
        check(f"exposes new column '{col}'", col in row)
    check("does NOT expose the bcrypt hash", "password" not in row,
          "upstream had no password column at all")

    # ==================================================================
    section("4. POST /Users -- admin-created account can log in")
    # regression: the handler used to pop() the password and drop it.
    # ==================================================================
    du = f"direct_{TAG}"
    r = requests.post(f"{BASE}/Users", headers=H(admin),
                      json={"username": du, "password": PW, "role": "viewer"},
                      timeout=15)
    check("POST /Users -> 2xx", 200 <= r.status_code < 300, f"HTTP {r.status_code}")
    check("the created user can actually authenticate",
          login(du, PW).status_code == 200)
    r = requests.post(f"{BASE}/Users", headers=H(admin),
                      json={"username": f"bad_{TAG}", "password": PW,
                            "role": "not_a_role"}, timeout=15)
    check("invalid role -> 400 (was an unhandled 500)", r.status_code == 400,
          f"HTTP {r.status_code}")

    # ==================================================================
    section("5. Self-registration -> pending waiting room  (NEW endpoint)")
    # ==================================================================
    net = psql('SELECT name FROM sensorthings."Network" ORDER BY id LIMIT 1')
    net2 = psql('SELECT name FROM sensorthings."Network" ORDER BY id DESC LIMIT 1')
    ru = f"reg_{TAG}"
    body = {
        "username": ru, "password": PW,
        "contact_info": {"email": f"{ru}@example.com"},
        "explanation": "gsoc acceptance run",
        "dataset_id": net, "requested_role": "viewer",
    }
    r = requests.post(f"{BASE}/Register", json=body, timeout=15)
    check("POST /Register (public, no token) -> 201", r.status_code == 201,
          f"HTTP {r.status_code}")
    reg_id = r.json().get("id") if r.status_code == 201 else None

    st = psql(f"SELECT role || '|' || status || '|' || coalesce(dataset_id,'-') "
              f"|| '|' || coalesce(requested_role,'-') "
              f"FROM sensorthings.\"User\" WHERE username = '{ru}'")
    check("row is role=pending status=pending, request recorded",
          st == f"pending|pending|{net}|viewer", st)

    r = login(ru, PW)
    check("pending user CANNOT log in -> 403 (no dead JWT issued)",
          r.status_code == 403, f"HTTP {r.status_code}")

    r = requests.get(f"{BASE}/Users", headers=H(admin), timeout=10)
    me = [u for u in r.json().get("value", []) if u["username"] == ru]
    check("admin queue shows status='pending', not a misleading 'active'",
          me and me[0]["status"] == "pending",
          me[0]["status"] if me else "row missing")

    check("duplicate username -> 409",
          requests.post(f"{BASE}/Register", json=body, timeout=15).status_code == 409)

    # ==================================================================
    section("6. Admin approval + network scoping  (NEW)")
    # ==================================================================
    r = requests.patch(f"{BASE}/Users/{reg_id}/policy-approval", headers=H(admin),
                       json={"role": "viewer", "dataset": net}, timeout=15)
    check("PATCH /Users/{id}/policy-approval -> 200", r.status_code == 200,
          f"HTTP {r.status_code}: {r.text[:90]}")
    st = psql(f"SELECT role || '|' || status FROM sensorthings.\"User\" "
              f"WHERE username = '{ru}'")
    check("approved row is role=viewer status=active", st == "viewer|active", st)

    r = login(ru, PW)
    viewer = tok_of(r)
    check("approved user can now log in", r.status_code == 200 and viewer)

    r = requests.patch(f"{BASE}/Users/{reg_id}/policy-approval", headers=H(admin),
                       json={"role": "viewer", "dataset": "NoSuchNetwork"},
                       timeout=15)
    check("approval with an unknown Network -> 4xx (validated, not silent)",
          r.status_code >= 400, f"HTTP {r.status_code}")

    # ground truth straight from the database
    ds_net = int(psql('SELECT count(*) FROM sensorthings."Datastream" '
                      f"WHERE network_id = (SELECT id FROM sensorthings.\"Network\" WHERE name='{net}')"))
    ds_all = int(psql('SELECT count(*) FROM sensorthings."Datastream"'))
    obs_net = int(psql('SELECT count(*) FROM sensorthings."Observation" o '
                       'JOIN sensorthings."Datastream" d ON d.id = o.datastream_id '
                       f"WHERE d.network_id = (SELECT id FROM sensorthings.\"Network\" WHERE name='{net}')"))
    obs_all = int(psql('SELECT count(*) FROM sensorthings."Observation"'))
    print(f"    ground truth: network '{net}' = {ds_net}/{ds_all} datastreams, "
          f"{obs_net}/{obs_all} observations")

    check("admin (unscoped) sees ALL datastreams",
          count("Datastreams", admin) == ds_all, f"{count('Datastreams', admin)}")
    check("scoped viewer sees ONLY its network's datastreams (RLS)",
          count("Datastreams", viewer) == ds_net,
          f"saw {count('Datastreams', viewer)}, expected {ds_net}")
    check("scoped viewer sees ONLY its network's observations (RLS)",
          count("Observations", viewer) == obs_net,
          f"saw {count('Observations', viewer)}, expected {obs_net}")

    seen = requests.get(f"{BASE}/Datastreams",
                        params={"$top": 200, "$expand": "Network($select=name)"},
                        headers=H(viewer), timeout=30).json().get("value", [])
    names = {d.get("Network", {}).get("name") for d in seen}
    check("every row the viewer sees belongs to its network", names == {net},
          str(names))

    r = requests.patch(f"{BASE}/Datastreams({seen[0]['@iot.id']})" if seen
                       else f"{BASE}/Datastreams(1)",
                       headers={**H(viewer), "commit-message": "acceptance"},
                       json={"name": f"nope_{TAG}"}, timeout=15)
    check("viewer cannot write (read-only role enforced)", r.status_code >= 400,
          f"HTTP {r.status_code}")

    # ==================================================================
    section("7. Re-scoping an ACTIVE user  (NEW -- PATCH /Users/{id}/role)")
    # ==================================================================
    r = requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(admin),
                       json={"role": "editor"}, timeout=15)
    check("change role only -> 204", r.status_code == 204, f"HTTP {r.status_code}")
    check("role is now editor",
          psql(f"SELECT role FROM sensorthings.\"User\" WHERE id={reg_id}") == "editor")

    r = requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(admin),
                       json={"dataset": net2}, timeout=15)
    check("change network scope only -> 204", r.status_code == 204,
          f"HTTP {r.status_code}")
    check("dataset_id moved to the other network",
          psql(f"SELECT dataset_id FROM sensorthings.\"User\" WHERE id={reg_id}") == net2)

    ed = tok_of(login(ru, PW))
    ds_net2 = int(psql('SELECT count(*) FROM sensorthings."Datastream" '
                       f"WHERE network_id = (SELECT id FROM sensorthings.\"Network\" WHERE name='{net2}')"))
    check("re-scoped user's visibility followed the change, live",
          count("Datastreams", ed) == ds_net2,
          f"saw {count('Datastreams', ed)}, expected {ds_net2}")

    r = requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(admin),
                       json={"dataset": "NoSuchNetwork"}, timeout=15)
    check("unknown Network on re-scope -> 400", r.status_code == 400,
          f"HTTP {r.status_code}")
    r = requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(admin),
                       json={}, timeout=15)
    check("empty body -> 422 (validator requires role or dataset)",
          r.status_code == 422, f"HTTP {r.status_code}")

    # clear the scope, confirm unrestricted
    requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(admin),
                   json={"dataset": ""}, timeout=15)
    ed = tok_of(login(ru, PW))
    check('dataset="" clears the scope -> sees everything',
          count("Datastreams", ed) == ds_all,
          f"saw {count('Datastreams', ed)}, expected {ds_all}")

    # ==================================================================
    section("8. Rejection + re-application  (NEW)")
    # ==================================================================
    ru2 = f"rej_{TAG}"
    body2 = dict(body, username=ru2)
    body2["contact_info"] = {"email": f"{ru2}@example.com"}
    rid2 = requests.post(f"{BASE}/Register", json=body2, timeout=15).json().get("id")
    r = requests.patch(f"{BASE}/Users/{rid2}/reject", headers=H(admin),
                       json={"reason": "acceptance run"}, timeout=15)
    check("PATCH /Users/{id}/reject -> 200", r.status_code == 200,
          f"HTTP {r.status_code}")
    st = psql(f"SELECT role || '|' || status FROM sensorthings.\"User\" WHERE id={rid2}")
    check("rejected row is role=pending status=rejected", st == "pending|rejected", st)
    lr = login(ru2, PW)
    check("rejected user still cannot log in (401 + explanatory message)",
          lr.status_code in (401, 403) and "reject" in lr.text.lower(),
          f"HTTP {lr.status_code}: {lr.text[:70]}")

    r = requests.post(f"{BASE}/Register", json=body2, timeout=15)
    check("rejected user may RE-APPLY -> 201 (not 409)", r.status_code == 201,
          f"HTTP {r.status_code}")
    st = psql(f"SELECT role || '|' || status FROM sensorthings.\"User\" WHERE id={rid2}")
    check("re-application resets to role=pending status=pending",
          st == "pending|pending", st)

    # ==================================================================
    section("9. Policies  (reworked for the app-layer model)")
    # ==================================================================
    r = requests.get(f"{BASE}/Policies", headers=H(admin), timeout=15)
    check("GET /Policies -> 200", r.status_code == 200, f"HTTP {r.status_code}")
    pol_count = len(r.json().get("value", [])) if r.status_code == 200 else 0
    check("static RLS policies exist in the database", pol_count > 0,
          f"{pol_count} policies")

    # Upstream's payload contract ({users, name, permissions}) is preserved.
    r = requests.post(f"{BASE}/Policies", headers=H(admin),
                      json={"users": [ru], "name": f"noop_{TAG}",
                            "permissions": {"type": "viewer"}}, timeout=15)
    check("POST /Policies type=viewer -> 400 (static policy covers it, nothing to create)",
          r.status_code == 400, f"HTTP {r.status_code}: {r.text[:80]}")
    r = requests.post(f"{BASE}/Policies", headers=H(admin),
                      json={"users": [ru]}, timeout=15)
    check("POST /Policies missing 'name'/'permissions' -> 400 (upstream contract)",
          r.status_code == 400, f"HTTP {r.status_code}")

    r = requests.patch(f"{BASE}/Policies", headers=H(admin), json={}, timeout=10)
    check("PATCH /Policies -> 405 (broken endpoint removed)",
          r.status_code == 405, f"HTTP {r.status_code}")

    cu = f"cust_{TAG}"
    requests.post(f"{BASE}/Users", headers=H(admin),
                  json={"username": cu, "password": PW, "role": "custom"}, timeout=15)
    ctok = tok_of(login(cu, PW))
    base_custom = count("Datastreams", ctok)
    check("a 'custom' user gets NO blanket grant (starts at 0 rows)",
          base_custom == 0, f"saw {base_custom}")

    pol_name = f"cust_{TAG}"
    r = requests.post(f"{BASE}/Policies", headers=H(admin),
                      json={"users": [cu], "name": pol_name,
                            "permissions": {
                                "type": "custom",
                                "policy": {"datastream": {"select": "id % 2 = 0"}},
                            }}, timeout=15)
    made = 200 <= r.status_code < 300
    check("POST /Policies type=custom -> creates a scoped policy",
          made, f"HTTP {r.status_code}: {r.text[:100]}")
    if made:
        ctok = tok_of(login(cu, PW))
        even = int(psql('SELECT count(*) FROM sensorthings."Datastream" WHERE id % 2 = 0'))
        got = count("Datastreams", ctok)
        check("the custom predicate is actually enforced by RLS",
              got == even, f"saw {got}, expected {even}")
        pol = psql("SELECT count(*) FROM pg_policies WHERE schemaname='sensorthings' "
                   "AND qual LIKE '%current_app_user_id%'")
        check("policy is group-scoped + identity clause (not a dead TO <username>)",
              pol != "0", f"{pol} identity-scoped policies")
        roles = psql("SELECT coalesce(string_agg(DISTINCT r, ','), '-') FROM "
                     "(SELECT unnest(roles) AS r FROM pg_policies "
                     "WHERE schemaname='sensorthings') s "
                     "WHERE r NOT IN ('user','sensor','qc','administrator','guest','public')")
        check("no policy targets a per-user PG role (the pivot holds)",
              roles == "-", f"unexpected policy roles: {roles}")
        # create_policies() names each rule "<name>_<table>_<operation>"
        r = requests.delete(f"{BASE}/Policies", headers=H(admin),
                            params={"policy": f"{pol_name}_datastream_select"},
                            timeout=15)
        check("DELETE /Policies -> 2xx", 200 <= r.status_code < 300,
              f"HTTP {r.status_code}: {r.text[:80]}")
        ctok = tok_of(login(cu, PW))
        check("access revoked after the policy is deleted",
              count("Datastreams", ctok) == 0, f"saw {count('Datastreams', ctok)}")

    # ==================================================================
    section("10. Audit trail  (NEW -- append-only)")
    # ==================================================================
    n = psql("SELECT count(*) FROM sensorthings.\"AuditLog\" "
             "WHERE action_type = 'RESTRICTED_REQUEST'")
    check("registrations wrote RESTRICTED_REQUEST rows", n != "0", f"{n} rows")
    n = psql("SELECT count(*) FROM sensorthings.\"AuditLog\" "
             "WHERE action_type = 'ADMIN_APPROVAL'")
    check("approvals wrote ADMIN_APPROVAL rows", n != "0", f"{n} rows")
    upd = subprocess.run(
        ["docker", "exec", DB, "psql", "-U", "postgres", "-d", "istsos", "-c",
         'SET ROLE "administrator"; UPDATE sensorthings."AuditLog" SET action_type=\'X\';'],
        capture_output=True, text=True)
    check("AuditLog is append-only (owner cannot UPDATE)",
          "permission denied" in (upd.stderr + upd.stdout).lower(),
          (upd.stderr or upd.stdout).strip().splitlines()[-1][:70] if (upd.stderr or upd.stdout) else "")

    # ==================================================================
    section("11. Session/JWT lifecycle")
    # ==================================================================
    r = requests.post(f"{BASE}/Refresh", headers=H(admin), timeout=10)
    check("POST /Refresh -> 2xx", 200 <= r.status_code < 300, f"HTTP {r.status_code}")
    # With REDIS=1, /Refresh revokes the token it was called with, so carry on
    # with the fresh one it returns (harmless with REDIS=0).
    if r.ok and r.json().get("access_token"):
        admin = r.json()["access_token"]
    throw = tok_of(login(du, PW))
    r = requests.post(f"{BASE}/Logout", headers=H(throw), timeout=10)
    check("POST /Logout -> 2xx", 200 <= r.status_code < 300, f"HTTP {r.status_code}")
    # tokens carry no unique id: a login in the same second as the logout would
    # get the identical, now-revoked string when REDIS=1
    time.sleep(1.1)
    r = requests.get(f"{BASE}/Datastreams", params={"$top": 1}, headers=H(throw),
                     timeout=10)
    # The deny-list is a property of the running API, not of this script's
    # shell, so ask the container (fall back to the caller's env).
    api_env = subprocess.run(
        ["docker", "exec", os.getenv("API_CONTAINER", "istsos4-api"), "printenv", "REDIS"],
        capture_output=True, text=True).stdout.strip()
    redis_on = (api_env or os.getenv("REDIS", "0")) not in ("0", "", "false", "False")
    if redis_on:
        check("token is revoked after logout -> 401", r.status_code == 401,
              f"HTTP {r.status_code}")
    else:
        check("logout revocation needs REDIS=1 (config, not a defect)",
              r.status_code == 200,
              "REDIS=0 -> the deny-list is disabled, token stays valid "
              "until it expires; same as upstream")

    # Deactivation is the revocation path that works without Redis: the role
    # and status are re-read from the database on every single request.
    dead = tok_of(login(du, PW))
    du_id = psql(f"SELECT id FROM sensorthings.\"User\" WHERE username='{du}'")
    requests.delete(f"{BASE}/Users/{du_id}", headers=H(admin), timeout=15)
    r = requests.get(f"{BASE}/Datastreams", params={"$top": 1}, headers=H(dead),
                     timeout=10)
    check("an already-issued JWT dies the moment the account is deactivated",
          r.status_code in (401, 403), f"HTTP {r.status_code}")

    r = requests.get(f"{BASE}/Datastreams", params={"$top": 1},
                     headers={"Authorization": "Bearer garbage.token.here"}, timeout=10)
    check("forged/garbage token -> 401", r.status_code == 401, f"HTTP {r.status_code}")

    # ==================================================================
    section("12. Privilege boundaries")
    # ==================================================================
    vt = tok_of(login(ru, PW))
    check("non-admin GET /Users -> 401/403",
          requests.get(f"{BASE}/Users", headers=H(vt), timeout=10).status_code in (401, 403))
    check("non-admin POST /Users -> 401/403",
          requests.post(f"{BASE}/Users", headers=H(vt),
                        json={"username": f"x{TAG}", "password": PW,
                              "role": "viewer"}, timeout=10).status_code in (401, 403))
    check("non-admin can't approve -> 401/403",
          requests.patch(f"{BASE}/Users/{rid2}/policy-approval", headers=H(vt),
                         json={"role": "viewer"}, timeout=10).status_code in (401, 403))
    # a *valid* role, so authorization is what rejects this, not validation
    check("non-admin can't change roles -> 401/403",
          requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(vt),
                         json={"role": "qc"}, timeout=10).status_code in (401, 403))
    r = requests.patch(f"{BASE}/Users/{reg_id}/role", headers=H(admin),
                       json={"role": "administrator"}, timeout=15)
    check("'administrator' is not API-assignable, even by an admin -> 4xx",
          r.status_code >= 400, f"HTTP {r.status_code}")
    check("...and the target was not silently promoted",
          psql(f"SELECT role FROM sensorthings.\"User\" WHERE id={reg_id}") != "administrator")
    admins = psql("SELECT count(*) FROM sensorthings.\"User\" "
                  "WHERE role = 'administrator'")
    r = requests.patch(f"{BASE}/Users/1/role", headers=H(admin),
                       json={"role": "viewer"}, timeout=15)
    check("last administrator cannot be demoted (lockout guard)",
          r.status_code >= 400 if admins == "1" else True,
          f"HTTP {r.status_code}, {admins} admin(s)")

    # ==================================================================
    section("13. Upstream SensorThings contract still intact")
    # ==================================================================
    for ent in ("Things", "Sensors", "ObservedProperties", "Locations",
                "FeaturesOfInterest", "HistoricalLocations", "Datastreams",
                "Observations"):
        c = count(ent, admin)
        check(f"GET /{ent} works", c is not None, f"@iot.count={c}")
    r = requests.get(f"{BASE}/Datastreams", params={"$top": 2, "$expand": "Thing"},
                     headers=H(admin), timeout=20)
    check("$expand works", r.status_code == 200 and r.json().get("value"))
    r = requests.get(f"{BASE}/Datastreams", params={"$filter": "id gt 0", "$top": 1},
                     headers=H(admin), timeout=20)
    check("$filter works", r.status_code == 200)
    r = requests.get(f"{BASE}/Datastreams(999999)", headers=H(admin), timeout=10)
    check("missing entity -> 404", r.status_code == 404, f"HTTP {r.status_code}")
    r = requests.get(f"{BASE}/Datastreams", params={"$filter": "!!bad!!"},
                     headers=H(admin), timeout=10)
    check("malformed $filter -> 400", r.status_code == 400, f"HTTP {r.status_code}")

    # ==================================================================
    section("14. OIDC surface wired  (full round trip: run_oidc_e2e.py)")
    # ==================================================================
    r = requests.get(f"{BASE}/auth/google/login", allow_redirects=False, timeout=10)
    check("GET /auth/google/login -> 302 to the provider",
          r.status_code == 302, f"HTTP {r.status_code}")
    r = requests.get(f"{BASE}/auth/notaprovider/login", allow_redirects=False,
                     timeout=10)
    check("unknown provider -> 4xx", r.status_code >= 400, f"HTTP {r.status_code}")
    cols = psql("SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema='sensorthings' AND table_name='User' "
                "AND column_name IN ('auth_provider','external_sub_id',"
                "'possible_duplicate_of','requested_role','dataset_id','status','password')")
    check("all 7 identity/lifecycle columns present in the schema",
          cols == "7", f"{cols}/7")

    # ------------------------------------------------------------------
    _report(time.time() - t0)


def _report(elapsed):
    print("\n" + "=" * 66)
    by_sec = {}
    for sec, _name, ok in _results:
        p, f = by_sec.get(sec, (0, 0))
        by_sec[sec] = (p + (1 if ok else 0), f + (0 if ok else 1))
    for sec, (p, f) in by_sec.items():
        flag = "\033[32mOK\033[0m" if f == 0 else f"\033[31m{f} FAILED\033[0m"
        print(f"  {sec:<62} {p}/{p + f} {flag}")
    total = len(_results)
    failed = [f"{s} :: {n}" for s, n, ok in _results if not ok]
    print("=" * 66)
    print(f"  {total - len(failed)}/{total} checks passed in {elapsed:.1f}s")
    if failed:
        print("\n  FAILURES:")
        for f in failed:
            print(f"    - {f}")
        sys.exit(1)
    print("  \033[32mALL GREEN\033[0m")
    sys.exit(0)


if __name__ == "__main__":
    main()
