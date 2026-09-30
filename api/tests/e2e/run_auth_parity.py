#!/usr/bin/env python3
# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License").
#
# Privilege parity: does an EXTERNALLY authenticated (OIDC) user get exactly
# the same capabilities as a LOCAL (password) user holding the same role and
# the same Network scope?
#
# The two provisioning paths write different columns -- a local account has a
# bcrypt `password` and NULL auth_provider/external_sub_id; an OIDC account is
# the mirror image. Everything downstream (RLS, set_role, the commit trail)
# is supposed to key off `role` and `dataset_id` only, so the two should be
# indistinguishable once activated. This asserts that, instead of assuming it.
#
# For every role it creates BOTH kinds of user, scoped to the same Network,
# runs an identical battery of requests as each, and reports any operation
# where the two disagree.
#
#   docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --build
#   python api/tests/e2e/run_auth_parity.py
#
# Exits non-zero on any parity break.

import os
import re
import subprocess
import sys
import uuid

import requests

BASE = os.getenv("ISTSOS_BASE", "http://localhost:8018/istsos4/v1.1")
FAKE = os.getenv("FAKE_BASE", "http://localhost:9000")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
DB = os.getenv("DB_CONTAINER", "istsos4-database")

TAG = uuid.uuid4().hex[:6]
PW = "CorrectHorseBattery1!"
ROLES = ["viewer", "editor", "obs_manager", "sensor", "qc", "custom"]

BROWSER = {"Accept": "text/html,application/xhtml+xml,*/*;q=0.8"}

# Differences that are correct by design, not defects. Anything NOT listed
# here is a genuine parity break and fails the run.
#
# "PATCH own password": a local account authenticates against the bcrypt hash
# in User.password; an OIDC account has password IS NULL and authenticates
# only through its provider, so there is no current_password to verify and
# nothing a new one would be used for. The endpoint says so explicitly --
# 400 "External identities cannot update passwords locally." -- rather than
# failing obscurely. Letting an external identity set a local password would
# ADD a second, weaker way into that account, so refusing is the safer
# behaviour, not a missing feature.
EXPECTED_DIFFS = {"PATCH own password"}
_breaks = []
_rows = []


def psql(sql):
    return subprocess.run(
        ["docker", "exec", DB, "psql", "-U", "postgres", "-d", "istsos", "-tAc", sql],
        capture_output=True, text=True).stdout.strip()


def admin_token():
    return requests.post(f"{BASE}/Login",
                         data={"username": ADMIN_USER, "password": ADMIN_PASS},
                         timeout=15).json()["access_token"]


def make_local(role, net, admin):
    """Register -> admin approves with role + network scope -> log in."""
    u = f"loc_{role}_{TAG}"
    requests.post(f"{BASE}/Register", timeout=20, json={
        "username": u, "password": PW,
        "contact_info": {"email": f"{u}@example.org"},
        "explanation": "parity harness", "dataset_id": net,
        "requested_role": role})
    uid = psql(f"SELECT id FROM sensorthings.\"User\" WHERE username='{u}'")
    requests.patch(f"{BASE}/Users/{uid}/policy-approval",
                   headers={"Authorization": f"Bearer {admin}"},
                   json={"role": role, "dataset": net}, timeout=20)
    r = requests.post(f"{BASE}/Login", data={"username": u, "password": PW},
                      timeout=15)
    return u, uid, r.json().get("access_token")


def oidc_roundtrip(sub, email):
    """The real browser flow against the fake IdP."""
    s = requests.Session()
    s.headers.update(BROWSER)
    r = s.get(f"{BASE}/auth/google/login", allow_redirects=True, timeout=20)
    hidden = dict(re.findall(r'name="(\w+)" value="([^"]*)"', r.text))
    return s.post(f"{FAKE}/authorize", timeout=20, allow_redirects=True,
                  data={**hidden, "sub": sub, "email": email, "name": sub})


def make_oidc(role, net, admin):
    """OIDC signup -> admin approves with role + network scope -> sign in."""
    sub = f"parity-{role}-{TAG}"
    email = f"{sub}@example.org"
    oidc_roundtrip(sub, email)
    uid = psql("SELECT id FROM sensorthings.\"User\" "
               f"WHERE external_sub_id='{sub}'")
    requests.patch(f"{BASE}/Users/{uid}/policy-approval",
                   headers={"Authorization": f"Bearer {admin}"},
                   json={"role": role, "dataset": net}, timeout=20)
    r = oidc_roundtrip(sub, email)
    return sub, uid, r.json().get("access_token")


def probe(tok, uid, ctx, side, phase):
    """Run the identical battery. Returns {operation: status_code}.

    `side` ("loc"/"oidc") only makes create payloads unique -- the two sides
    must never collide with each other's rows, or the second one to run
    would get a spurious 409 that looks like a privilege difference.

    `phase` splits reads from writes. Both sides must run every READ before
    either runs a WRITE: otherwise the first side's POST/DELETE changes the
    row count the second side then measures, and the harness reports a
    one-row difference that has nothing to do with privileges.
    """
    h = {"Authorization": f"Bearer {tok}"}
    hw = {**h, "commit-message": "parity"}
    out = {}

    def go(name, fn):
        try:
            out[name] = fn().status_code
        except Exception as exc:  # noqa: BLE001
            out[name] = f"ERR:{type(exc).__name__}"

    # ---- reads ----------------------------------------------------
    if phase == "read":
      for ent in ("Things", "Sensors", "ObservedProperties", "Locations",
                "FeaturesOfInterest", "HistoricalLocations", "Datastreams",
                "Observations", "Networks", "Commits"):
        go(f"GET /{ent}",
             lambda e=ent: requests.get(f"{BASE}/{e}", params={"$top": 1},
                                        headers=h, timeout=30))
      # row counts must match too, not just the status code
      for ent in ("Datastreams", "Observations"):
        try:
            r = requests.get(f"{BASE}/{ent}",
                             params={"$count": "true", "$top": 1},
                             headers=h, timeout=30)
            out[f"count {ent}"] = r.json().get("@iot.count") \
                if r.status_code == 200 else f"HTTP{r.status_code}"
        except Exception:  # noqa: BLE001
            out[f"count {ent}"] = "ERR"
      go("GET /Datastreams $expand",
         lambda: requests.get(f"{BASE}/Datastreams",
                              params={"$top": 2, "$expand": "Thing"},
                              headers=h, timeout=30))
      go("GET /Users", lambda: requests.get(f"{BASE}/Users", headers=h,
                                            timeout=20))
      go("GET /Policies", lambda: requests.get(f"{BASE}/Policies", headers=h,
                                               timeout=20))
      return out


    # ---- writes on entities inside the caller's own scope ----------
    ds = ctx["ds_in_scope"]
    go("PATCH /Datastreams (in scope)",
       lambda: requests.patch(f"{BASE}/Datastreams({ds})", headers=hw,
                              json={"description": f"parity {TAG} {side}"},
                              timeout=30))
    go("PATCH /Datastreams (out of scope)",
       lambda: requests.patch(f"{BASE}/Datastreams({ctx['ds_out_scope']})",
                              headers=hw, json={"description": "x"},
                              timeout=30))
    go("PATCH /Things",
       lambda: requests.patch(f"{BASE}/Things({ctx['thing']})", headers=hw,
                              json={"description": f"parity {TAG} {side}"},
                              timeout=30))
    go("PATCH /Observations",
       lambda: requests.patch(f"{BASE}/Observations({ctx['obs']})", headers=hw,
                              json={"resultQuality": "checked"}, timeout=30))
    go("POST /Observations",
       lambda: requests.post(f"{BASE}/Observations", headers=hw, timeout=30,
                             json={"phenomenonTime": ctx["ptime"],
                                   "result": 1.0,
                                   "Datastream": {"@iot.id": int(ds)}}))
    go("POST /Things",
       lambda: requests.post(f"{BASE}/Things", headers=hw, timeout=30,
                             json={"name": f"parity_{TAG}_{side}",
                                   "description": "parity harness"}))
    go("DELETE /Observations",
       lambda: requests.delete(f"{BASE}/Observations({ctx['obs']})",
                               headers=hw, timeout=30))

    # ---- admin-only surface: both must be refused ------------------
    go("POST /Users",
       lambda: requests.post(f"{BASE}/Users", headers=h, timeout=20,
                             json={"username": f"esc_{TAG}", "password": PW,
                                   "role": "viewer"}))
    go("PATCH own role (escalate)",
       lambda: requests.patch(f"{BASE}/Users/{uid}/role", headers=h,
                              json={"role": "administrator"}, timeout=20))

    # ---- session / account self-service ---------------------------
    go("POST /Refresh", lambda: requests.post(f"{BASE}/Refresh", headers=h,
                                              timeout=20))
    go("PATCH own password",
       lambda: requests.patch(f"{BASE}/Users/{uid}/password", headers=h,
                              timeout=20,
                              json={"current_password": PW,
                                    "new_password": "AnotherGoodPass9!"}))
    return out


def main():
    print(f"parity run {TAG}   {BASE}\n")
    admin = admin_token()

    nets = psql('SELECT name FROM sensorthings."Network" ORDER BY id').split("\n")
    net, other = nets[0], nets[-1]
    ds_in = psql('SELECT d.id FROM sensorthings."Datastream" d '
                 'JOIN sensorthings."Network" n ON n.id = d.network_id '
                 f"WHERE n.name = '{net}' ORDER BY d.id LIMIT 1")
    ds_out = psql('SELECT d.id FROM sensorthings."Datastream" d '
                  'JOIN sensorthings."Network" n ON n.id = d.network_id '
                  f"WHERE n.name = '{other}' ORDER BY d.id LIMIT 1")
    thing = psql('SELECT id FROM sensorthings."Thing" ORDER BY id LIMIT 1')
    print(f"scope network '{net}'   in-scope datastream {ds_in}   "
          f"out-of-scope {ds_out} (network '{other}')\n")

    for role in ROLES:
        # each side gets its own Observation to mutate, so a destructive op
        # by one cannot change the other's result
        obs_l = psql('SELECT o.id FROM sensorthings."Observation" o '
                     f"WHERE o.datastream_id = {ds_in} ORDER BY o.id LIMIT 1")
        obs_o = psql('SELECT o.id FROM sensorthings."Observation" o '
                     f"WHERE o.datastream_id = {ds_in} ORDER BY o.id OFFSET 1 LIMIT 1")

        lu, luid, ltok = make_local(role, net, admin)
        ou, ouid, otok = make_oidc(role, net, admin)
        if not ltok or not otok:
            print(f"[{role}] SETUP FAILED  local_token={bool(ltok)} "
                  f"oidc_token={bool(otok)}")
            _breaks.append((role, "login", "token issued", "no token"))
            continue

        base = {"ds_in_scope": ds_in, "ds_out_scope": ds_out, "thing": thing}
        # Year 2099 is well past the dummy-data range (which runs to 2027), and
        # (role, side) makes each timestamp unique, so POST /Observations never
        # collides with seed data or a prior run.
        ri = ROLES.index(role) + 1
        lctx = {**base, "obs": obs_l, "ptime": f"2099-{ri:02d}-01T01:00:00Z"}
        octx = {**base, "obs": obs_o, "ptime": f"2099-{ri:02d}-01T02:00:00Z"}
        # both sides read the SAME database state, then both write
        L = probe(ltok, luid, lctx, "loc", "read")
        O = probe(otok, ouid, octx, "oidc", "read")
        L.update(probe(ltok, luid, lctx, "loc", "write"))
        O.update(probe(otok, ouid, octx, "oidc", "write"))
        psql(f"""DELETE FROM sensorthings."Observation"
                 WHERE "phenomenonTimeStart" >= '2099-{ri:02d}-01'
                   AND "phenomenonTimeStart" <  '2099-{ri:02d}-02'""")

        diffs = [(k, L[k], O[k]) for k in L if L[k] != O[k]]
        real = [d for d in diffs if d[0] not in EXPECTED_DIFFS]
        expected = [d for d in diffs if d[0] in EXPECTED_DIFFS]
        mark = "MATCH" if not real else f"{len(real)} BREAK"
        if expected:
            mark += f"  (+{len(expected)} expected)"
        print(f"[{role:<11}] local={lu:<22} oidc-sub={ou:<26} {mark}")
        for k, lv, ov in expected:
            print(f"      by design: {k:<26} local={lv!s:<10} oidc={ov!s}")
        for k, lv, ov in real:
            print(f"      BREAK:     {k:<26} local={lv!s:<10} oidc={ov!s}")
            _breaks.append((role, k, lv, ov))
        _rows.append((role, L, O))

    # ---------------------------------------------------------------
    print("\n" + "=" * 72)
    if not _breaks:
        n = len(_rows[0][1]) if _rows else 0
        print(f"  PARITY HOLDS — {len(_rows)} roles x {n} operations.")
        print("  Local and OIDC users are identical in every cell except the")
        print("  documented password-change difference (see EXPECTED_DIFFS).")
        sys.exit(0)
    print(f"  {len(_breaks)} PARITY BREAK(S)")
    for role, op, lv, ov in _breaks:
        print(f"    {role:<12} {op:<34} local={lv!s:<10} oidc={ov!s}")
    sys.exit(1)


if __name__ == "__main__":
    main()
