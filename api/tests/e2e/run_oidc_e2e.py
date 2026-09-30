#!/usr/bin/env python3
# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License").
#
# End-to-end test of the istSOS4 EXTERNAL (OIDC) authentication pathway,
# including network-scoped access to Datastreams and Observations.
#
# It drives the real browser round trip against a fake OpenID Connect
# provider (see api/tests/e2e/fake_oidc/), so Authlib inside the API does
# a genuine authorization-code exchange + id_token/JWKS validation -- only
# the identity is canned.
#
# Prerequisites:
#   docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --build
#   (wait for istsos4-database healthy + dummy data loaded)
#
# Run:
#   python api/tests/e2e/run_oidc_e2e.py
#
# Env (optional):
#   ISTSOS_BASE   default http://localhost:8018/istsos4/v1.1
#   FAKE_BASE     default http://localhost:9000
#   ADMIN_USER / ADMIN_PASS   default admin / admin
#   DB_CONTAINER  default istsos4-database

import os
import subprocess
import sys
import urllib.parse

import requests

BASE = os.getenv("ISTSOS_BASE", "http://localhost:8018/istsos4/v1.1")
FAKE = os.getenv("FAKE_BASE", "http://localhost:9000")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
DB = os.getenv("DB_CONTAINER", "istsos4-database")
PROVIDER = "google"  # the fake is wired in as the "google" client

_fail = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f"  ({detail})" if detail else ""))
    if not cond:
        _fail.append(name)


def psql(sql):
    out = subprocess.run(
        ["docker", "exec", DB, "psql", "-U", "postgres", "-d", "istsos",
         "-tAc", sql],
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


def admin_token():
    r = requests.post(f"{BASE}/Login",
                      data={"username": ADMIN_USER, "password": ADMIN_PASS})
    r.raise_for_status()
    return r.json()["access_token"]


def oidc_roundtrip(session, dataset_id=None, requested_role=None):
    """Drive login -> fake authorize -> callback. Returns the final
    requests.Response from the istSOS callback."""
    params = {}
    if dataset_id is not None:
        params["dataset_id"] = dataset_id
    if requested_role is not None:
        params["requested_role"] = requested_role
    r1 = session.get(f"{BASE}/auth/{PROVIDER}/login", params=params,
                     allow_redirects=False)
    assert r1.status_code == 302, f"/login expected 302, got {r1.status_code}"
    authorize_url = r1.headers["location"]
    # the discovery doc uses the container hostname; the driver reaches the
    # fake on the published port instead.
    authorize_url = authorize_url.replace("http://oidc-fake:9000", FAKE)

    r2 = session.get(authorize_url, allow_redirects=False)
    assert r2.status_code == 302, f"fake /authorize expected 302, got {r2.status_code}"
    callback_url = r2.headers["location"]

    # callback_url is whatever redirect_uri Authlib built; make sure the
    # host is reachable from the driver.
    parsed = urllib.parse.urlparse(callback_url)
    callback_url = callback_url.replace(
        f"{parsed.scheme}://{parsed.netloc}", BASE.rsplit("/istsos4", 1)[0]
    )
    return session.get(callback_url, allow_redirects=False)


def main():
    print(f"istSOS: {BASE}   fake OIDC: {FAKE}")

    # --- 0. sanity ------------------------------------------------------
    try:
        d = requests.get(f"{FAKE}/.well-known/openid-configuration", timeout=5)
        check("fake OIDC provider reachable", d.status_code == 200)
    except Exception as e:  # noqa: BLE001
        check("fake OIDC provider reachable", False, str(e))
        _report()

    tok = admin_token()
    check("admin login", bool(tok))

    # This test drives the SAME fixed identity (FAKE_OIDC_SUB) every run and
    # step 1 asserts a fresh signup -> 202 pending. If an earlier run (or the
    # acceptance / parity harness, which also hit this provider) already
    # provisioned + activated that identity, step 1 would see an existing
    # active user instead. Delete the row first so the run is order-independent.
    #
    # A plain DELETE on "User" trips AuditLog_actor_id_fkey's ON DELETE SET
    # NULL, which runs as the AuditLog owner ("administrator") -- deliberately
    # never granted UPDATE on the append-only AuditLog. Grant it for the
    # length of the delete only.
    external_sub = os.getenv("FAKE_OIDC_SUB", "fake-sub-001")
    psql(
        'GRANT UPDATE ON sensorthings."AuditLog" TO "administrator"; '
        'DELETE FROM sensorthings."User" '
        f"WHERE auth_provider='{PROVIDER}' AND external_sub_id='{external_sub}'; "
        'REVOKE UPDATE ON sensorthings."AuditLog" FROM "administrator";'
    )

    # ground truth: pick a network and count its datastreams / observations
    net_id, net_name = psql(
        'SELECT id, name FROM sensorthings."Network" ORDER BY id LIMIT 1'
    ).split("|")
    ds_in_net = int(psql(
        f'SELECT count(*) FROM sensorthings."Datastream" WHERE network_id = {net_id}'
    ))
    ds_total = int(psql('SELECT count(*) FROM sensorthings."Datastream"'))
    obs_in_net = int(psql(
        'SELECT count(*) FROM sensorthings."Observation" o '
        'JOIN sensorthings."Datastream" d ON d.id = o.datastream_id '
        f'WHERE d.network_id = {net_id}'
    ))
    obs_total = int(psql('SELECT count(*) FROM sensorthings."Observation"'))
    print(f"  network '{net_name}' (id {net_id}): "
          f"{ds_in_net}/{ds_total} datastreams, {obs_in_net}/{obs_total} obs")
    check("test network has a subset of datastreams",
          0 < ds_in_net < ds_total, f"{ds_in_net}/{ds_total}")

    # --- 1. first OIDC login: unknown identity -> pending -------------
    s1 = requests.Session()
    r = oidc_roundtrip(s1, dataset_id=net_name, requested_role="viewer")
    check("first OIDC callback -> 202 (pending)", r.status_code == 202,
          f"HTTP {r.status_code}: {r.text[:120]}")

    row = psql(
        "SELECT id, role, status, dataset_id FROM sensorthings.\"User\" "
        f"WHERE auth_provider='{PROVIDER}' AND external_sub_id='{external_sub}'"
    )
    check("pending OIDC user row created", bool(row), row)
    uid, role, st, ds_col = (row.split("|") + ["", "", ""])[:4]
    check("new OIDC user role == pending", role == "pending", role)
    check("new OIDC user status == pending", st == "pending", st)
    check("requested dataset stored on the row", ds_col == net_name,
          f"{ds_col!r}")

    # --- 2. second OIDC login while still pending -> still 202 --------
    s2 = requests.Session()
    r = oidc_roundtrip(s2)
    check("OIDC login while pending -> 202", r.status_code == 202,
          f"HTTP {r.status_code}")

    # --- 3. admin approves with the network scope ---------------------
    r = requests.patch(
        f"{BASE}/Users/{uid}/policy-approval",
        headers={"Authorization": f"Bearer {tok}"},
        json={"role": "viewer", "dataset": net_name},
    )
    check("admin approval (role=viewer, dataset=network) -> 200",
          r.status_code == 200, f"HTTP {r.status_code}: {r.text[:150]}")
    role_after, ds_after = psql(
        f'SELECT role, dataset_id FROM sensorthings."User" WHERE id = {uid}'
    ).split("|")
    check("user role == viewer after activation", role_after == "viewer")
    check("user dataset_id == network name", ds_after == net_name)

    # --- 4. OIDC login now succeeds with a real JWT -----------------
    s3 = requests.Session()
    r = oidc_roundtrip(s3)
    check("OIDC login after activation -> 200 + token", r.status_code == 200,
          f"HTTP {r.status_code}")
    jwt_tok = r.json().get("access_token") if r.status_code == 200 else None
    check("callback returned an access_token", bool(jwt_tok))

    # --- 5. the OIDC user sees ONLY its network's data -------------
    h = {"Authorization": f"Bearer {jwt_tok}"}
    dc = requests.get(f"{BASE}/Datastreams",
                      params={"$count": "true", "$top": 1}, headers=h)
    ds_seen = dc.json().get("@iot.count")
    check("OIDC user datastream count == network subset",
          ds_seen == ds_in_net, f"saw {ds_seen}, expected {ds_in_net}")

    oc = requests.get(f"{BASE}/Observations",
                      params={"$count": "true", "$top": 1}, headers=h)
    obs_seen = oc.json().get("@iot.count")
    check("OIDC user observation count == network subset",
          obs_seen == obs_in_net, f"saw {obs_seen}, expected {obs_in_net}")

    nets = requests.get(
        f"{BASE}/Datastreams",
        params={"$top": 100, "$expand": "Network($select=name)"}, headers=h,
    ).json().get("value", [])
    net_names = {x.get("Network", {}).get("name") for x in nets}
    check("every datastream the OIDC user sees is in its network",
          net_names == {net_name}, str(net_names))

    # --- 6. a write outside the model is still blocked -------------
    w = requests.patch(
        f"{BASE}/Datastreams({nets[0]['@iot.id']})" if nets else f"{BASE}/Datastreams(0)",
        headers={**h, "commit-message": "e2e"}, json={"name": "e2e-nope"},
    )
    check("OIDC viewer cannot write (403/404/401, not 2xx)",
          w.status_code >= 400, f"HTTP {w.status_code}")

    _report()


def _report():
    print()
    if _fail:
        print(f"RESULT: {len(_fail)} FAILED -> " + ", ".join(_fail))
        sys.exit(1)
    print("RESULT: all checks passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
