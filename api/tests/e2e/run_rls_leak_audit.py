#!/usr/bin/env python3
# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License").
#
# RLS / Network-scoping leak audit.
#
# Sets up TWO networks, A and B, each with its own Thing -> Datastream ->
# Observations chain and its own Location. Then, as a user scoped to A ONLY,
# tries every way to reach or mutate B's data:
#
#   reads  : direct list, by-id, navigation, $expand, $filter, $orderby,
#            related entities (Thing/Sensor/Location/FoI/ObservedProperty)
#   writes : PATCH/PUT/DELETE/POST on B's Datastream, Observation, Thing,
#            Sensor, Location, and on Network B itself
#
# Every check states what SHOULD happen. A leak = the scoped user reached
# or changed B's data.
#
#   docker compose -f docker-compose.yml -f docker-compose.e2e.yml up -d --build
#   python api/tests/e2e/run_rls_leak_audit.py

import os
import subprocess
import sys
import uuid

import requests

BASE = os.getenv("ISTSOS_BASE", "http://localhost:8018/istsos4/v1.1")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
DB = os.getenv("DB_CONTAINER", "istsos4-database")
TAG = uuid.uuid4().hex[:6]
PW = "CorrectHorseBattery1!"

_leaks = []
_notes = []
_ok = 0


def psql(sql):
    return subprocess.run(
        ["docker", "exec", DB, "psql", "-U", "postgres", "-d", "istsos", "-tAc", sql],
        capture_output=True, text=True).stdout.strip()


def admin_tok():
    return requests.post(f"{BASE}/Login",
                         data={"username": ADMIN_USER, "password": ADMIN_PASS},
                         timeout=15).json()["access_token"]


def expect(name, cond_ok, detail=""):
    """cond_ok True => behaved correctly (no leak)."""
    global _ok
    if cond_ok:
        _ok += 1
        print(f"  [ ok ] {name}" + (f"   ({detail})" if detail else ""))
    else:
        _leaks.append((name, detail))
        print(f"  [LEAK] {name}   ({detail})")


def note(name, cond_ok, detail=""):
    """Pre-existing / out-of-scope observation — reported, does not fail the run."""
    global _ok
    if cond_ok:
        _ok += 1
        print(f"  [ ok ] {name}" + (f"   ({detail})" if detail else ""))
    else:
        _notes.append((name, detail))
        print(f"  [note] {name}   ({detail})")


A = "administrator"  # placeholder to avoid shadowing


def main():
    global A
    adm = admin_tok()
    A = {"Authorization": f"Bearer {adm}"}
    print(f"RLS leak audit {TAG}   {BASE}\n")

    # ------------------------------------------------------------------
    # 0. ground truth: two networks, disjoint data
    # ------------------------------------------------------------------
    nets = psql('SELECT string_agg(name, \'|\' ORDER BY id) FROM sensorthings."Network"').split("|")
    net_a, net_b = nets[0], nets[-1]
    a_id = psql(f"SELECT id FROM sensorthings.\"Network\" WHERE name='{net_a}'")
    b_id = psql(f"SELECT id FROM sensorthings.\"Network\" WHERE name='{net_b}'")
    a_ds = psql(f"SELECT id FROM sensorthings.\"Datastream\" WHERE network_id={a_id} ORDER BY id LIMIT 1")
    b_ds = psql(f"SELECT id FROM sensorthings.\"Datastream\" WHERE network_id={b_id} ORDER BY id LIMIT 1")
    b_ds_count = int(psql(f'SELECT count(*) FROM sensorthings."Datastream" WHERE network_id={b_id}'))
    b_obs = psql(f"SELECT o.id FROM sensorthings.\"Observation\" o WHERE o.datastream_id={b_ds} ORDER BY o.id LIMIT 1")
    b_obs_count = int(psql(f'SELECT count(*) FROM sensorthings."Observation" o '
                           f'JOIN sensorthings."Datastream" d ON d.id=o.datastream_id WHERE d.network_id={b_id}'))
    # B's Thing / Sensor / Location / ObservedProperty / FoI
    b_thing = psql(f"SELECT t.id FROM sensorthings.\"Thing\" t "
                   f"JOIN sensorthings.\"Datastream\" d ON d.thing_id=t.id WHERE d.network_id={b_id} LIMIT 1")
    b_sensor = psql(f"SELECT s.id FROM sensorthings.\"Sensor\" s "
                    f"JOIN sensorthings.\"Datastream\" d ON d.sensor_id=s.id WHERE d.network_id={b_id} LIMIT 1")
    b_loc = psql(f"SELECT l.id FROM sensorthings.\"Location\" l "
                 f"JOIN sensorthings.\"Thing_Location\" tl ON tl.location_id=l.id "
                 f"JOIN sensorthings.\"Datastream\" d ON d.thing_id=tl.thing_id WHERE d.network_id={b_id} LIMIT 1")
    b_foi = psql(f"SELECT o.\"featuresOfInterest_id\" FROM sensorthings.\"Observation\" o "
                 f"JOIN sensorthings.\"Datastream\" d ON d.id=o.datastream_id "
                 f"WHERE d.network_id={b_id} AND o.\"featuresOfInterest_id\" IS NOT NULL LIMIT 1")

    print(f"  network A = '{net_a}' (id {a_id})   datastream {a_ds}")
    print(f"  network B = '{net_b}' (id {b_id})   {b_ds_count} datastreams, {b_obs_count} obs")
    print(f"    B: thing={b_thing} sensor={b_sensor} location={b_loc} foi={b_foi} ds={b_ds} obs={b_obs}\n")

    if not (a_ds and b_ds and b_obs):
        print("SETUP FAILED — need 2 networks with data. Is the e2e stack up with dummy data?")
        sys.exit(2)

    # ------------------------------------------------------------------
    # helper: make a scoped user of a given role
    # ------------------------------------------------------------------
    def scoped_user(role):
        u = f"leak_{role}_{TAG}"
        requests.post(f"{BASE}/Register", timeout=20, json={
            "username": u, "password": PW,
            "contact_info": {"email": f"{u}@x.org"},
            "explanation": "leak audit", "dataset_id": net_a,
            "requested_role": role})
        uid = psql(f"SELECT id FROM sensorthings.\"User\" WHERE username='{u}'")
        requests.patch(f"{BASE}/Users/{uid}/policy-approval", headers=A,
                       json={"role": role, "dataset": net_a}, timeout=20)
        r = requests.post(f"{BASE}/Login", data={"username": u, "password": PW}, timeout=15)
        return r.json().get("access_token"), uid

    def count(tok, path, params=None):
        r = requests.get(f"{BASE}{path}", headers={"Authorization": f"Bearer {tok}"},
                         params={**(params or {}), "$count": "true", "$top": 1}, timeout=30)
        if r.status_code != 200:
            return f"HTTP{r.status_code}"
        return r.json().get("@iot.count")

    def body_val(tok, path, params=None):
        r = requests.get(f"{BASE}{path}", headers={"Authorization": f"Bearer {tok}"},
                         params=params or {}, timeout=30)
        return r.status_code, (r.json() if r.headers.get("content-type", "").startswith("application/json") else {})

    # ==================================================================
    print("── READ: viewer scoped to A, trying to see B's Datastreams/Observations")
    # ==================================================================
    vt, vid = scoped_user("viewer")

    expect("GET /Datastreams excludes B",
           count(vt, "/Datastreams") == int(psql(f'SELECT count(*) FROM sensorthings."Datastream" WHERE network_id={a_id}')),
           f"sees {count(vt, '/Datastreams')}")
    expect("GET /Observations excludes B",
           count(vt, "/Observations") == int(psql(f'SELECT count(*) FROM sensorthings."Observation" o JOIN sensorthings."Datastream" d ON d.id=o.datastream_id WHERE d.network_id={a_id}')),
           f"sees {count(vt, '/Observations')}")

    sc, _ = body_val(vt, f"/Datastreams({b_ds})")
    expect("GET /Datastreams(B_id) by direct id -> 404", sc == 404, f"HTTP {sc}")
    sc, _ = body_val(vt, f"/Observations({b_obs})")
    expect("GET /Observations(B_obs) by direct id -> 404", sc == 404, f"HTTP {sc}")

    expect("nav /Networks(B)/Datastreams -> 0",
           count(vt, f"/Networks({b_id})/Datastreams") in (0, "HTTP404"),
           f"{count(vt, f'/Networks({b_id})/Datastreams')}")

    # A Thing/Sensor can legitimately be shared across networks, so the
    # right check is not "count 0" but "every datastream returned is in A".
    def nav_stays_in_a(path):
        sc, j = body_val(vt, path, {"$top": 200, "$expand": "Network($select=name)"})
        if sc != 200:
            return sc in (403, 404)
        return all(d.get("Network", {}).get("name") in (net_a, None)
                   for d in j.get("value", []))

    expect("nav /Things(B_thing)/Datastreams stays in A",
           nav_stays_in_a(f"/Things({b_thing})/Datastreams"),
           "a B-network datastream surfaced")
    expect("nav /Sensors(B_sensor)/Datastreams stays in A",
           nav_stays_in_a(f"/Sensors({b_sensor})/Datastreams"),
           "a B-network datastream surfaced")
    if b_foi:
        sc, j = body_val(vt, f"/FeaturesOfInterest({b_foi})/Observations", {"$top": 5})
        expect("nav /FeaturesOfInterest(B_foi)/Observations -> 0/blocked",
               sc in (403, 404) or not j.get("value"),
               f"HTTP {sc}, {len(j.get('value', []))} rows")

    # $expand from an unscoped entity into scoped ones
    sc, j = body_val(vt, "/Things", {"$top": 200, "$expand": "Datastreams($select=id,network_id)"})
    expanded = [ds for t in j.get("value", []) for ds in t.get("Datastreams", [])]
    bad = [ds for ds in expanded if str(ds.get("network_id")) == str(b_id)]
    expect("$expand Things/Datastreams leaks no B datastream", not bad,
           f"{len(bad)} B-datastreams surfaced via expand")

    sc, j = body_val(vt, "/Networks", {"$top": 200, "$expand": "Datastreams($select=id)"})
    nb = [ds for n in j.get("value", []) if str(n.get("@iot.id")) == str(b_id)
          for ds in n.get("Datastreams", [])]
    expect("$expand Networks/Datastreams leaks no B datastream", not nb,
           f"{len(nb)} under network B")

    # $filter fishing for B
    expect("$filter network_id eq B -> 0",
           count(vt, "/Datastreams", {"$filter": f"network_id eq {b_id}"}) in (0, "HTTP400", "HTTP404"),
           f"{count(vt, '/Datastreams', {'$filter': f'network_id eq {b_id}'})}")
    expect("$filter id eq B_ds -> 0",
           count(vt, "/Datastreams", {"$filter": f"id eq {b_ds}"}) in (0, "HTTP400", "HTTP404"),
           f"{count(vt, '/Datastreams', {'$filter': f'id eq {b_ds}'})}")

    # deep expand: Observations -> Datastream -> Network
    sc, j = body_val(vt, "/Observations", {"$top": 50, "$expand": "Datastream($expand=Network($select=name))"})
    seen_nets = {o.get("Datastream", {}).get("Network", {}).get("name") for o in j.get("value", [])}
    expect("deep $expand Observations->Datastream->Network stays in A",
           seen_nets <= {net_a, None}, f"networks seen: {seen_nets}")

    # ==================================================================
    print("\n── READ: what an unscoped-but-not-secret table exposes (by design)")
    # ==================================================================
    for ent in ("Things", "Sensors", "Locations", "ObservedProperties",
                "FeaturesOfInterest", "HistoricalLocations", "Networks"):
        c = count(vt, f"/{ent}")
        total = psql(f'SELECT count(*) FROM sensorthings."{ent[:-1] if ent!="FeaturesOfInterest" else "FeaturesOfInterest"}"') \
            if ent not in ("FeaturesOfInterest",) else psql('SELECT count(*) FROM sensorthings."FeaturesOfInterest"')
        tag = "design: not network-scoped" if str(c) == str(total) else ""
        print(f"       /{ent:<20} viewer sees {c}/{total}   {tag}")

    # ==================================================================
    print("\n── WRITE: editor scoped to A, trying to mutate B")
    # ==================================================================
    et, eid = scoped_user("editor")
    EW = {"Authorization": f"Bearer {et}", "commit-message": f"leak-{TAG}"}

    def mutated(sql_before, sql_after_expr):
        return psql(sql_before) != psql(sql_after_expr)

    # B's Datastream
    before = psql(f"SELECT name FROM sensorthings.\"Datastream\" WHERE id={b_ds}")
    r = requests.patch(f"{BASE}/Datastreams({b_ds})", headers=EW, json={"name": f"hacked_{TAG}"}, timeout=20)
    after = psql(f"SELECT name FROM sensorthings.\"Datastream\" WHERE id={b_ds}")
    expect("editor PATCH B's Datastream blocked", before == after and r.status_code >= 400,
           f"HTTP {r.status_code}, name {'unchanged' if before == after else 'CHANGED'}")

    r = requests.delete(f"{BASE}/Datastreams({b_ds})", headers=EW, timeout=20)
    still = psql(f"SELECT count(*) FROM sensorthings.\"Datastream\" WHERE id={b_ds}")
    expect("editor DELETE B's Datastream blocked", still == "1",
           f"HTTP {r.status_code}, row {'gone!' if still == '0' else 'intact'}")

    # B's Observation
    before = psql(f"SELECT \"resultQuality\" FROM sensorthings.\"Observation\" WHERE id={b_obs}")
    r = requests.patch(f"{BASE}/Observations({b_obs})", headers=EW, json={"resultQuality": f"hacked_{TAG}"}, timeout=20)
    after = psql(f"SELECT \"resultQuality\" FROM sensorthings.\"Observation\" WHERE id={b_obs}")
    expect("editor PATCH B's Observation blocked", before == after,
           f"HTTP {r.status_code}, {'CHANGED' if before != after else 'unchanged'}")

    r = requests.delete(f"{BASE}/Observations({b_obs})", headers=EW, timeout=20)
    still = psql(f"SELECT count(*) FROM sensorthings.\"Observation\" WHERE id={b_obs}")
    expect("editor DELETE B's Observation blocked", still == "1",
           f"HTTP {r.status_code}, {'GONE' if still == '0' else 'intact'}")

    # new Observation on B's datastream
    cnt_before = psql(f'SELECT count(*) FROM sensorthings."Observation" WHERE datastream_id={b_ds}')
    r = requests.post(f"{BASE}/Observations", headers=EW, timeout=20,
                      json={"phenomenonTime": "2035-01-01T00:00:00Z", "result": 1,
                            "Datastream": {"@iot.id": int(b_ds)}})
    cnt_after = psql(f'SELECT count(*) FROM sensorthings."Observation" WHERE datastream_id={b_ds}')
    expect("editor POST Observation onto B's Datastream blocked", cnt_before == cnt_after,
           f"HTTP {r.status_code}, count {cnt_before}->{cnt_after}")

    # Shared reference tables: a SCOPED editor must not be able to
    # UPDATE / DELETE an existing Thing / Sensor / Location / ObservedProperty.
    # (A network-exclusive Sensor is the sharpest case -- deleting it
    # cascades into another network's Datastreams.)
    b_sensor_excl = psql(
        'SELECT s.id FROM sensorthings."Sensor" s '
        'WHERE (SELECT count(DISTINCT d.network_id) FROM sensorthings."Datastream" d '
        f'       WHERE d.sensor_id = s.id) = 1 '
        f'  AND EXISTS (SELECT 1 FROM sensorthings."Datastream" d '
        f'              WHERE d.sensor_id = s.id AND d.network_id = {b_id}) LIMIT 1')
    b_op = psql(f'SELECT DISTINCT d."observedProperty_id" FROM sensorthings."Datastream" d '
                f'WHERE d.network_id={b_id} LIMIT 1')
    for label, ent, oid, col in [
        ("Thing", "Thing", b_thing, "name"),
        ("Sensor", "Sensor", b_sensor_excl or b_sensor, "name"),
        ("Location", "Location", b_loc, "name"),
        ("ObservedProperty", "ObservedProperty", b_op, "name"),
    ]:
        if not oid:
            continue
        before = psql(f'SELECT "{col}" FROM sensorthings."{ent}" WHERE id={oid}')
        r = requests.patch(f"{BASE}/{label}s({oid})", headers=EW, json={col: f"hack_{TAG}"}, timeout=20)
        after = psql(f'SELECT "{col}" FROM sensorthings."{ent}" WHERE id={oid}')
        if before != after:
            psql(f"UPDATE sensorthings.\"{ent}\" SET \"{col}\"='{before}' WHERE id={oid}")
        expect(f"scoped editor PATCH a shared {label} blocked", before == after,
               f"HTTP {r.status_code}, {'CHANGED' if before != after else 'unchanged'}")
        rc = psql(f'SELECT count(*) FROM sensorthings."{ent}" WHERE id={oid}')
        r = requests.delete(f"{BASE}/{label}s({oid})", headers=EW, timeout=20)
        rc2 = psql(f'SELECT count(*) FROM sensorthings."{ent}" WHERE id={oid}')
        expect(f"scoped editor DELETE a shared {label} blocked", rc == rc2,
               f"HTTP {r.status_code}, {'DELETED' if rc != rc2 else 'intact'}")

    # positive control: a SCOPED editor may still CREATE a new reference row
    # (onboarding), just not mutate shared existing ones.
    r = requests.post(f"{BASE}/Sensors", headers=EW, timeout=20,
                      json={"name": f"onboard_{TAG}", "description": "d",
                            "encodingType": "application/pdf", "metadata": "x"})
    made = psql(f"SELECT count(*) FROM sensorthings.\"Sensor\" WHERE name='onboard_{TAG}'")
    expect("scoped editor CAN still POST a new Sensor (onboarding)", made == "1",
           f"HTTP {r.status_code}")
    psql(f"DELETE FROM sensorthings.\"Sensor\" WHERE name='onboard_{TAG}'")

    # and an UNSCOPED (global) editor keeps full write on shared refs
    gu = f"leak_global_ed_{TAG}"
    requests.post(f"{BASE}/Users", headers=A, timeout=20,
                  json={"username": gu, "password": PW, "role": "editor"})
    gt = requests.post(f"{BASE}/Login", data={"username": gu, "password": PW},
                       timeout=15).json().get("access_token")
    if b_sensor_excl and gt:
        gb = psql(f'SELECT name FROM sensorthings."Sensor" WHERE id={b_sensor_excl}')
        r = requests.patch(f"{BASE}/Sensors({b_sensor_excl})",
                           headers={"Authorization": f"Bearer {gt}", "commit-message": "g"},
                           json={"name": f"global_{TAG}"}, timeout=20)
        ga = psql(f'SELECT name FROM sensorthings."Sensor" WHERE id={b_sensor_excl}')
        if gb != ga:
            psql(f"UPDATE sensorthings.\"Sensor\" SET name='{gb}' WHERE id={b_sensor_excl}")
        expect("UNSCOPED editor CAN write shared refs (trusted global op)", gb != ga,
               f"HTTP {r.status_code}, {'wrote' if gb != ga else 'BLOCKED unexpectedly'}")

    # Network B itself
    before = psql(f"SELECT description FROM sensorthings.\"Network\" WHERE id={b_id}")
    r = requests.patch(f"{BASE}/Networks({b_id})", headers=EW, json={"description": f"hacked_{TAG}"}, timeout=20)
    after = psql(f"SELECT description FROM sensorthings.\"Network\" WHERE id={b_id}")
    expect("editor PATCH Network B blocked", before == after,
           f"HTTP {r.status_code}, {'CHANGED' if before != after else 'unchanged'}")

    r = requests.delete(f"{BASE}/Networks({b_id})", headers=EW, timeout=20)
    still = psql(f"SELECT count(*) FROM sensorthings.\"Network\" WHERE id={b_id}")
    expect("editor DELETE Network B blocked", still == "1",
           f"HTTP {r.status_code}, {'GONE' if still == '0' else 'intact'}")

    ncnt = psql('SELECT count(*) FROM sensorthings."Network"')
    r = requests.post(f"{BASE}/Networks", headers=EW, json={"name": f"rogue_{TAG}", "description": "x"}, timeout=20)
    ncnt2 = psql('SELECT count(*) FROM sensorthings."Network"')
    expect("editor POST new Network blocked", ncnt == ncnt2,
           f"HTTP {r.status_code}, count {ncnt}->{ncnt2}")
    psql(f"DELETE FROM sensorthings.\"Network\" WHERE name='rogue_{TAG}'")

    # ==================================================================
    print("\n── WRITE: sensor scoped to A, trying to mutate B")
    # ==================================================================
    st, sid = scoped_user("sensor")
    SH = {"Authorization": f"Bearer {st}"}
    cnt_before = psql(f'SELECT count(*) FROM sensorthings."Observation" WHERE datastream_id={b_ds}')
    r = requests.post(f"{BASE}/Observations", headers=SH, timeout=20,
                      json={"phenomenonTime": "2035-02-02T00:00:00Z", "result": 2,
                            "Datastream": {"@iot.id": int(b_ds)}})
    cnt_after = psql(f'SELECT count(*) FROM sensorthings."Observation" WHERE datastream_id={b_ds}')
    expect("sensor POST Observation onto B's Datastream blocked", cnt_before == cnt_after,
           f"HTTP {r.status_code}, {cnt_before}->{cnt_after}")

    if b_loc:
        before = psql(f'SELECT name FROM sensorthings."Location" WHERE id={b_loc}')
        r = requests.patch(f"{BASE}/Locations({b_loc})", headers=SH, json={"name": f"shack_{TAG}"}, timeout=20)
        after = psql(f'SELECT name FROM sensorthings."Location" WHERE id={b_loc}')
        if before != after:
            psql(f"UPDATE sensorthings.\"Location\" SET name='{before}' WHERE id={b_loc}")
        expect("scoped sensor PATCH a Location blocked", before == after,
               f"HTTP {r.status_code}, {'CHANGED' if before != after else 'unchanged'}")

    # ==================================================================
    print("\n── custom role: no blanket grant")
    # ==================================================================
    ct, cid = scoped_user("custom")
    expect("custom sees 0 datastreams", count(ct, "/Datastreams") in (0, "HTTP404"),
           f"{count(ct, '/Datastreams')}")
    expect("custom sees 0 observations", count(ct, "/Observations") in (0, "HTTP404"),
           f"{count(ct, '/Observations')}")
    expect("custom sees 0 things", count(ct, "/Things") in (0, "HTTP404"),
           f"{count(ct, '/Things')}")

    # ==================================================================
    print("\n── cross-user metadata (Commit / AuditLog)")
    # ==================================================================
    sc, j = body_val(vt, "/Commits", {"$top": 5})
    # commit.py is byte-identical to upstream; with VERSIONING=0 the
    # dedicated /Commits router is not even registered -- this response comes
    # from read.py's catch-all serving the Commit entity via the query
    # engine. Commit rows carry an edit message + the actor's /Users(N) uri.
    # Pre-existing, spans the versioning subsystem -> mentor decision, not a
    # branch regression. Reported, does not fail the audit.
    note("viewer GET /Commits exposes edit history (pre-existing / versioning)",
         sc in (401, 403) or not j.get("value"),
         f"HTTP {sc}, {len(j.get('value', []))} rows")

    # ------------------------------------------------------------------
    print("\n" + "=" * 68)
    print(f"  {_ok} checks passed")
    if _notes:
        print(f"  {len(_notes)} pre-existing note(s) (not branch regressions):")
        for n, d in _notes:
            print(f"    - {n}   ({d})")
    if _leaks:
        print(f"  {len(_leaks)} LEAK(S):")
        for n, d in _leaks:
            print(f"    - {n}   ({d})")
        sys.exit(1)
    print("  NO LEAKS in the network-scoping model")
    sys.exit(0)


if __name__ == "__main__":
    main()
