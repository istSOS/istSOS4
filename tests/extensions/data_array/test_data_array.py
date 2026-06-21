"""
test_data_array.py -- OGC SensorThings API v1.1 Data Array extension conformance.

Conformance class:  req/data-array/data-array
  (advertised in serverSettings.conformance:
   http://www.opengis.net/spec/iot_sensing/1.1/req/data-array/data-array)

The Data Array extension has two sides, both implemented by istSOS4:

  READ  -- GET .../Observations?$resultFormat=dataArray returns Observations
           grouped per Datastream as a compact table.  Each element of the
           top-level `value` array is a group object with EXACTLY these keys:
             { "Datastream@iot.navigationLink": "...",
               "components": ["id","phenomenonTime","resultTime","result",
                              "resultQuality","validTime","parameters"],
               "dataArray@iot.count": N,
               "dataArray": [ [<row>], ... ] }   # one inner list per Observation
           Both the navigation path (/Datastreams(<id>)/Observations) and the
           collection path (/Observations) emit this shape; the collection path
           groups the matching Observations per Datastream.

  CREATE -- POST /CreateObservations with the inverse representation creates
           many Observations in one request and returns a JSON list of the
           created Observation selfLinks (one element per dataArray row).

SEED (read-only entitiesDefault.json subtree; see conftest.py / sample_data.py):
  DS1 "datastream name 1" -> results [3, 4]  @ 2015-03-03 / 2015-03-04
  DS2 "datastream name 2" -> results [5, 6]  @ 2015-03-05 / 2015-03-06
The local DB is NOT empty and xdist workers each seed an identically named
subtree, so every read assertion is scoped to a seed Datastream id (the nav
path is naturally scoped; the collection-path test locates the seed's own
group among the others without asserting any global totals).
"""

from __future__ import annotations

import pytest

from client import format_id, id_from_self_link

pytestmark = pytest.mark.data_array

# The spec group object carries exactly these four members.
GROUP_KEYS = {
    "Datastream@iot.navigationLink",
    "components",
    "dataArray@iot.count",
    "dataArray",
}


# ---------------------------------------------------------------------------
# helpers (self-contained -- single-file ownership)
# ---------------------------------------------------------------------------
def get_data_array(client, ds, params=None) -> dict:
    """GET a seed Datastream's Observations as a data array (navigation path).

    Returns the single group object for that Datastream.  Asserts 200, a
    well-formed `value` list with exactly one (Datastream-scoped) group, and
    that the group is the DIRECT spec object (no "json" wrapper).
    """
    qp = {"$resultFormat": "dataArray"}
    if params:
        qp.update(params)
    path = f"Datastreams({format_id(ds.id)})/Observations"
    r = client.get(path, params=qp)
    assert r.status_code == 200, (
        f"GET {path} {qp} -> {r.status_code}: {r.text[:300]}"
    )
    body = r.json()
    assert isinstance(body.get("value"), list), "data-array response must have a 'value' list"
    assert len(body["value"]) == 1, (
        f"a Datastream-scoped data array must yield exactly one group, "
        f"got {len(body['value'])}"
    )
    group = body["value"][0]
    assert "json" not in group, (
        f"group must be the direct spec object, not wrapped in a 'json' key: {list(group.keys())}"
    )
    return group


def rows_as_dicts(group: dict) -> list[dict]:
    """Map each dataArray row onto its `components` -> {component: value}."""
    comps = group["components"]
    return [dict(zip(comps, row)) for row in group["dataArray"]]


def column(group: dict, name: str) -> list:
    """Return the per-row values of one component column, in row order."""
    idx = group["components"].index(name)
    return [row[idx] for row in group["dataArray"]]


# ===========================================================================
# 1. READ (nav path) -- strict structure of the data-array response
#    req/data-array/data-array
# ===========================================================================
def test_data_array_read_structure(client, seed):
    """req/data-array/data-array -- GET Observations as a data array yields a
    `value` list whose group object carries EXACTLY the four spec keys, a
    list-of-rows `dataArray`, and `dataArray@iot.count` == number of rows."""
    group = get_data_array(client, seed.ds1)

    # group object: exactly the four spec keys, no extras, no "json" wrapper.
    assert set(group.keys()) == GROUP_KEYS, (
        f"group keys must be exactly {sorted(GROUP_KEYS)}, got {sorted(group.keys())}"
    )

    # components: a list including at least the mandatory id/phenomenonTime/result.
    comps = group["components"]
    assert isinstance(comps, list) and comps, "components must be a non-empty list"
    for required in ("id", "phenomenonTime", "result"):
        assert required in comps, f"components must include '{required}': {comps}"

    # dataArray: a list of rows, each row a list with one value per component.
    data_array = group["dataArray"]
    assert isinstance(data_array, list), "dataArray must be a list"
    assert data_array, "seed Datastream has Observations -> dataArray must be non-empty"
    for row in data_array:
        assert isinstance(row, list), f"each dataArray row must be a list, got {type(row)}"
        assert len(row) == len(comps), (
            f"row length {len(row)} must match components length {len(comps)}"
        )

    # dataArray@iot.count == number of rows; the seed Datastream has 2 Observations.
    assert group["dataArray@iot.count"] == len(data_array), (
        f"dataArray@iot.count ({group['dataArray@iot.count']}) must equal "
        f"the number of rows ({len(data_array)})"
    )
    assert len(data_array) == len(seed.ds1.results) == 2


# ===========================================================================
# 2. READ (nav path) -- values map correctly through the `components` order
#    req/data-array/data-array
# ===========================================================================
def test_data_array_read_values_ds1(client, seed):
    """req/data-array/data-array -- decoding each dataArray row by the components
    order yields the seed DS1 result values and Observation ids."""
    rows = rows_as_dicts(get_data_array(client, seed.ds1))
    assert sorted(float(r["result"]) for r in rows) == [3.0, 4.0] == sorted(
        float(x) for x in seed.ds1.results
    )
    assert {r["id"] for r in rows} == set(seed.ds1.observation_ids)


def test_data_array_read_values_ds2(client, seed):
    """req/data-array/data-array -- same row-decoding check for seed DS2 [5,6]."""
    rows = rows_as_dicts(get_data_array(client, seed.ds2))
    assert sorted(float(r["result"]) for r in rows) == [5.0, 6.0] == sorted(
        float(x) for x in seed.ds2.results
    )
    assert {r["id"] for r in rows} == set(seed.ds2.observation_ids)


# ===========================================================================
# 3. READ (nav path) -- $top limits the rows of the data array
#    req/data-array/data-array  (with req/request-data/top)
# ===========================================================================
def test_data_array_read_top(client, seed):
    """req/data-array/data-array -- $top=1 returns exactly one data-array row
    (and dataArray@iot.count reflects the returned rows)."""
    group = get_data_array(client, seed.ds1, {"$top": 1})
    assert len(group["dataArray"]) == 1, (
        f"$top=1 must return exactly 1 row, got {len(group['dataArray'])}"
    )
    assert group["dataArray@iot.count"] == 1
    # The single returned row is one of the seed's Observations.
    only = rows_as_dicts(group)[0]
    assert only["id"] in set(seed.ds1.observation_ids)
    assert float(only["result"]) in {float(x) for x in seed.ds1.results}


# ===========================================================================
# 4. READ (nav path) -- $orderby sorts the data-array rows
#    req/data-array/data-array  (with req/request-data/orderby)
# ===========================================================================
def test_data_array_read_orderby(client, seed):
    """req/data-array/data-array -- $orderby=phenomenonTime asc/desc returns the
    data-array rows ordered by the phenomenonTime column accordingly."""
    asc = column(get_data_array(client, seed.ds1, {"$orderby": "phenomenonTime asc"}),
                 "phenomenonTime")
    desc = column(get_data_array(client, seed.ds1, {"$orderby": "phenomenonTime desc"}),
                  "phenomenonTime")

    # ISO-8601 UTC strings sort lexicographically == chronologically.
    assert asc == sorted(asc), f"asc rows not ascending by phenomenonTime: {asc}"
    assert desc == sorted(desc, reverse=True), f"desc rows not descending: {desc}"
    assert asc == list(reversed(desc)), "asc and desc must be exact reverses"
    assert len(asc) == 2  # both seed DS1 Observations present


# ===========================================================================
# 5. READ (collection path) -- /Observations groups per Datastream
#    req/data-array/data-array
# ===========================================================================
def test_data_array_collection_path(client, seed):
    """req/data-array/data-array -- GET /Observations?$resultFormat=dataArray
    groups the matching Observations per Datastream as list-of-rows with a
    correct per-group dataArray@iot.count.  Concurrency-safe: locate the seed
    DS1 group among the others (by navigationLink), assert ITS shape only."""
    # Generous $top so every Observation (hence the seed group) is included on
    # the page regardless of how full the shared DB is.
    r = client.get("Observations", params={"$resultFormat": "dataArray", "$top": 100000})
    assert r.status_code == 200, f"collection data-array -> {r.status_code}: {r.text[:300]}"
    groups = r.json()["value"]
    assert isinstance(groups, list) and groups, "collection data-array must return groups"

    suffix = f"Datastreams({format_id(seed.ds1.id)})"
    mine = [g for g in groups if g["Datastream@iot.navigationLink"].endswith(suffix)]
    assert len(mine) == 1, (
        f"exactly one group must match {suffix}; found {len(mine)} among "
        f"{[g['Datastream@iot.navigationLink'] for g in groups]}"
    )
    group = mine[0]

    # Same direct spec shape on the collection path (no "json" wrapper).
    assert "json" not in group
    assert set(group.keys()) == GROUP_KEYS
    data_array = group["dataArray"]
    assert all(isinstance(row, list) for row in data_array), "dataArray must be list-of-rows"
    assert group["dataArray@iot.count"] == len(data_array) == len(seed.ds1.results) == 2, (
        f"seed DS1 group count must be 2, got count={group['dataArray@iot.count']} "
        f"rows={len(data_array)}"
    )
    # Values decode to the seed DS1 results (NOT the old single hard-coded row).
    rows = rows_as_dicts(group)
    assert sorted(float(x["result"]) for x in rows) == [3.0, 4.0]
    assert {x["id"] for x in rows} == set(seed.ds1.observation_ids)


# ===========================================================================
# 6. CREATE -- POST /CreateObservations (Data Array create)
#    req/data-array/data-array
# ===========================================================================
def test_create_observations_data_array(client, seed):
    """req/data-array/data-array -- POST the Data Array `CreateObservations`
    representation to create several Observations on a seed Datastream in one
    request; the response is the list of created selfLinks (one per row).  GET
    each back and verify the round-tripped result.  Every created Observation
    is DELETEd in teardown so the seed Datastream returns to its original size.
    """
    ds = seed.ds1
    posted = [
        ("2099-06-01T00:00:00Z", 90001.5),
        ("2099-06-02T00:00:00Z", 90002.5),
        ("2099-06-03T00:00:00Z", 90003.5),
    ]
    payload = [
        {
            "Datastream": {"@iot.id": ds.id},
            "components": ["phenomenonTime", "result"],
            "dataArray": [[t, str(v)] for t, v in posted],
        }
    ]

    created_ids: list = []
    try:
        resp = client.post("CreateObservations", json=payload)
        assert resp.status_code == 201, (
            f"CreateObservations must return 201, got {resp.status_code}: {resp.text[:300]}"
        )

        links = resp.json()
        assert isinstance(links, list), f"response must be a JSON list, got {type(links)}"
        assert len(links) == len(posted), (
            f"one selfLink per dataArray row expected ({len(posted)}), got {len(links)}: {links}"
        )
        assert all(isinstance(u, str) and "Observations(" in u for u in links), (
            f"each element must be an Observation selfLink, got {links}"
        )

        created_ids = [id_from_self_link(u) for u in links]

        # GET each created Observation back and verify the round-tripped pairing.
        for oid in created_ids:
            obs = client.nav(f"Observations({format_id(oid)})")
            ptime = obs["phenomenonTime"]
            matched = [v for t, v in posted if t[:19] in ptime]
            assert matched and float(obs["result"]) == matched[0], (
                f"Observation {oid}: result/phenomenonTime did not round-trip "
                f"(time={ptime!r}, result={obs['result']!r})"
            )

        # The created results now appear in the seed Datastream's data array.
        all_results = {float(r["result"]) for r in rows_as_dicts(get_data_array(client, ds))}
        assert {v for _, v in posted} <= all_results, (
            "created results must appear in the Datastream data array"
        )
    finally:
        for oid in created_ids:
            try:
                client.delete(f"Observations({format_id(oid)})")
            except Exception:
                pass

    # After cleanup the seed Datastream is back to its original 2 Observations.
    group = get_data_array(client, ds)
    assert group["dataArray@iot.count"] == len(seed.ds1.results) == 2, (
        "created Observations must be fully removed in teardown"
    )


# ===========================================================================
# 7. CREATE -- malformed Data Array payloads are rejected with a clean 4xx
#    req/data-array/data-array
# ===========================================================================
def test_create_observations_missing_result_component(client, seed):
    """req/data-array/data-array -- a CreateObservations payload whose components
    omit the mandatory 'result' is rejected with a client error (istSOS4: 400)."""
    payload = [
        {
            "Datastream": {"@iot.id": seed.ds1.id},
            "components": ["phenomenonTime"],
            "dataArray": [["2099-07-01T00:00:00Z"]],
        }
    ]
    resp = client.post("CreateObservations", json=payload)
    assert 400 <= resp.status_code < 500, (
        f"missing 'result' component must be a 4xx, got {resp.status_code}: {resp.text[:300]}"
    )


def test_create_observations_missing_datastream(client, seed):
    """req/data-array/data-array -- a CreateObservations set without a Datastream
    @iot.id is rejected with a client error (istSOS4: 400)."""
    payload = [
        {
            "components": ["phenomenonTime", "result"],
            "dataArray": [["2099-07-02T00:00:00Z", "1.0"]],
        }
    ]
    resp = client.post("CreateObservations", json=payload)
    assert 400 <= resp.status_code < 500, (
        f"missing Datastream id must be a 4xx, got {resp.status_code}: {resp.text[:300]}"
    )
