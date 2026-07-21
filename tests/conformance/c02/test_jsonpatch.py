"""
test_jsonpatch.py -- JSON Patch (RFC 6902) conformance tests for c02.

Standard:   OGC 18-088 §10.3 / RFC 6902
Requirement: req/create-update-delete/update-entity-jsonpatch
             (ADVERTISED in istSOS4 serverSettings.conformance array)

FROST v2.7.2 reference (JsonPatchTests.java @ /home/ist/workspace/GIT/FROST-Server):
  jsonPatchThingTest      — add /properties={key1:1}; then copy key1→keyCopy1 +
                            move key1→key2; verify keyCopy1==1, key1 absent, key2==1
  jsonPatchThingNoOpTest  — add /properties={key1:2}; then replace /properties/key1=2
                            (same value, "no-op"); verify key1 still 2
  jsonPatchDatastreamTest — same add→copy+move sequence on a Datastream entity

Ops exercised by FROST: add, replace, copy, move   (on /properties subtree)
Ops NOT in FROST: remove, test  (bonus `remove` test added; xfailed when server
doesn't support any JSON-Patch at all — see [B9])

Protocol: PATCH with  Content-Type: application/json-patch+json  and a JSON array
body where each element is an RFC-6902 operation object.

CANDIDATE API-FIXER TICKET [B9]:
  istSOS4 ADVERTISES req/create-update-delete/update-entity-jsonpatch in its
  serverSettings.conformance array but returns 422 for ALL JSON-Patch requests.
  Root cause: FastAPI PATCH handlers declare `payload: dict = Body(...)` — they
  reject a JSON array body before any routing logic runs.
  Reproduction:
    PATCH /Things(<id>)
    Content-Type: application/json-patch+json
    [{"op":"add","path":"/properties","value":{"key1":1}}]
  Expected: 200/204 with entity updated per RFC 6902
  Actual:   422 {"detail":[{"type":"dict_type","loc":["body"],
                 "msg":"Input should be a valid dictionary","input":[...]}]}
  Req citation: req/create-update-delete/update-entity-jsonpatch
"""

from __future__ import annotations

import json

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link

pytestmark = pytest.mark.c02


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_JSON_PATCH_CT = "application/json-patch+json"


def json_patch(client, url: str, ops: list):
    """Send a JSON-Patch PATCH request; return the raw httpx.Response."""
    return client.patch(
        url,
        content=json.dumps(ops).encode(),
        headers={"content-type": _JSON_PATCH_CT},
    )


# ---------------------------------------------------------------------------
# Shared setup helpers
# ---------------------------------------------------------------------------

def create_thing_with_props(client, unique_name, cleanup):
    """Create a fresh Thing with an empty properties bag.  Return (url, id)."""
    tag = unique_name("jp-thing")
    resp = client.create(
        "Things",
        {
            "name": f"{tag} Thing",
            "description": "json-patch test thing",
            "properties": {},
        },
    )
    assert resp.status_code == 201, (
        f"Setup Thing failed: {resp.status_code} {resp.text[:300]}"
    )
    url = client.location_of(resp)
    t_id = id_from_self_link(url)
    cleanup(url)
    return url, t_id


def create_datastream_tree(client, unique_name, cleanup):
    """Create Thing+Sensor+ObservedProperty+Datastream.
    Return (thing_url, ds_url, ds_id)."""
    tag = unique_name("jp-ds")

    # Thing
    t_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert t_resp.status_code == 201
    thing_url = client.location_of(t_resp)
    t_id = id_from_self_link(thing_url)
    cleanup(thing_url)

    # Sensor
    s_resp = client.create("Sensors", sample_data.minimal_sensor(tag))
    assert s_resp.status_code == 201
    s_url = client.location_of(s_resp)
    s_id = id_from_self_link(s_url)
    cleanup(s_url)

    # ObservedProperty
    op_resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
    assert op_resp.status_code == 201
    op_url = client.location_of(op_resp)
    op_id = id_from_self_link(op_url)
    cleanup(op_url)

    # Datastream
    ds_resp = client.create(
        "Datastreams",
        sample_data.minimal_datastream(tag, t_id, s_id, op_id),
    )
    assert ds_resp.status_code == 201
    ds_url = client.location_of(ds_resp)
    ds_id = id_from_self_link(ds_url)
    # DS is cascade-deleted when Thing is deleted; track explicitly as fallback
    cleanup(ds_url)

    return thing_url, ds_url, ds_id


# ===========================================================================
# Conformance advertisement check (must pass independently of implementation)
# ===========================================================================

@pytest.mark.c02
def test_jsonpatch_advertised_in_conformance(client):
    """req/create-update-delete/update-entity-jsonpatch URI must appear in
    serverSettings.conformance (18-088 §8.8).

    This check is independent of whether the server correctly implements JSON-Patch.
    istSOS4 does advertise the URI → PASSES.
    """
    root = client.nav("")
    conformance = root.get("serverSettings", {}).get("conformance", [])
    target = (
        "http://www.opengis.net/spec/iot_sensing/1.1/req/"
        "create-update-delete/update-entity-jsonpatch"
    )
    assert target in conformance, (
        f"req/create-update-delete/update-entity-jsonpatch must appear in "
        f"serverSettings.conformance. Found: {conformance}"
    )


# ===========================================================================
# jsonPatchThingTest  (mirrors FROST JsonPatchTests.jsonPatchThingTest)
#
# Step 1: add /properties = {"key1": 1}
#         → GET and verify properties.key1 == 1
#
# Step 2: [copy /properties/key1 → /properties/keyCopy1,
#          move /properties/key1 → /properties/key2]
#         → GET and verify keyCopy1==1, key1 absent/null, key2==1
# ===========================================================================

@pytest.mark.c02
def test_jsonpatch_thing_add(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.1 (add).

    Mirrors FROST JsonPatchTests.jsonPatchThingTest step 1:
      add /properties = {"key1": 1}
    GET must show properties.key1 == 1.

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    url, _ = create_thing_with_props(client, unique_name, cleanup)

    ops = [{"op": "add", "path": "/properties", "value": {"key1": 1}}]
    resp = json_patch(client, url, ops)

    assert resp.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch 'add' on /properties must return 200/204; "
        f"got {resp.status_code}: {resp.text[:300]}"
    )

    after = client.nav(url)
    props = after.get("properties") or {}
    assert props.get("key1") == 1, (
        f"JSON-Patch 'add' must set properties.key1=1; got properties={props!r}"
    )


@pytest.mark.c02
def test_jsonpatch_thing_copy_then_move(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.4/§4.5 (copy+move).

    Mirrors FROST JsonPatchTests.jsonPatchThingTest step 2:
      copy /properties/key1 → /properties/keyCopy1
      move /properties/key1 → /properties/key2
    Pre-condition: properties.key1 must already exist (set via regular PATCH first).
    GET after the JSON-Patch must show keyCopy1==1, key1 absent, key2==1.

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    url, _ = create_thing_with_props(client, unique_name, cleanup)

    # Pre-condition: set properties.key1=1 via regular merge-patch PATCH
    # (content-type application/json — works today) so the JSON-Patch ops have
    # a source value to operate on if they're ever dispatched.
    pre = client.patch(url, json={"properties": {"key1": 1}})
    assert pre.status_code in (200, 204), (
        f"Pre-condition PATCH to set key1 failed: {pre.status_code} {pre.text[:200]}"
    )

    # Now apply the JSON-Patch (advertised, must work per spec)
    ops = [
        {"op": "copy", "from": "/properties/key1", "path": "/properties/keyCopy1"},
        {"op": "move", "from": "/properties/key1", "path": "/properties/key2"},
    ]
    resp = json_patch(client, url, ops)

    assert resp.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch copy+move on /properties must return 200/204; "
        f"got {resp.status_code}: {resp.text[:300]}"
    )

    after = client.nav(url)
    props = after.get("properties") or {}
    assert props.get("keyCopy1") == 1, (
        f"JSON-Patch 'copy' must create properties.keyCopy1=1; got {props!r}"
    )
    assert props.get("key1") is None, (
        f"JSON-Patch 'move' must remove properties.key1; got {props!r}"
    )
    assert props.get("key2") == 1, (
        f"JSON-Patch 'move' must create properties.key2=1; got {props!r}"
    )


# ===========================================================================
# jsonPatchThingNoOpTest  (mirrors FROST JsonPatchTests.jsonPatchThingNoOpTest)
#
# Step 1: add /properties = {"key1": 2}  → verify key1==2
# Step 2: replace /properties/key1 = 2   ("no-op": same value) → verify key1 still 2
# ===========================================================================

@pytest.mark.c02
def test_jsonpatch_thing_replace_same_value(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.3 (replace).

    Mirrors FROST JsonPatchTests.jsonPatchThingNoOpTest:
      add /properties = {"key1": 2}
      replace /properties/key1 = 2  (same value → "no-op")
    GET must show key1 == 2 (unchanged).

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    url, _ = create_thing_with_props(client, unique_name, cleanup)

    # Step 1: add whole /properties
    ops1 = [{"op": "add", "path": "/properties", "value": {"key1": 2}}]
    r1 = json_patch(client, url, ops1)
    assert r1.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch 'add' /properties must return 200/204; "
        f"got {r1.status_code}: {r1.text[:300]}"
    )

    after1 = client.nav(url)
    assert (after1.get("properties") or {}).get("key1") == 2, (
        "After 'add', properties.key1 must be 2"
    )

    # Step 2: replace with same value ("no-op")
    ops2 = [{"op": "replace", "path": "/properties/key1", "value": 2}]
    r2 = json_patch(client, url, ops2)
    assert r2.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch 'replace' /properties/key1=2 must return 200/204; "
        f"got {r2.status_code}: {r2.text[:300]}"
    )

    after2 = client.nav(url)
    props = after2.get("properties") or {}
    assert props.get("key1") == 2, (
        f"After no-op 'replace', properties.key1 must still be 2; got {props!r}"
    )


# ===========================================================================
# jsonPatchDatastreamTest  (mirrors FROST JsonPatchTests.jsonPatchDatastreamTest)
#
# Step 1: add /properties = {"key1": 1}      → verify key1==1
# Step 2: copy key1→keyCopy1 + move key1→key2 → verify keyCopy1==1, key1 absent, key2==1
# ===========================================================================

@pytest.mark.c02
def test_jsonpatch_datastream_add(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.1 (add on Datastream).

    Mirrors FROST JsonPatchTests.jsonPatchDatastreamTest step 1:
      add /properties = {"key1": 1}
    GET must show properties.key1 == 1.

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    _, ds_url, _ = create_datastream_tree(client, unique_name, cleanup)

    ops = [{"op": "add", "path": "/properties", "value": {"key1": 1}}]
    resp = json_patch(client, ds_url, ops)

    assert resp.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch 'add' on Datastream /properties must return 200/204; "
        f"got {resp.status_code}: {resp.text[:300]}"
    )

    after = client.nav(ds_url)
    props = after.get("properties") or {}
    assert props.get("key1") == 1, (
        f"JSON-Patch 'add' must set Datastream.properties.key1=1; got {props!r}"
    )


@pytest.mark.c02
def test_jsonpatch_datastream_copy_then_move(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.4/§4.5
    (copy+move on Datastream).

    Mirrors FROST JsonPatchTests.jsonPatchDatastreamTest step 2:
      copy /properties/key1 → /properties/keyCopy1
      move /properties/key1 → /properties/key2
    Pre-condition: properties.key1=1 set via regular merge-patch PATCH.
    GET after JSON-Patch must show keyCopy1==1, key1 absent, key2==1.

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    _, ds_url, _ = create_datastream_tree(client, unique_name, cleanup)

    # Pre-condition: set properties.key1=1 via regular merge-patch
    pre = client.patch(ds_url, json={"properties": {"key1": 1}})
    assert pre.status_code in (200, 204), (
        f"Pre-condition PATCH to set DS key1 failed: {pre.status_code} {pre.text[:200]}"
    )

    ops = [
        {"op": "copy", "from": "/properties/key1", "path": "/properties/keyCopy1"},
        {"op": "move", "from": "/properties/key1", "path": "/properties/key2"},
    ]
    resp = json_patch(client, ds_url, ops)

    assert resp.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch copy+move on Datastream /properties must return 200/204; "
        f"got {resp.status_code}: {resp.text[:300]}"
    )

    after = client.nav(ds_url)
    props = after.get("properties") or {}
    assert props.get("keyCopy1") == 1, (
        f"JSON-Patch 'copy' must create Datastream.properties.keyCopy1=1; got {props!r}"
    )
    assert props.get("key1") is None, (
        f"JSON-Patch 'move' must remove Datastream.properties.key1; got {props!r}"
    )
    assert props.get("key2") == 1, (
        f"JSON-Patch 'move' must create Datastream.properties.key2=1; got {props!r}"
    )


# ===========================================================================
# RFC 6902 §4.2 'remove' op
#
# Not in FROST's explicit test set, but RFC 6902 defines 6 ops and the
# advertised conformance class (update-entity-jsonpatch) references RFC 6902 —
# all 6 ops are therefore required.  Asserting the spec; will pass once [B9]
# is fixed by api-fixer.
# ===========================================================================

@pytest.mark.c02
def test_jsonpatch_thing_remove(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.2 (remove).

    Pre-condition: properties.key0='zero' and properties.key1=1 set via merge-patch.
    JSON-Patch 'remove' must delete key0; key1 must be unaffected.

    RFC 6902 defines all 6 ops; the advertised class references RFC 6902.
    This is therefore a real spec requirement, not a bonus.

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    url, _ = create_thing_with_props(client, unique_name, cleanup)

    # Pre-condition
    pre = client.patch(url, json={"properties": {"key0": "zero", "key1": 1}})
    assert pre.status_code in (200, 204), (
        f"Pre-condition PATCH failed: {pre.status_code} {pre.text[:200]}"
    )

    ops = [{"op": "remove", "path": "/properties/key0"}]
    resp = json_patch(client, url, ops)

    assert resp.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch 'remove' on /properties/key0 must return 200/204; "
        f"got {resp.status_code}: {resp.text[:300]}"
    )

    after = client.nav(url)
    props = after.get("properties") or {}
    assert "key0" not in props, (
        f"'remove' must delete properties.key0; got {props!r}"
    )
    assert props.get("key1") == 1, (
        f"'remove' must leave properties.key1 intact; got {props!r}"
    )


# ===========================================================================
# RFC 6902 §4.6 'test' op
#
# Not in FROST's explicit test set, but all 6 RFC 6902 ops are required when
# the server advertises update-entity-jsonpatch.  Asserting the spec; will
# pass once [B9] is fixed by api-fixer.
# ===========================================================================

@pytest.mark.c02
def test_jsonpatch_thing_test_op(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-jsonpatch + RFC 6902 §4.6 (test).

    The 'test' operation asserts that a target location has a specified value;
    if the assertion fails the entire patch document MUST fail (RFC 6902 §4.6).
    A passing 'test' op (value matches) must return 200/204 and leave the
    entity unchanged.

    Pre-condition: properties.key1=42 set via merge-patch.
    JSON-Patch: [{"op":"test","path":"/properties/key1","value":42}]
    Expected: 200/204 (test passes; entity state unchanged).
    GET after: properties.key1 still 42.

    RFC 6902 defines all 6 ops; the advertised class references RFC 6902.
    This is therefore a real spec requirement.

    FAILS against istSOS4 [B9] — advertised but not implemented (422).
    Route to api-fixer.
    """
    url, _ = create_thing_with_props(client, unique_name, cleanup)

    # Pre-condition: set a known value
    pre = client.patch(url, json={"properties": {"key1": 42}})
    assert pre.status_code in (200, 204), (
        f"Pre-condition PATCH failed: {pre.status_code} {pre.text[:200]}"
    )

    # 'test' op: value matches → patch succeeds, entity unchanged
    ops = [{"op": "test", "path": "/properties/key1", "value": 42}]
    resp = json_patch(client, url, ops)

    assert resp.status_code in (200, 204), (
        f"SPEC VIOLATION (req/create-update-delete/update-entity-jsonpatch): "
        f"JSON-Patch 'test' with matching value must return 200/204; "
        f"got {resp.status_code}: {resp.text[:300]}"
    )

    after = client.nav(url)
    props = after.get("properties") or {}
    assert props.get("key1") == 42, (
        f"After a passing 'test' op, properties.key1 must remain 42; got {props!r}"
    )
