"""
test_validation.py -- OGC SensorThings API v1.1 c02 validation-error tests.

Standard:  OGC 18-088 §10.2  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage:
  CREATE 7  – Validation errors (missing mandatory property, bad link,
               malformed JSON, unknown property)
  REGRESSION – Bad-link → controlled 4xx (not 5xx, no Postgres leak)
               req/create-update-delete/create-entity (link-to-existing)
"""

from __future__ import annotations

import pytest

import sample_data
from client import id_from_self_link
from c02.conftest import _create_datastream_tree

pytestmark = pytest.mark.c02


# ===========================================================================
# CREATE 7 – Validation errors
# ===========================================================================

@pytest.mark.c02
def test_validation_missing_name_thing(client, unique_name):
    """req/create-update-delete/create-entity — missing mandatory property → 400.

    Posting a Thing without the mandatory 'name' property must be rejected
    with a 400 Bad Request.
    """
    resp = client.create("Things", {"description": "no-name-thing"})
    assert resp.status_code == 400, (
        f"Missing mandatory 'name' should return 400, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_missing_name_location(client, unique_name):
    """req/create-update-delete/create-entity — missing mandatory 'name' for Location → 400."""
    resp = client.create(
        "Locations",
        {
            "description": "no-name",
            "encodingType": sample_data.GEOJSON,
            "location": sample_data.SEED_POINT,
        },
    )
    assert resp.status_code == 400, (
        f"Missing 'name' in Location should return 400, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_missing_mandatory_datastream(client, unique_name, cleanup):
    """req/create-update-delete/create-entity — Datastream without mandatory Sensor → 4xx."""
    tag = unique_name("valmissing")

    # Create Thing + ObservedProperty for a partial Datastream
    t_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert t_resp.status_code == 201
    t_url = client.location_of(t_resp)
    t_id = id_from_self_link(t_url)
    cleanup(t_url)

    op_resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
    assert op_resp.status_code == 201
    op_url = client.location_of(op_resp)
    op_id = id_from_self_link(op_url)
    cleanup(op_url)

    # Datastream without Sensor (mandatory)
    resp = client.create(
        "Datastreams",
        {
            "name": f"{tag} DS-no-sensor",
            "description": "missing sensor",
            "unitOfMeasurement": sample_data.unit_lumen(),
            "observationType": sample_data.OM_MEASUREMENT,
            "Thing": {"@iot.id": t_id},
            "ObservedProperty": {"@iot.id": op_id},
        },
    )
    assert resp.status_code in (400, 422), (
        f"Datastream without Sensor should be rejected (4xx), got {resp.status_code}"
    )


@pytest.mark.c02
def test_validation_bad_iot_id_link(client, unique_name):
    """req/create-update-delete/create-entity — non-existent @iot.id link → 4xx.

    Referencing a non-existent entity via {"@iot.id": <id>} must be rejected.
    """
    resp = client.create(
        "Datastreams",
        {
            "name": "bad-link-ds",
            "description": "bad links",
            "unitOfMeasurement": sample_data.unit_lumen(),
            "observationType": sample_data.OM_MEASUREMENT,
            "Thing": {"@iot.id": 999999999},
            "Sensor": {"@iot.id": 999999998},
            "ObservedProperty": {"@iot.id": 999999997},
        },
    )
    assert resp.status_code in (400, 404, 409), (
        f"Non-existent @iot.id link should return 4xx, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_malformed_json(client, unique_name):
    """req/create-update-delete/create-entity — malformed JSON body → 4xx.

    Sending a syntactically invalid JSON body must result in a 4xx response.
    Note: istSOS4 (FastAPI) returns 422 for JSON parse errors [B7].
    """
    resp = client.post(
        "Things",
        content=b"{not valid json!!!}",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code in (400, 422), (
        f"Malformed JSON should return 4xx, got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_unknown_property(client, unique_name):
    """req/create-update-delete/create-entity — unknown/extra property → 4xx.

    Posting with an unknown property key must be rejected.
    18-088 §10.2.1 requires the server to reject unrecognised properties.
    """
    resp = client.create(
        "Things",
        {
            "name": "unknown-prop-thing",
            "description": "has an unknown property",
            "unknownExtraProperty": "should-be-rejected",
        },
    )
    assert resp.status_code in (400, 422), (
        f"Unknown property should be rejected (4xx), got {resp.status_code}: {resp.text[:300]}"
    )


@pytest.mark.c02
def test_validation_sensor_string_metadata(client, unique_name):
    """18-088 §8.2.5 — Sensor.metadata bare-string URL must be accepted.

    Per OGC 18-088 §8.2.5, for encodingType 'application/pdf' the metadata
    property is a URL (string).  The server must accept and store it as-is.

    SPEC VIOLATION: istSOS4 stores metadata as JSONB and rejects bare strings
    with 400 [B6].  This test will pass once api-fixer resolves the issue.
    req/create-update-delete/create-entity
    """
    tag = unique_name("strmeta")
    resp = client.create(
        "Sensors",
        {
            "name": f"{tag} Sensor-string-meta",
            "description": "sensor with string metadata",
            "encodingType": sample_data.SENSOR_PDF,
            "metadata": "https://example.org/sensor-spec.pdf",
        },
    )
    # Per 18-088 §8.2.5 this MUST succeed with 201.
    # istSOS4 returns 400 — routed to api-fixer.
    assert resp.status_code == 201, (
        f"SPEC VIOLATION (req/create-update-delete/create-entity, 18-088 §8.2.5): "
        f"Sensor with string metadata must return 201; got {resp.status_code}: {resp.text[:300]}"
    )
    if resp.status_code == 201:
        client.delete(client.location_of(resp))


# ===========================================================================
# REGRESSION – Bad-link → controlled 4xx, no Postgres internals in body
#
# req/create-update-delete/create-entity (link-to-existing / bad reference)
#
# After the FK-violation fix, istSOS4 must:
#   1. Return 4xx (not 5xx) for any POST/PATCH that references a non-existent
#      entity via {"@iot.id": <bogus>}.
#   2. Return a CONTROLLED structured error body {code, type, message} where
#      the message does NOT expose raw Postgres internals (no "constraint",
#      "DETAIL", "violates", "fkey", or "Key (").
#
# These tests lock in both guarantees across the main linking endpoints so
# that regressions in error handling surface immediately.
# ===========================================================================

# Each entry: (test_id, collection, payload)
# All payloads reference ids that cannot possibly exist (999999999-range).
_BAD_LINK_POST_CASES = [
    pytest.param(
        "Datastreams",
        {
            "name": "regr-bad-ds",
            "description": "bad links",
            "unitOfMeasurement": sample_data.unit_lumen(),
            "observationType": sample_data.OM_MEASUREMENT,
            "Thing": {"@iot.id": 999999999},
            "Sensor": {"@iot.id": 999999998},
            "ObservedProperty": {"@iot.id": 999999997},
        },
        id="datastream_bad_thing_sensor_op",
    ),
    pytest.param(
        "HistoricalLocations",
        {
            "time": "2024-01-01T00:00:00Z",
            "Thing": {"@iot.id": 999999999},
            "Locations": [{"@iot.id": 999999999}],
        },
        id="historicallocation_bad_thing_location",
    ),
    pytest.param(
        "Observations",
        {
            "phenomenonTime": "2024-01-01T00:00:00Z",
            "result": 1,
            "Datastream": {"@iot.id": 999999999},
            "FeatureOfInterest": {"@iot.id": 999999999},
        },
        id="observation_bad_datastream_foi",
    ),
    pytest.param(
        "Things",
        {
            "name": "regr-bad-thing",
            "description": "bad location link",
            "Locations": [{"@iot.id": 999999999}],
        },
        id="thing_bad_location_link",
    ),
]

# Postgres substrings that must NEVER appear in a controlled error response.
_POSTGRES_LEAK_MARKERS = ["constraint", "DETAIL", "violates", "fkey", "Key ("]


@pytest.mark.c02
@pytest.mark.parametrize("collection,payload", _BAD_LINK_POST_CASES)
def test_link_to_nonexistent_returns_4xx(client, collection, payload):
    """req/create-update-delete/create-entity — bad @iot.id link → controlled 4xx.

    POST to each linking endpoint with a non-existent referenced entity must:
      1. Return 4xx (never 5xx) — server-side FK violation must NOT become 500.
      2. Return a structured JSON error {code, type, message} — not a raw
         Postgres exception dump.
      3. The error message must NOT expose Postgres internals:
         'constraint', 'DETAIL', 'violates', 'fkey', 'Key ('.

    Covers: Datastreams, HistoricalLocations, Observations, Things
    with various bad @iot.id links.

    Regression lock: fixes the FK-violation → 400 (was 500) in istSOS4
    create endpoints. Any reversion will be caught immediately here.
    """
    resp = client.post(collection, json=payload)

    # Guard 1 — must be a CLIENT error, never a server crash.
    assert 400 <= resp.status_code < 500, (
        f"POST {collection} with bad @iot.id must return 4xx (not 5xx); "
        f"got {resp.status_code}: {resp.text[:400]}"
    )

    # Guard 2 — body must be structured JSON, not a raw exception.
    try:
        body = resp.json()
    except Exception as exc:
        pytest.fail(
            f"POST {collection} bad-link response body is not valid JSON: {exc}\n"
            f"raw: {resp.text[:400]}"
        )

    assert "code" in body and "type" in body and "message" in body, (
        f"Error response for POST {collection} must have {{code, type, message}}; "
        f"got: {body}"
    )

    # Guard 3 — no Postgres internals leaked in the message.
    message_lower = str(body.get("message", "")).lower()
    raw_lower = resp.text.lower()
    for marker in _POSTGRES_LEAK_MARKERS:
        assert marker.lower() not in message_lower, (
            f"Postgres internal string {marker!r} must not appear in the error "
            f"message for POST {collection} bad-link; message: {body['message']!r}"
        )
        assert marker.lower() not in raw_lower, (
            f"Postgres internal string {marker!r} must not appear anywhere in the "
            f"error response body for POST {collection} bad-link; "
            f"body excerpt: {resp.text[:400]}"
        )


@pytest.mark.c02
def test_patch_link_to_nonexistent_returns_4xx(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH bad @iot.id relink → controlled 4xx.

    PATCH a Datastream to relink its Sensor to a non-existent id.
    The server must return 4xx (not 5xx) and a structured error body
    with no Postgres internals.

    Regression lock: mirrors the POST bad-link fix applied to PATCH paths.
    """
    # Create a real Datastream to PATCH against (provides a valid target URL).
    tree = _create_datastream_tree(client, unique_name, cleanup)
    ds_url = tree["ds_url"]

    resp = client.patch(ds_url, json={"Sensor": {"@iot.id": 999999999}})

    # Guard 1 — 4xx, not 5xx.
    assert 400 <= resp.status_code < 500, (
        f"PATCH Datastream with bad Sensor @iot.id must return 4xx; "
        f"got {resp.status_code}: {resp.text[:400]}"
    )

    # Guard 2 — structured JSON body.
    try:
        body = resp.json()
    except Exception as exc:
        pytest.fail(
            f"PATCH Datastream bad-link response is not valid JSON: {exc}\n"
            f"raw: {resp.text[:400]}"
        )

    assert "code" in body and "type" in body and "message" in body, (
        f"Error response for PATCH Datastream bad Sensor link must have "
        f"{{code, type, message}}; got: {body}"
    )

    # Guard 3 — no Postgres internals.
    message_lower = str(body.get("message", "")).lower()
    raw_lower = resp.text.lower()
    for marker in _POSTGRES_LEAK_MARKERS:
        assert marker.lower() not in message_lower, (
            f"Postgres internal string {marker!r} must not appear in PATCH error "
            f"message; message: {body['message']!r}"
        )
        assert marker.lower() not in raw_lower, (
            f"Postgres internal string {marker!r} must not appear in PATCH error "
            f"response body; body: {resp.text[:400]}"
        )
