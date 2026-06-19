"""
test_validation.py -- OGC SensorThings API v1.1 c02 validation-error tests.

Standard:  OGC 18-088 §10.2  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage:
  CREATE 7  – Validation errors (missing mandatory property, bad link,
               malformed JSON, unknown property)
"""

from __future__ import annotations

import pytest

import sample_data
from client import id_from_self_link

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
