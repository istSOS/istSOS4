"""
test_update_put.py -- OGC SensorThings API v1.1 c02 PUT (full-replace) tests.

Standard:  OGC 18-088 §10.3  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/update-entity-put

PUT is DECLARED in serverSettings.conformance and IMPLEMENTED by istSOS4.
All tests in this module are positive (no xfail).

Coverage:
  UPDATE 11 – PUT full replacement: success case, mandatory-property validation,
               optional-property reset semantics.
"""

from __future__ import annotations

import pytest

import sample_data

pytestmark = pytest.mark.c02


# ===========================================================================
# UPDATE 11 – PUT full replacement
#   req/create-update-delete/update-entity-put  (18-088 §10.3)
# ===========================================================================

@pytest.mark.c02
def test_put_replace_thing(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT replaces an entity (full replace).

    18-088 §10.3: PUT shall perform a COMPLETE replacement of the entity with the
    provided body.  The server must return 200 or 204.  The entity's @iot.id and
    @iot.selfLink must be immutable (preserved across the replace).
    Optional properties omitted from the PUT body must be reset to null/absent
    (full replace, NOT merge — contrast with PATCH).

    Verified behaviours:
      - PUT /Things(<id>) with new name+description but WITHOUT 'properties'
      - Returns 200 or 204
      - GET after → name/description updated, properties null/absent (reset)
      - @iot.id and @iot.selfLink are unchanged (id immutable)
    """
    tag = unique_name("put")
    original_payload = {
        "name": f"{tag} original",
        "description": "original-description",
        "properties": {"key": "val", "extra": 123},
    }
    resp = client.create("Things", original_payload)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Record id and selfLink before PUT
    before = client.nav(url)
    original_id = before["@iot.id"]
    original_self_link = before["@iot.selfLink"]

    # PUT: full replacement WITHOUT 'properties' field
    put_resp = client.put(
        url,
        json={"name": f"{tag} replaced", "description": "replaced-description"},
    )
    assert put_resp.status_code in (200, 204), (
        f"req/create-update-delete/update-entity-put: PUT should return 200 or 204; "
        f"got {put_resp.status_code}: {put_resp.text[:300]}"
    )

    after = client.nav(url)

    # Name and description must be updated
    assert after["name"] == f"{tag} replaced", (
        "PUT must update the 'name' property"
    )
    assert after["description"] == "replaced-description", (
        "PUT must update the 'description' property"
    )

    # Optional 'properties' not in PUT body → must be reset to null/absent (full replace)
    props = after.get("properties")
    assert props is None or props == {}, (
        "req/create-update-delete/update-entity-put: PUT full-replace must reset "
        f"'properties' to null/absent when not supplied; got {props!r}"
    )

    # @iot.id must be immutable across PUT
    assert after["@iot.id"] == original_id, (
        f"@iot.id must not change after PUT: was {original_id!r}, now {after['@iot.id']!r}"
    )
    # @iot.selfLink must be preserved (same absolute URL)
    assert after["@iot.selfLink"] == original_self_link, (
        f"@iot.selfLink must be preserved after PUT: "
        f"was {original_self_link!r}, now {after['@iot.selfLink']!r}"
    )


@pytest.mark.c02
def test_put_missing_mandatory_property_thing(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT Thing without mandatory 'name' → 400.

    18-088 §10.3: A PUT body missing a mandatory property must be rejected.
    The mandatory properties for Thing are: name, description.
    The server must return 400 (not 500, not 200).
    """
    tag = unique_name("put-mand-thing")
    resp = client.create(
        "Things",
        {"name": f"{tag} Thing", "description": "to-be-replaced"},
    )
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # PUT WITHOUT mandatory 'name' → must be rejected
    bad_resp = client.put(url, json={"description": "no-name-put"})
    assert bad_resp.status_code == 400, (
        f"req/create-update-delete/update-entity-put: PUT Thing without 'name' must "
        f"return 400; got {bad_resp.status_code}: {bad_resp.text[:300]}"
    )
    # The error body must not be a server error (5xx content is unacceptable)
    assert bad_resp.status_code < 500, (
        f"Server must return a structured 4xx client error, not 5xx; got {bad_resp.status_code}"
    )


@pytest.mark.c02
def test_put_missing_mandatory_property_sensor(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT Sensor without mandatory 'metadata' → 400.

    18-088 §10.3 + §8.2.5: The mandatory properties for Sensor are:
    name, description, encodingType, metadata.
    PUT without 'metadata' must be rejected with 400.
    """
    tag = unique_name("put-mand-sensor")
    resp = client.create(
        "Sensors",
        sample_data.minimal_sensor(tag),
    )
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # PUT WITHOUT mandatory 'metadata' → must be rejected
    bad_resp = client.put(
        url,
        json={
            "name": f"{tag} Sensor replaced",
            "description": "no-metadata",
            "encodingType": sample_data.SENSOR_PDF,
            # 'metadata' intentionally omitted
        },
    )
    assert bad_resp.status_code == 400, (
        f"req/create-update-delete/update-entity-put: PUT Sensor without 'metadata' must "
        f"return 400; got {bad_resp.status_code}: {bad_resp.text[:300]}"
    )


@pytest.mark.c02
def test_put_optional_property_reset(client, unique_name, cleanup):
    """req/create-update-delete/update-entity-put — PUT without optional property resets it.

    18-088 §10.3: PUT is COMPLETE replacement.  An optional property omitted from
    the PUT body must NOT be retained (unlike PATCH, which merges).
    This test specifically verifies that Thing.properties is reset to null/absent
    when a PUT body includes name+description but omits 'properties'.
    """
    tag = unique_name("put-opt-reset")
    resp = client.create(
        "Things",
        {
            "name": f"{tag} Thing",
            "description": "with-properties",
            "properties": {"foo": "bar", "count": 42},
        },
    )
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Confirm 'properties' is set before PUT
    before = client.nav(url)
    assert before.get("properties") not in (None, {}), (
        "Pre-condition: 'properties' must be set before PUT"
    )

    # PUT with only mandatory fields (no 'properties')
    put_resp = client.put(
        url,
        json={"name": f"{tag} Thing", "description": "without-properties"},
    )
    assert put_resp.status_code in (200, 204), (
        f"PUT must succeed; got {put_resp.status_code}: {put_resp.text[:300]}"
    )

    after = client.nav(url)
    props = after.get("properties")
    assert props is None or props == {}, (
        "req/create-update-delete/update-entity-put: PUT without 'properties' must reset "
        f"it to null/absent (full replace semantics); got {props!r}"
    )
