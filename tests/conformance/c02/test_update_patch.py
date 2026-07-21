"""
test_update_patch.py -- OGC SensorThings API v1.1 c02 PATCH (merge-update) tests.

Standard:  OGC 18-088 §10.3  (https://docs.ogc.org/is/18-088/18-088.html)
Req namespace: req/create-update-delete/

Coverage:
  UPDATE 8  – PATCH scalar property on each entity type (200/204; GET confirms change,
               others untouched)
  UPDATE 9  – PATCH relation (re-link a Datastream to a different Sensor)
  UPDATE 10 – PATCH non-existent entity → 404
"""

from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link
from c02.conftest import create_datastream_tree

pytestmark = pytest.mark.c02


# ===========================================================================
# UPDATE 8 – PATCH scalar property on each entity type
#   req/create-update-delete/update-entity
# ===========================================================================

@pytest.mark.c02
def test_patch_thing(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Thing scalar property.

    PATCH must update only the specified properties and leave others unchanged.
    """
    tag = unique_name("patch-thing")
    original = sample_data.minimal_thing(tag)
    resp = client.create("Things", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-description"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-description", "PATCH must update description"
    assert after["name"] == original["name"], "PATCH must not change unspecified properties"


@pytest.mark.c02
def test_patch_location(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Location scalar property."""
    tag = unique_name("patch-loc")
    original = sample_data.minimal_location(tag)
    resp = client.create("Locations", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-location"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-location"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_sensor(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Sensor scalar property."""
    tag = unique_name("patch-sensor")
    original = sample_data.minimal_sensor(tag)
    resp = client.create("Sensors", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-sensor"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-sensor"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_observed_property(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH ObservedProperty scalar property."""
    tag = unique_name("patch-op")
    original = sample_data.minimal_observed_property(tag)
    resp = client.create("ObservedProperties", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    patch_resp = client.patch(url, json={"description": "patched-op"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-op"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_datastream(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Datastream scalar property."""
    tag = unique_name("patch-ds")
    tree = create_datastream_tree(client, unique_name, cleanup)
    ds_url = tree["ds_url"]
    original_ds = client.nav(ds_url)

    patch_resp = client.patch(ds_url, json={"description": "patched-datastream"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(ds_url)
    assert after["description"] == "patched-datastream"
    assert after["name"] == original_ds["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_observation(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Observation scalar property (result)."""
    tag = unique_name("patch-obs")
    tree = create_datastream_tree(client, unique_name, cleanup)
    ds_id = tree["ds_id"]

    obs_payload = sample_data.minimal_observation(tag, ds_id, result=10.0)
    obs_resp = client.create("Observations", obs_payload)
    assert obs_resp.status_code == 201
    obs_url = client.location_of(obs_resp)
    obs_id = id_from_self_link(obs_url)
    cleanup(obs_url)

    # Track auto-generated FoI
    foi_resp = client.get(
        f"{client.base_url}/Observations({format_id(obs_id)})/FeatureOfInterest"
    )
    if foi_resp.status_code == 200:
        cleanup(
            f"{client.base_url}/FeaturesOfInterest({format_id(entity_id(foi_resp.json()))})"
        )

    patch_resp = client.patch(obs_url, json={"result": 99.9})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(obs_url)
    assert after["result"] == 99.9, "result must be updated after PATCH"
    assert after["phenomenonTime"] == obs_payload["phenomenonTime"], (
        "phenomenonTime must not change after PATCH"
    )


@pytest.mark.c02
def test_patch_feature_of_interest(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH FeatureOfInterest.

    Note: istSOS4 requires the 'feature' property even in partial PATCH bodies [B5].
    This test includes 'feature' to make the basic PATCH work. A separate test
    (test_patch_foi_partial_update) verifies the spec-required partial-update
    behaviour and is expected to fail (candidate spec violation).
    """
    tag = unique_name("patch-foi")
    original = sample_data.minimal_feature_of_interest(tag)
    resp = client.create("FeaturesOfInterest", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Include 'feature' to satisfy the server's non-standard requirement
    patch_resp = client.patch(
        url,
        json={
            "description": "patched-foi",
            "feature": original["feature"],
        },
    )
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(url)
    assert after["description"] == "patched-foi"
    assert after["name"] == original["name"], "name must be unchanged"


@pytest.mark.c02
def test_patch_foi_partial_update(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — FoI PATCH with partial body must succeed.

    18-088 §10.3: "only values of those properties specified in the PATCH request
    body are updated".  Patching just 'description' must not require 'feature'.
    Fixed by api-fixer [B5]; now passes.
    """
    tag = unique_name("foi-partial")
    original = sample_data.minimal_feature_of_interest(tag)
    resp = client.create("FeaturesOfInterest", original)
    assert resp.status_code == 201
    url = client.location_of(resp)
    cleanup(url)

    # Partial PATCH — only description, no feature
    patch_resp = client.patch(url, json={"description": "partial-patch-foi"})
    assert patch_resp.status_code in (200, 204), (
        f"Partial PATCH of FoI description should return 200/204. "
        f"Got {patch_resp.status_code}: {patch_resp.text[:300]}"
    )

    after = client.nav(url)
    assert after["description"] == "partial-patch-foi", "description must be updated"
    assert after["name"] == original["name"], "name must be unchanged by partial PATCH"


@pytest.mark.c02
def test_patch_historical_location(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH HistoricalLocation scalar property."""
    tag = unique_name("patch-hl")

    thing_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert thing_resp.status_code == 201
    thing_url = client.location_of(thing_resp)
    t_id = id_from_self_link(thing_url)
    cleanup(thing_url)

    hl_resp = client.create(
        "HistoricalLocations",
        sample_data.minimal_historical_location(tag, t_id, time="2022-01-01T00:00:00Z"),
    )
    assert hl_resp.status_code == 201
    hl_url = client.location_of(hl_resp)
    cleanup(hl_url)

    patch_resp = client.patch(hl_url, json={"time": "2022-06-01T00:00:00Z"})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH should return 200 or 204, got {patch_resp.status_code}"
    )

    after = client.nav(hl_url)
    assert "2022-06-01" in after["time"], "time must be updated after PATCH"


# ===========================================================================
# UPDATE 9 – PATCH relation (re-link)
#   req/create-update-delete/update-entity
# ===========================================================================

@pytest.mark.c02
def test_patch_relation_relink_sensor(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Datastream to re-link its Sensor.

    PATCH {"Sensor": {"@iot.id": <new_sensor_id>}} must update the Datastream's
    Sensor relation.
    """
    tag = unique_name("relink")
    tree = create_datastream_tree(client, unique_name, cleanup)
    ds_url = tree["ds_url"]
    original_sensor_id = tree["sensor_id"]

    # Create a second Sensor to re-link to
    tag2 = unique_name("sensor-b")
    s2_resp = client.create("Sensors", sample_data.minimal_sensor(tag2))
    assert s2_resp.status_code == 201
    s2_url = client.location_of(s2_resp)
    s2_id = id_from_self_link(s2_url)
    cleanup(s2_url)

    # Verify original Sensor is linked
    ds_before = client.nav(ds_url, params={"$expand": "Sensor"})
    assert entity_id(ds_before["Sensor"]) == original_sensor_id

    # Re-link to Sensor 2
    patch_resp = client.patch(ds_url, json={"Sensor": {"@iot.id": s2_id}})
    assert patch_resp.status_code in (200, 204), (
        f"PATCH relation should return 200 or 204, got {patch_resp.status_code}: {patch_resp.text[:300]}"
    )

    # Verify relation changed
    ds_after = client.nav(ds_url, params={"$expand": "Sensor"})
    assert entity_id(ds_after["Sensor"]) == s2_id, (
        "Datastream must now reference the new Sensor after PATCH re-link"
    )


# ===========================================================================
# UPDATE 10 – PATCH non-existent entity → 404
#   req/create-update-delete/update-entity
# ===========================================================================

@pytest.mark.c02
def test_patch_nonexistent_thing(client):
    """req/create-update-delete/update-entity — PATCH non-existent entity → 404."""
    resp = client.patch("Things(999999999)", json={"name": "ghost"})
    assert resp.status_code == 404, (
        f"PATCH on non-existent Thing must return 404, got {resp.status_code}"
    )


@pytest.mark.c02
def test_patch_nonexistent_observation(client):
    """req/create-update-delete/update-entity — PATCH non-existent Observation → 404."""
    resp = client.patch("Observations(999999999)", json={"result": 0})
    assert resp.status_code == 404, (
        f"PATCH on non-existent Observation must return 404, got {resp.status_code}"
    )


# ===========================================================================
# UPDATE 11 – PATCH with INLINE related entities is rejected with 400
#   req/create-update-delete/update-entity (18-088 §10.3)
# ===========================================================================
# A PATCH may only (re)link related entities by reference ({"@iot.id": <id>}).
# A full inline related object (no @iot.id) is an illegal update payload and
# must be rejected with a clean 400 — NOT crash with a 500, and NOT be silently
# accepted (200). Regression: these inline-related cases used to raise a bare
# Exception that the handler mapped to 500.

@pytest.mark.c02
def test_patch_thing_inline_location_returns_400(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Thing with an inline
    Location (no @iot.id) → 400, not 500."""
    tag = unique_name("patch-inline")
    t_resp = client.create("Things", {
        **sample_data.minimal_thing(tag),
        "Locations": [sample_data.minimal_location(tag)],
    })
    assert t_resp.status_code == 201, t_resp.text[:200]
    thing_url = client.location_of(t_resp)
    cleanup(thing_url)
    thing_id = id_from_self_link(thing_url)

    resp = client.patch(
        f"Things({format_id(thing_id)})",
        json={"Locations": [sample_data.minimal_location(unique_name("inline"))]},
    )
    assert resp.status_code == 400, (
        f"PATCH with inline Location must be 400, got {resp.status_code}: {resp.text[:200]}"
    )
    body = resp.json()
    assert body.get("code") == 400 and body.get("type") == "error", body


@pytest.mark.c02
def test_patch_thing_inline_datastream_returns_400(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Thing with an inline
    Datastream (no @iot.id) → 400, not 500."""
    tag = unique_name("patch-inline-ds")
    t_resp = client.create("Things", sample_data.minimal_thing(tag))
    assert t_resp.status_code == 201, t_resp.text[:200]
    thing_url = client.location_of(t_resp)
    cleanup(thing_url)
    thing_id = id_from_self_link(thing_url)

    resp = client.patch(
        f"Things({format_id(thing_id)})",
        json={"Datastreams": [{"unitOfMeasurement": sample_data.unit_lumen()}]},
    )
    assert resp.status_code == 400, (
        f"PATCH with inline Datastream must be 400, got {resp.status_code}: {resp.text[:200]}"
    )
    body = resp.json()
    assert body.get("code") == 400 and body.get("type") == "error", body


@pytest.mark.c02
def test_patch_datastream_inline_sensor_returns_400(client, unique_name, cleanup):
    """req/create-update-delete/update-entity — PATCH Datastream with an inline
    Sensor (no @iot.id) → 400, not 500."""
    tree = create_datastream_tree(client, unique_name, cleanup)
    resp = client.patch(
        f"Datastreams({format_id(tree['ds_id'])})",
        json={"Sensor": sample_data.minimal_sensor(unique_name("inline-sensor"))},
    )
    assert resp.status_code == 400, (
        f"PATCH with inline Sensor must be 400, got {resp.status_code}: {resp.text[:200]}"
    )
    body = resp.json()
    assert body.get("code") == 400 and body.get("type") == "error", body
