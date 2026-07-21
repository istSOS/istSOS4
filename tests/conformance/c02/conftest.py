"""
c02/conftest.py -- shared fixtures and helper functions for the c02 test suite.

Owned by the c02 agent; consumed by all c02 sub-modules.
Root conftest fixtures (client, seed, unique_name, base_url) auto-inherit.

Public API:
  cleanup   (fixture)  -- track URLs; delete in reverse order on teardown.
  create_datastream_tree (function) -- build a minimal Thing+Location+Sensor+
                                         ObservedProperty+Datastream tree and
                                         register it for cleanup.  Used by tests
                                         that need a ready Datastream/Observation.
"""

from __future__ import annotations

import pytest

import sample_data
from client import entity_id, format_id, id_from_self_link


# ---------------------------------------------------------------------------
# Cleanup fixture – tracks absolute URL strings; deletes in reverse order on
# teardown so dependents are removed before parents.  Tolerates 404 because
# cascade deletes may have already removed some entities.
# ---------------------------------------------------------------------------

@pytest.fixture
def cleanup(client):
    """Collect self-link URLs; delete all on teardown (tolerate 404/any error)."""
    links: list[str] = []

    def track(*urls: str) -> None:
        """Register one or more absolute self-link URLs for cleanup."""
        links.extend(u for u in urls if u)

    yield track

    for url in reversed(links):
        try:
            client.delete(url)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Internal setup helper – builds a minimal Thing+Location+Sensor+ObservedProperty
# +Datastream tree for tests that need a ready Datastream.
# ---------------------------------------------------------------------------

def create_datastream_tree(client, unique_name, cleanup):
    """Create the minimal subtree required to post Observations.

    Returns a dict with keys: thing_id, location_id, sensor_id, op_id, ds_id,
    thing_url, location_url, sensor_url, op_url, ds_url.
    """
    tag = unique_name("cud")

    # Thing with inline Location (ensures HistoricalLocation + Location linkage).
    thing_payload = {
        **sample_data.minimal_thing(tag),
        "Locations": [sample_data.minimal_location(tag)],
    }
    t_resp = client.create("Things", thing_payload)
    assert t_resp.status_code == 201, (
        f"setup Thing failed: {t_resp.status_code} {t_resp.text[:300]}"
    )
    thing_url = client.location_of(t_resp)
    thing_data = client.nav(thing_url, params={"$expand": "Locations"})
    t_id = entity_id(thing_data)
    loc_id = entity_id(thing_data["Locations"][0])
    loc_url = f"{client.base_url}/Locations({format_id(loc_id)})"

    # Sensor
    s_resp = client.create("Sensors", sample_data.minimal_sensor(tag))
    assert s_resp.status_code == 201, (
        f"setup Sensor failed: {s_resp.status_code} {s_resp.text[:300]}"
    )
    sensor_url = client.location_of(s_resp)
    s_id = id_from_self_link(sensor_url)

    # ObservedProperty
    op_resp = client.create("ObservedProperties", sample_data.minimal_observed_property(tag))
    assert op_resp.status_code == 201, (
        f"setup ObservedProperty failed: {op_resp.status_code} {op_resp.text[:300]}"
    )
    op_url = client.location_of(op_resp)
    op_id = id_from_self_link(op_url)

    # Datastream
    ds_resp = client.create(
        "Datastreams",
        sample_data.minimal_datastream(tag, t_id, s_id, op_id),
    )
    assert ds_resp.status_code == 201, (
        f"setup Datastream failed: {ds_resp.status_code} {ds_resp.text[:300]}"
    )
    ds_url = client.location_of(ds_resp)
    ds_id = id_from_self_link(ds_url)

    # Register for cleanup.
    # Deletion order (reversed list, so we add in reverse-of-desired order):
    #   desired: sensor, op, loc, thing (thing cascades DS+HistLocs)
    #   added:   thing, loc, op, sensor  →  reversed: sensor, op, loc, thing ✓
    cleanup(thing_url, loc_url, op_url, sensor_url)

    return {
        "thing_id": t_id,
        "location_id": loc_id,
        "sensor_id": s_id,
        "op_id": op_id,
        "ds_id": ds_id,
        "thing_url": thing_url,
        "location_url": loc_url,
        "sensor_url": sensor_url,
        "op_url": op_url,
        "ds_url": ds_url,
    }
