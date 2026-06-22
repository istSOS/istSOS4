"""
NETWORK extension -- read paths (proprietary, NOT OGC 18-088).

Mirrors what c01 does for Sensor/ObservedProperty, applied to Network:
collection GET, entity-by-id, control info, $select, $filter. Requires NETWORK=1.
All assertions scope to network_seed (by id or the unique tagged name).
"""

from __future__ import annotations

import pytest

from client import entity_id, format_id

pytestmark = pytest.mark.network


def test_networks_collection_returns_200(client, network_seed):
    """GET /Networks returns 200 with a value array."""
    r = client.get("Networks")
    assert r.status_code == 200, r.text
    assert isinstance(r.json().get("value"), list)


def test_network_entity_by_id(client, network_seed):
    """GET /Networks(id) returns the seeded network with its name."""
    doc = client.by_id("Networks", network_seed.net_a_id)
    assert entity_id(doc) == network_seed.net_a_id
    assert doc["name"] == network_seed.net_a_name


def test_network_control_info(client, network_seed):
    """A Network carries control info: @iot.id, absolute @iot.selfLink, and the
    Datastreams navigation link."""
    doc = client.by_id("Networks", network_seed.net_a_id)
    assert doc["@iot.id"] == network_seed.net_a_id
    assert doc["@iot.selfLink"].rstrip("/").endswith(f"Networks({format_id(network_seed.net_a_id)})")
    assert "Datastreams@iot.navigationLink" in doc


def test_network_selflink_resolves(client, network_seed):
    """Following a Network's @iot.selfLink returns the same entity."""
    doc = client.by_id("Networks", network_seed.net_a_id)
    again = client.follow_self_link(doc)
    assert entity_id(again) == network_seed.net_a_id


def test_network_select_name(client, network_seed):
    """$select=name on /Networks projects only the requested property."""
    doc = client.collection(
        "Networks",
        {"$filter": f"name eq '{network_seed.net_a_name}'", "$select": "name"},
    )
    assert doc["value"], "seed network not found"
    assert set(doc["value"][0].keys()) == {"name"}
    assert doc["value"][0]["name"] == network_seed.net_a_name


def test_networks_filter_by_name(client, network_seed):
    """$filter=name eq '<seed net>' returns exactly the one seeded network
    (name is uniquely tagged, so the result is deterministic on a shared DB)."""
    doc = client.collection("Networks", {"$filter": f"name eq '{network_seed.net_a_name}'"})
    ids = [entity_id(n) for n in doc["value"]]
    assert ids == [network_seed.net_a_id]


def test_network_mandatory_properties(client, network_seed):
    """The Network mandatory property `name` is present and non-empty."""
    doc = client.by_id("Networks", network_seed.net_a_id)
    assert isinstance(doc.get("name"), str) and doc["name"]
    assert doc["name"] == network_seed.net_a_name


def test_network_name_property(client, network_seed):
    """Property access: GET /Networks(id)/name -> {"name": <value>}."""
    doc = client.nav(f"Networks({format_id(network_seed.net_a_id)})/name")
    assert doc == {"name": network_seed.net_a_name}


def test_network_name_dollar_value(client, network_seed):
    """Raw property value: GET /Networks(id)/name/$value -> text/plain literal."""
    r = client.get(f"Networks({format_id(network_seed.net_a_id)})/name/$value")
    assert r.status_code == 200, r.text
    assert r.headers["content-type"].startswith("text/plain")
    assert r.text == network_seed.net_a_name
