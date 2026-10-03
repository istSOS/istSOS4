"""
NETWORK extension -- bidirectional navigation Datastream <-> Network
(many-to-one / one-to-many), $ref, $expand and the relation $filter.
Proprietary (NOT 18-088). Requires NETWORK=1. Scoped to network_seed.
"""

from __future__ import annotations

import pytest

from client import entity_id, format_id

pytestmark = pytest.mark.network


def _names(docs):
    return sorted(d["name"] for d in docs)


def test_datastream_to_network(client, network_seed):
    """Many-to-one: GET /Datastreams(id)/Network resolves to the owning Network."""
    ds_id = network_seed.a_ds_ids[0]
    net = client.nav(f"Datastreams({format_id(ds_id)})/Network")
    assert entity_id(net) == network_seed.net_a_id
    assert net["name"] == network_seed.net_a_name


def test_datastream_network_ref(client, network_seed):
    """$ref on the many-to-one link: GET /Datastreams(id)/Network/$ref -> selfLink."""
    ds_id = network_seed.a_ds_ids[0]
    r = client.get(f"Datastreams({format_id(ds_id)})/Network/$ref")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("@iot.selfLink", "").rstrip("/").endswith(
        f"Networks({format_id(network_seed.net_a_id)})"
    )


def test_network_to_datastreams_grouping_A(client, network_seed):
    """One-to-many: GET /Networks(A)/Datastreams returns exactly Network A's
    datastreams (grouping by network)."""
    doc = client.nav(f"Networks({format_id(network_seed.net_a_id)})/Datastreams")
    assert _names(doc["value"]) == sorted(network_seed.a_ds_names)
    assert sorted(entity_id(d) for d in doc["value"]) == sorted(network_seed.a_ds_ids)


def test_network_to_datastreams_grouping_B(client, network_seed):
    """Grouping is per-network: Network B has only its single datastream."""
    doc = client.nav(f"Networks({format_id(network_seed.net_b_id)})/Datastreams")
    assert _names(doc["value"]) == sorted(network_seed.b_ds_names)


def test_network_datastreams_ref(client, network_seed):
    """$ref on the one-to-many link: GET /Networks(A)/Datastreams/$ref -> 200
    with one selfLink per linked datastream."""
    r = client.get(f"Networks({format_id(network_seed.net_a_id)})/Datastreams/$ref")
    assert r.status_code == 200, r.text
    refs = r.json().get("value", [])
    assert len(refs) == len(network_seed.a_ds_ids)
    assert all("@iot.selfLink" in ref for ref in refs)


def test_datastream_exposes_network_navlink(client, network_seed):
    """The Datastream representation gains a Network@iot.navigationLink (a
    proprietary extra relation beyond 18-088, present only under NETWORK=1)."""
    ds_id = network_seed.a_ds_ids[0]
    doc = client.by_id("Datastreams", ds_id)
    assert "Network@iot.navigationLink" in doc
    assert doc["Network@iot.navigationLink"].rstrip("/").endswith(
        f"Datastreams({format_id(ds_id)})/Network"
    )


def test_expand_network_on_datastream(client, network_seed):
    """$expand=Network embeds the Network in the Datastream response."""
    ds_id = network_seed.a_ds_ids[0]
    doc = client.collection(
        "Datastreams",
        {"$filter": f"id eq {format_id(ds_id)}", "$expand": "Network"},
    )
    assert doc["value"], "datastream not found"
    net = doc["value"][0].get("Network")
    assert net and entity_id(net) == network_seed.net_a_id
    assert net["name"] == network_seed.net_a_name


def test_filter_datastreams_by_network_name(client, network_seed):
    """Relation $filter: Datastreams?$filter=Network/name eq '<seed A>' returns
    exactly Network A's datastreams (deterministic via the unique network name)."""
    doc = client.collection(
        "Datastreams",
        {"$filter": f"Network/name eq '{network_seed.net_a_name}'"},
    )
    assert sorted(entity_id(d) for d in doc["value"]) == sorted(network_seed.a_ds_ids)


def test_select_network_navigation_on_datastream(client, network_seed):
    """$select of the Network navigation property on a Datastream returns just the
    Network@iot.navigationLink (mirrors c03 test_select_navigation_property)."""
    ds_id = network_seed.a_ds_ids[0]
    doc = client.collection(
        "Datastreams",
        {"$filter": f"id eq {format_id(ds_id)}", "$select": "Network"},
    )
    assert doc["value"], "datastream not found"
    assert set(doc["value"][0].keys()) == {"Network@iot.navigationLink"}
