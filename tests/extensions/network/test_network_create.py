"""
NETWORK extension -- create paths + Network mandatoriness (proprietary, NOT 18-088).
Requires NETWORK=1. Each test creates its own uniquely-tagged data and purges it.

Verified behaviour under NETWORK=1 / AUTHORIZATION=0:
  * Working create = DEEP-INSERT (Datastream carries Network inline, or Network
    carries nested Datastreams).
  * Datastream WITHOUT a Network -> 400 "Missing required properties 'Network'"
    (mandatory, exactly like omitting Sensor/ObservedProperty).
  * Direct POST /Datastreams (or POST /Things(id)/Datastreams) with an explicit
    "Network" key -> 400 "Invalid keys in payload: Network", because the endpoint
    only adds "Network" to its allowed keys under AUTHORIZATION (not NETWORK).
    That direct/link create path is therefore xfail here (see test below).
"""

from __future__ import annotations

import pytest

from client import entity_id, format_id, id_from_self_link

pytestmark = pytest.mark.network

OM_MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
_UOM = {"name": "Lumen", "symbol": "lm", "definition": "http://example.org/lumen"}


def _ds(tag, suffix, network_id=None):
    p = {
        "name": f"{tag}-ds-{suffix}",
        "description": "d",
        "observationType": OM_MEASUREMENT,
        "unitOfMeasurement": _UOM,
        "ObservedProperty": {"name": f"{tag}-op-{suffix}", "definition": f"http://example.org/op/{suffix}", "description": "o"},
        "Sensor": {"name": f"{tag}-s-{suffix}", "description": "s", "encodingType": "application/pdf", "metadata": "m"},
    }
    if network_id is not None:
        p["Network"] = {"@iot.id": network_id}
    return p


def _purge(client, tag):
    """Delete every tag-scoped entity (Things/Networks first cascade datastreams)."""
    for coll in ("Things", "Networks", "Locations", "Sensors", "ObservedProperties", "FeaturesOfInterest"):
        try:
            doc = client.collection(coll, {"$filter": f"substringof('{tag}',name)", "$select": "@iot.id"})
        except Exception:
            continue
        for e in doc.get("value", []):
            try:
                client.delete(f"{coll}({format_id(entity_id(e))})")
            except Exception:
                pass


def test_post_network_minimal(client, unique_name):
    """POST /Networks {name} -> 201 + Location; the network is then readable."""
    tag = unique_name("netcreate")
    try:
        r = client.create("Networks", {"name": f"{tag}-net"})
        assert r.status_code == 201, r.text
        nid = id_from_self_link(client.location_of(r))
        doc = client.by_id("Networks", nid)
        assert doc["name"] == f"{tag}-net"
    finally:
        _purge(client, tag)


def test_deep_insert_thing_datastream_with_network_inline(client, unique_name):
    """Deep-insert a Thing whose Datastream carries its Network inline -> 201,
    and the datastream resolves to that network."""
    tag = unique_name("netcreate")
    try:
        rn = client.create("Networks", {"name": f"{tag}-net"})
        assert rn.status_code == 201, rn.text
        nid = id_from_self_link(client.location_of(rn))

        tree = {
            "name": f"{tag}-thing", "description": "d",
            "Locations": [{"name": f"{tag}-loc", "description": "l",
                           "encodingType": "application/vnd.geo+json",
                           "location": {"type": "Point", "coordinates": [-117.05, 51.05]}}],
            "Datastreams": [_ds(tag, "1", network_id=nid)],
        }
        rt = client.create("Things", tree)
        assert rt.status_code == 201, rt.text
        thing_url = client.location_of(rt)
        ds = client.nav(f"{thing_url}/Datastreams")["value"][0]
        net = client.nav(f"Datastreams({format_id(entity_id(ds))})/Network")
        assert entity_id(net) == nid
    finally:
        _purge(client, tag)


def test_deep_insert_network_with_datastreams(client, unique_name):
    """Deep-insert from the Network side: POST /Networks with nested Datastreams
    -> 201, and the network then owns that datastream."""
    tag = unique_name("netcreate")
    try:
        # a Thing first (the datastream still needs a Thing + a Location for FoI)
        rt = client.create("Things", {
            "name": f"{tag}-thing", "description": "d",
            "Locations": [{"name": f"{tag}-loc", "description": "l",
                           "encodingType": "application/vnd.geo+json",
                           "location": {"type": "Point", "coordinates": [1, 1]}}],
        })
        assert rt.status_code == 201, rt.text
        thing_id = entity_id(client.nav(client.location_of(rt)))

        ds = _ds(tag, "n")
        ds["Thing"] = {"@iot.id": thing_id}
        rn = client.create("Networks", {"name": f"{tag}-net", "Datastreams": [ds]})
        assert rn.status_code == 201, rn.text
        nid = id_from_self_link(client.location_of(rn))

        owned = client.nav(f"Networks({format_id(nid)})/Datastreams")["value"]
        assert [d["name"] for d in owned] == [f"{tag}-ds-n"]
    finally:
        _purge(client, tag)


def test_datastream_requires_network(client, network_seed, unique_name):
    """Omitting Network on a Datastream create -> 4xx (mandatory, like omitting
    Sensor/ObservedProperty). Verified message: 'Missing required properties Network'."""
    tag = unique_name("netcreate")
    try:
        r = client.create(
            f"Things({format_id(network_seed.thing_id)})/Datastreams",
            _ds(tag, "nonet"),  # no Network key
        )
        assert 400 <= r.status_code < 500, f"expected 4xx, got {r.status_code}: {r.text[:200]}"
        assert "Network" in r.json().get("message", "")
    finally:
        _purge(client, tag)


def test_direct_post_datastream_with_network_link(client, unique_name):
    """Direct create with a Network {@iot.id} link: POST /Datastreams carrying
    Thing/Sensor/ObservedProperty/Network links -> 201, and it resolves to that
    Network. (api-fixed: create/datastream.py now gates the 'Network' allowed key
    on NETWORK, not AUTHORIZATION.) Uses its own throwaway Thing+Network so it
    never perturbs the shared network_seed grouping assertions."""
    tag = unique_name("netcreate")
    try:
        rn = client.create("Networks", {"name": f"{tag}-net"})
        assert rn.status_code == 201, rn.text
        nid = id_from_self_link(client.location_of(rn))
        rt = client.create("Things", {"name": f"{tag}-thing", "description": "d"})
        assert rt.status_code == 201, rt.text
        tid = entity_id(client.nav(client.location_of(rt)))

        ds = _ds(tag, "link", network_id=nid)
        ds["Thing"] = {"@iot.id": tid}
        r = client.create("Datastreams", ds)
        assert r.status_code == 201, f"{r.status_code}: {r.text[:200]}"

        dsid = id_from_self_link(client.location_of(r))
        net = client.nav(f"Datastreams({format_id(dsid)})/Network")
        assert entity_id(net) == nid
    finally:
        _purge(client, tag)
