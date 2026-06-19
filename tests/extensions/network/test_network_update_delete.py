"""
NETWORK extension -- update / delete + the two documented route DEVIATIONS.
Proprietary (NOT 18-088). Requires NETWORK=1. Each test owns and purges its data.

Routes verified to exist: PATCH /Networks(id), DELETE /Networks(id), and PATCH of
a Datastream's Network link (update/datastream.py allows "Network" under NETWORK).
Deviations (xfail, route not implemented -> 405; decided as a separate feature):
  * POST /Networks(id)/Datastreams  -> 405  (no nav-link-POST route)
  * PUT  /Networks(id)              -> 405  (update/network.py is PATCH-only;
                                            PUT exists only for the 8 standard entities)
"""

from __future__ import annotations

import pytest

from client import entity_id, format_id, id_from_self_link

pytestmark = pytest.mark.network

OM_MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"
_UOM = {"name": "Lumen", "symbol": "lm", "definition": "http://example.org/lumen"}


def _purge(client, tag):
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


def _datastream_in_network(client, tag, network_id):
    """Deep-insert a Thing whose single Datastream is linked to network_id; return ds id."""
    tree = {
        "name": f"{tag}-thing", "description": "d",
        "Datastreams": [{
            "name": f"{tag}-ds", "description": "d", "observationType": OM_MEASUREMENT,
            "unitOfMeasurement": _UOM,
            "Network": {"@iot.id": network_id},
            "ObservedProperty": {"name": f"{tag}-op", "definition": f"http://example.org/op/{tag}", "description": "o"},
            "Sensor": {"name": f"{tag}-s", "description": "s", "encodingType": "application/pdf", "metadata": "m"},
        }],
    }
    rt = client.create("Things", tree)
    assert rt.status_code == 201, rt.text
    return entity_id(client.nav(f"{client.location_of(rt)}/Datastreams")["value"][0])


def test_patch_network(client, unique_name):
    """PATCH /Networks(id) updates a Network's name."""
    tag = unique_name("netud")
    try:
        r = client.create("Networks", {"name": f"{tag}-net"})
        assert r.status_code == 201, r.text
        nid = id_from_self_link(client.location_of(r))
        pr = client.patch(f"Networks({format_id(nid)})", json={"name": f"{tag}-net-renamed"})
        assert pr.status_code in (200, 204), f"{pr.status_code}: {pr.text[:200]}"
        assert client.by_id("Networks", nid)["name"] == f"{tag}-net-renamed"
    finally:
        _purge(client, tag)


def test_delete_network_then_404(client, unique_name):
    """DELETE /Networks(id) removes the Network; GET afterwards -> 404."""
    tag = unique_name("netud")
    try:
        r = client.create("Networks", {"name": f"{tag}-net"})
        assert r.status_code == 201, r.text
        nid = id_from_self_link(client.location_of(r))
        dr = client.delete(f"Networks({format_id(nid)})")
        assert dr.status_code in (200, 204), f"{dr.status_code}: {dr.text[:200]}"
        assert client.get(f"Networks({format_id(nid)})").status_code == 404
    finally:
        _purge(client, tag)


def test_patch_datastream_relink_network(client, unique_name):
    """PATCH a Datastream to relink it from one Network to another (mirrors
    c02 test_patch_relation_relink_sensor)."""
    tag = unique_name("netud")
    try:
        r1 = client.create("Networks", {"name": f"{tag}-net1"})
        r2 = client.create("Networks", {"name": f"{tag}-net2"})
        assert r1.status_code == 201 and r2.status_code == 201
        n1 = id_from_self_link(client.location_of(r1))
        n2 = id_from_self_link(client.location_of(r2))
        ds_id = _datastream_in_network(client, tag, n1)

        pr = client.patch(f"Datastreams({format_id(ds_id)})", json={"Network": {"@iot.id": n2}})
        assert pr.status_code in (200, 204), f"{pr.status_code}: {pr.text[:200]}"
        net = client.nav(f"Datastreams({format_id(ds_id)})/Network")
        assert entity_id(net) == n2
    finally:
        _purge(client, tag)


# --------------------------------------------------------------------------
# Documented route DEVIATIONS (xfail until/unless implemented as a feature).
# --------------------------------------------------------------------------
@pytest.mark.xfail(
    reason="POST /Networks(id)/Datastreams is not implemented (-> 405): no "
           "nav-link-POST route for Network. NOTE: conformance does not test this "
           "for Sensor/ObservedProperty either, so Network is already at parity "
           "with the standard parent entities. Adding the route is a separate "
           "feature decision (source off-limits here). xpasses if it is added.",
    strict=False,
)
def test_post_to_network_datastreams_navlink(client, network_seed):
    """A Datastream could be created under a Network's nav-link; currently 405."""
    r = client.post(
        f"Networks({format_id(network_seed.net_a_id)})/Datastreams",
        json={
            "name": "navlink-ds", "description": "d", "observationType": OM_MEASUREMENT,
            "unitOfMeasurement": _UOM,
            "Thing": {"@iot.id": network_seed.thing_id},
            "ObservedProperty": {"@iot.id": network_seed.op_ids[0]},
            "Sensor": {"@iot.id": network_seed.sensor_ids[0]},
        },
    )
    assert r.status_code == 201, f"{r.status_code}: {r.text[:200]}"


@pytest.mark.xfail(
    reason="PUT /Networks(id) is not implemented (-> 405): update/network.py is "
           "PATCH-only; full-replace PUT exists only for the 8 standard STA "
           "entities. Adding PUT for Network is a separate feature decision "
           "(source off-limits here). xpasses if it is added.",
    strict=False,
)
def test_put_replace_network(client, unique_name):
    """A Network could be fully replaced via PUT; currently 405."""
    tag = unique_name("netud")
    try:
        r = client.create("Networks", {"name": f"{tag}-net"})
        assert r.status_code == 201, r.text
        nid = id_from_self_link(client.location_of(r))
        pr = client.put(f"Networks({format_id(nid)})", json={"name": f"{tag}-net-put"})
        assert pr.status_code in (200, 204), f"{pr.status_code}: {pr.text[:200]}"
    finally:
        _purge(client, tag)
