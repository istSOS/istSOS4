"""
NETWORK extension -- create / update / delete for the Network-gated routes.
Proprietary (NOT 18-088). Requires NETWORK=1. Each test owns and purges its data.

Routes verified to exist under NETWORK=1:
  * PATCH  /Networks(id)              -- partial update of a Network
  * PUT    /Networks(id)              -- full-replace of a Network
                                         (req/create-update-delete/update-entity-put)
  * DELETE /Networks(id)             -- delete a Network
  * POST   /Networks(id)/Datastreams -- nav-link create of a Datastream under a
                                         Network (req/create-update-delete/create-entity,
                                         Req 33); the URL supplies the network_id
  * PATCH of a Datastream's Network link (update/datastream.py allows "Network").
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
# Network-gated nav-link create + full-replace PUT.
# --------------------------------------------------------------------------
def test_post_to_network_datastreams_navlink(client, unique_name):
    """POST /Networks(id)/Datastreams nav-link create: a Datastream posted to a
    Network's navigation link is created (201 + Location) with the Network taken
    from the URL (Req 33 nav-link create, req/create-update-delete/create-entity;
    Network-gated extension). The body carries Thing/Sensor/ObservedProperty by
    @iot.id (Table 24); the Network is NOT in the body. Self-contained throwaway
    data so the shared network_seed grouping is never perturbed."""
    tag = unique_name("netud")
    try:
        rn = client.create("Networks", {"name": f"{tag}-net"})
        assert rn.status_code == 201, rn.text
        nid = id_from_self_link(client.location_of(rn))
        rt = client.create("Things", {"name": f"{tag}-thing", "description": "d"})
        assert rt.status_code == 201, rt.text
        tid = entity_id(client.nav(client.location_of(rt)))
        rs = client.create("Sensors", {"name": f"{tag}-s", "description": "s",
                                        "encodingType": "application/pdf", "metadata": "m"})
        assert rs.status_code == 201, rs.text
        sid = id_from_self_link(client.location_of(rs))
        ro = client.create("ObservedProperties", {"name": f"{tag}-op",
                                                   "definition": f"http://example.org/op/{tag}",
                                                   "description": "o"})
        assert ro.status_code == 201, ro.text
        oid = id_from_self_link(client.location_of(ro))

        r = client.post(
            f"Networks({format_id(nid)})/Datastreams",
            json={
                "name": f"{tag}-navlink-ds", "description": "d",
                "observationType": OM_MEASUREMENT, "unitOfMeasurement": _UOM,
                "Thing": {"@iot.id": tid},
                "ObservedProperty": {"@iot.id": oid},
                "Sensor": {"@iot.id": sid},
            },
        )
        assert r.status_code == 201, f"{r.status_code}: {r.text[:200]}"
        loc = client.location_of(r)  # KeyError here if no Location header
        assert loc, "201 nav-link create must return a Location header"

        # Verify the link: the created Datastream resolves to the path Network.
        ds_id = id_from_self_link(loc)
        net = client.nav(f"Datastreams({format_id(ds_id)})/Network")
        assert entity_id(net) == nid, f"created datastream linked to {entity_id(net)}, expected {nid}"
    finally:
        _purge(client, tag)


def test_put_replace_network(client, unique_name):
    """PUT /Networks(id) full-replace: a valid replacement body succeeds and the
    follow-up GET reflects the replaced name under the same @iot.id
    (req/create-update-delete/update-entity-put; Network-gated extension)."""
    tag = unique_name("netud")
    try:
        r = client.create("Networks", {"name": f"{tag}-net"})
        assert r.status_code == 201, r.text
        nid = id_from_self_link(client.location_of(r))
        pr = client.put(f"Networks({format_id(nid)})", json={"name": f"{tag}-net-put"})
        assert pr.status_code in (200, 204), f"{pr.status_code}: {pr.text[:200]}"
        doc = client.by_id("Networks", nid)
        assert doc["name"] == f"{tag}-net-put", doc
        assert entity_id(doc) == nid, doc
    finally:
        _purge(client, tag)


def test_put_network_missing_name(client, unique_name):
    """PUT /Networks(id) full-replace omitting the mandatory `name` -> 400 (a
    full replace requires the mandatory properties; must be a clean 400, not 500).
    (req/create-update-delete/update-entity-put; Network-gated extension)."""
    tag = unique_name("netud")
    try:
        r = client.create("Networks", {"name": f"{tag}-net"})
        assert r.status_code == 201, r.text
        nid = id_from_self_link(client.location_of(r))
        pr = client.put(f"Networks({format_id(nid)})", json={"description": "no name"})
        assert pr.status_code == 400, f"{pr.status_code}: {pr.text[:200]}"
        assert "name" in pr.json().get("message", ""), pr.text[:200]
    finally:
        _purge(client, tag)
