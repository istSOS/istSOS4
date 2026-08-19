"""
network/conftest.py -- seed for the NETWORK extension suite.

Requires the service running with NETWORK=1 (Datastream.network_id NOT NULL,
/Networks collection live). Builds a known, tag-scoped dataset and tears it down.
The shared DB is not assumed empty, so every entity is uniquely tagged and all
assertions scope to the seed (by id or by the unique network name).

Shape:
  Network A (<tag>-netA)  <- 2 Datastreams (A1: results [3,4], A2: result [5])
  Network B (<tag>-netB)  <- 1 Datastream  (B1: result [6])
  one Thing (+ 1 Location, for FeatureOfInterest auto-generation) owns all 3
  Datastreams; each Datastream has its own Sensor + ObservedProperty + Observations.

Creation uses deep-insert (the working path under NETWORK=1/AUTHORIZATION=0): a
Datastream carries its Network inline as {"@iot.id": <network_id>}.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import pytest

from client import entity_id, format_id, id_from_self_link

OM_MEASUREMENT = "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement"


def _datastream(tag: str, suffix: str, network_id, results: list) -> dict:
    """A Datastream payload linked to its Network inline (deep-insert form)."""
    return {
        "name": f"{tag}-ds-{suffix}",
        "description": "network datastream",
        "observationType": OM_MEASUREMENT,
        "unitOfMeasurement": {
            "name": "Lumen",
            "symbol": "lm",
            "definition": "http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html/Lumen",
        },
        "Network": {"@iot.id": network_id},
        "ObservedProperty": {
            "name": f"{tag}-op-{suffix}",
            "definition": f"http://example.org/op/{suffix}",
            "description": "observed property",
        },
        "Sensor": {
            "name": f"{tag}-sensor-{suffix}",
            "description": "sensor",
            "encodingType": "application/pdf",
            "metadata": "metadata",
        },
        "Observations": [
            {"phenomenonTime": f"2015-03-{i + 3:02d}T00:00:00Z", "result": r}
            for i, r in enumerate(results)
        ],
    }


@dataclass
class NetworkSeed:
    tag: str
    thing_id: object
    location_id: object
    net_a_id: object
    net_a_name: str
    net_b_id: object
    net_b_name: str
    a_ds_ids: list          # datastream ids in Network A (ordered by name)
    a_ds_names: list
    a_ds_results: list      # flattened results in Network A
    b_ds_ids: list
    b_ds_names: list
    sensor_ids: list = field(default_factory=list)
    op_ids: list = field(default_factory=list)
    foi_ids: list = field(default_factory=list)

    @property
    def all_ds_ids(self) -> list:
        return self.a_ds_ids + self.b_ds_ids


@pytest.fixture(scope="session")
def network_seed(client) -> NetworkSeed:
    tag = "netseed-" + uuid.uuid4().hex[:10]

    # 1. two Networks
    ra = client.create("Networks", {"name": f"{tag}-netA"})
    assert ra.status_code == 201, f"create Network A failed: {ra.status_code} {ra.text[:300]}"
    net_a_id = id_from_self_link(client.location_of(ra))
    rb = client.create("Networks", {"name": f"{tag}-netB"})
    assert rb.status_code == 201, f"create Network B failed: {rb.status_code} {rb.text[:300]}"
    net_b_id = id_from_self_link(client.location_of(rb))

    # 2. one Thing (+ Location) owning 3 Datastreams: 2 -> A, 1 -> B (Network inline)
    tree = {
        "name": f"{tag}-thing",
        "description": "network seed thing",
        "Locations": [
            {
                "name": f"{tag}-loc",
                "description": "loc",
                "encodingType": "application/vnd.geo+json",
                "location": {"type": "Point", "coordinates": [-117.05, 51.05]},
            }
        ],
        "Datastreams": [
            _datastream(tag, "A1", net_a_id, [3, 4]),
            _datastream(tag, "A2", net_a_id, [5]),
            _datastream(tag, "B1", net_b_id, [6]),
        ],
    }
    rt = client.create("Things", tree)
    assert rt.status_code == 201, f"seed deep-insert failed: {rt.status_code} {rt.text[:400]}"
    thing_url = client.location_of(rt)
    thing_id = entity_id(client.nav(thing_url))

    location_id = entity_id(client.nav(f"{thing_url}/Locations")["value"][0])

    # 3. read the created tree back, grouping datastreams by their Network
    ds_docs = client.nav(
        f"{thing_url}/Datastreams",
        {
            "$orderby": "name asc",
            "$expand": "Network,Sensor,ObservedProperty,"
                       "Observations($expand=FeatureOfInterest)",
        },
    )["value"]
    assert len(ds_docs) == 3, f"expected 3 seed datastreams, got {len(ds_docs)}"

    a_ids, a_names, a_results, b_ids, b_names = [], [], [], [], []
    sensor_ids, op_ids, foi = [], [], set()
    for ds in ds_docs:
        net = ds.get("Network")
        assert net, f"seed datastream {ds.get('name')} has no expanded Network"
        sensor_ids.append(entity_id(ds["Sensor"]))
        op_ids.append(entity_id(ds["ObservedProperty"]))
        for o in ds.get("Observations", []):
            if o.get("FeatureOfInterest"):
                foi.add(entity_id(o["FeatureOfInterest"]))
        if entity_id(net) == net_a_id:
            a_ids.append(entity_id(ds))
            a_names.append(ds["name"])
            a_results.extend(o["result"] for o in ds.get("Observations", []))
        elif entity_id(net) == net_b_id:
            b_ids.append(entity_id(ds))
            b_names.append(ds["name"])

    assert len(a_ids) == 2 and len(b_ids) == 1, (
        f"network grouping wrong: A={a_ids} B={b_ids}"
    )

    data = NetworkSeed(
        tag=tag,
        thing_id=thing_id,
        location_id=location_id,
        net_a_id=net_a_id,
        net_a_name=f"{tag}-netA",
        net_b_id=net_b_id,
        net_b_name=f"{tag}-netB",
        a_ds_ids=a_ids,
        a_ds_names=a_names,
        a_ds_results=a_results,
        b_ds_ids=b_ids,
        b_ds_names=b_names,
        sensor_ids=sensor_ids,
        op_ids=op_ids,
        foi_ids=sorted(foi, key=str),
    )

    yield data

    # 4. teardown. Deleting the Thing cascades its Datastreams (+Observations);
    #    deleting the Networks cascades any remaining datastreams. Location,
    #    Sensors, ObservedProperties and the auto FeatureOfInterest are
    #    independent -> delete explicitly. Tolerate 404s.
    def _del(path):
        try:
            client.delete(path)
        except Exception:
            pass

    _del(f"Things({format_id(thing_id)})")
    _del(f"Networks({format_id(net_a_id)})")
    _del(f"Networks({format_id(net_b_id)})")
    _del(f"Locations({format_id(location_id)})")
    for s in sensor_ids:
        _del(f"Sensors({format_id(s)})")
    for op in op_ids:
        _del(f"ObservedProperties({format_id(op)})")
    for f in data.foi_ids:
        _del(f"FeaturesOfInterest({format_id(f)})")
