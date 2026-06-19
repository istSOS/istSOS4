"""
conftest.py -- shared fixtures for the OGC SensorThings API v1.1 conformance
suite. Owned by the conformance LEAD; consumed (never edited) by c01/c02/c03.

Fixture contract (see docs/CONFORMANCE_PLAN.md):
  base_url    (session)  -- STA_BASE_URL env var or the project default.
  client      (session)  -- configured STAClient (helpers in client.py).
  seed        (session)  -- deep-inserts the EXACT entitiesDefault.json subtree
                            (1 Thing, 1 Location, 2 Datastreams x (Sensor +
                            ObservedProperty + 2 Observations)), yields a
                            SeedData of ids/expected values, DELETES everything
                            on teardown. READ-ONLY for c01/c03.
  unique_name (function) -- a *factory*: call unique_name() to get a fresh
                            UUID-tagged string (collision-free created entities
                            for c02).
"""

from __future__ import annotations

import os
import sys
import uuid
from dataclasses import dataclass, field

# Anchor tests/conformance/ (this file's dir) on sys.path so test modules living
# in the c01/ c02/ c03/ data_array/ subfolders can still `import sample_data` /
# `from client import …`. Runs before the local imports below and before any test
# module is imported (conftest is loaded first), so it works in any --import-mode.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest

import sample_data
from client import STAClient, DEFAULT_BASE_URL, entity_id, format_id


# ---------------------------------------------------------------------------
# base_url / client
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session")
def base_url() -> str:
    return os.environ.get("STA_BASE_URL", DEFAULT_BASE_URL).rstrip("/")


@pytest.fixture(scope="session")
def client(base_url) -> STAClient:
    c = STAClient(base_url=base_url)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# unique_name -- factory, call it to get a fresh tag (for c02-created entities)
# ---------------------------------------------------------------------------
@pytest.fixture
def unique_name():
    def _make(prefix: str = "conf") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:12]}"
    return _make


# ---------------------------------------------------------------------------
# seed -- the read-only known dataset (entitiesDefault.json, deep insert + teardown)
# ---------------------------------------------------------------------------
@dataclass
class DatastreamSeed:
    """One seeded Datastream and its dependents (ids derived at runtime)."""
    id: object
    name: str
    unit_name: str
    observed_property_id: object
    observed_property_name: str
    sensor_id: object
    sensor_name: str
    observation_ids: list = field(default_factory=list)
    results: list = field(default_factory=list)
    phenomenon_times: list = field(default_factory=list)


@dataclass
class SeedData:
    """Ids + expected values for the seeded entitiesDefault.json subtree.
    Everything is derived at runtime; id type is whatever the server assigns."""
    thing_id: object
    thing_name: str
    location_id: object
    location_name: str
    datastreams: list                       # list[DatastreamSeed], payload order (DS1, DS2)
    foi_ids: list = field(default_factory=list)

    # convenience accessors -------------------------------------------------
    @property
    def ds1(self) -> DatastreamSeed:
        return self.datastreams[0]

    @property
    def ds2(self) -> DatastreamSeed:
        return self.datastreams[1]

    @property
    def datastream_ids(self) -> list:
        return [d.id for d in self.datastreams]

    @property
    def all_observation_ids(self) -> list:
        return [oid for d in self.datastreams for oid in d.observation_ids]

    @property
    def all_results(self) -> list:
        return [r for d in self.datastreams for r in d.results]   # [3, 4, 5, 6]

    @property
    def all_phenomenon_times(self) -> list:
        return [t for d in self.datastreams for t in d.phenomenon_times]

    @property
    def n_observations(self) -> int:
        return len(self.all_observation_ids)                       # 4


@pytest.fixture(scope="session")
def seed(client) -> SeedData:
    # 1. Deep-insert the EXACT compliance dataset in one request.
    resp = client.create("Things", sample_data.deep_insert_tree())
    assert resp.status_code == 201, (
        f"seed deep-insert failed: {resp.status_code} {resp.text[:400]}"
    )
    thing_url = client.location_of(resp)
    thing = client.nav(thing_url)
    thing_id = entity_id(thing)

    # 2. Read ids back by navigating the created tree (id-type-agnostic),
    #    ordering deterministically so DS1/DS2 and observation results map
    #    to entitiesDefault.json order.
    locations = client.nav(f"{thing_url}/Locations")["value"]
    assert locations, "seed Thing has no Locations"
    location = locations[0]

    ds_docs = client.nav(
        f"{thing_url}/Datastreams",
        params={
            "$orderby": "name asc",
            "$expand": "Sensor,ObservedProperty,"
                       "Observations($orderby=phenomenonTime asc;"
                       "$expand=FeatureOfInterest)",
        },
    )["value"]
    assert len(ds_docs) == 2, f"expected 2 seed Datastreams, got {len(ds_docs)}"

    datastreams = []
    foi_ids = set()
    for ds in ds_docs:
        observations = ds["Observations"]
        for o in observations:
            if o.get("FeatureOfInterest"):
                foi_ids.add(entity_id(o["FeatureOfInterest"]))
        datastreams.append(
            DatastreamSeed(
                id=entity_id(ds),
                name=ds["name"],
                unit_name=ds["unitOfMeasurement"]["name"],
                observed_property_id=entity_id(ds["ObservedProperty"]),
                observed_property_name=ds["ObservedProperty"]["name"],
                sensor_id=entity_id(ds["Sensor"]),
                sensor_name=ds["Sensor"]["name"],
                observation_ids=[entity_id(o) for o in observations],
                results=[o["result"] for o in observations],
                phenomenon_times=[o["phenomenonTime"] for o in observations],
            )
        )

    data = SeedData(
        thing_id=thing_id,
        thing_name=thing["name"],
        location_id=entity_id(location),
        location_name=location["name"],
        datastreams=datastreams,
        foi_ids=sorted(foi_ids, key=str),
    )

    yield data

    # 3. Teardown. Deleting the Thing cascades to its Datastreams (and their
    #    Observations) and HistoricalLocations. Location, Sensors,
    #    ObservedProperties and the auto-generated FeatureOfInterest are
    #    independent entities -> delete explicitly. Tolerate 404s.
    def _safe_delete(path):
        try:
            client.delete(path)
        except Exception:
            pass

    _safe_delete(f"Things({format_id(thing_id)})")
    _safe_delete(f"Locations({format_id(data.location_id)})")
    for d in datastreams:
        _safe_delete(f"Sensors({format_id(d.sensor_id)})")
        _safe_delete(f"ObservedProperties({format_id(d.observed_property_id)})")
    for fid in data.foi_ids:
        _safe_delete(f"FeaturesOfInterest({format_id(fid)})")
