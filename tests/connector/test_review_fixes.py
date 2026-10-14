"""
Regression tests for the fixes made after validating the connector on real
data (snapshots of the Fraunhofer FROST and Osnabrueck SensorThings servers):

  - every skos:Concept used as dcat:theme carries the skos:prefLabel that
    DCAT-AP's shapes require, and a Thing without a description still gets
    the mandatory dct:description on its DatasetSeries
  - a Thing whose Datastreams were all skipped yields no empty DatasetSeries
  - dct:license sits on the Distributions, not on the Dataset
  - the linear-time Turtle serializer produces the same graph as rdflib's
  - the cache publishes whole generations: readers never see a gap, a failed
    serialization leaves the previous catalog served, and two deployments
    with different key prefixes do not see each other's keys
  - a STAC failure in a harvest cycle no longer prevents the DCAT write

None of these need a running stack; Redis is replaced by an in-memory fake.
"""

from __future__ import annotations

import asyncio
import fnmatch
from datetime import datetime, timezone
from pathlib import Path

import pytest
from asyncpg import Range
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import DCAT, DCTERMS as DCT, RDF, SKOS

from app.v1.connector import cache, dcat_transformer, scheduler
from app.v1.connector.harvester import HarvestedCatalog, HarvestedThing
from app.v1.connector.serialization import serialize_turtle

SHAPES = Path(__file__).parent / "validate" / "turtle" / "dcat-ap-SHACL.ttl"


def _datastream(ds_id, *, phenomenon_time=True, definition="http://dd.eionet.europa.eu/vocabulary/aq/pollutant/8"):
    return {
        "id": ds_id,
        "name": f"ds{ds_id}",
        "description": "test datastream",
        "properties": None,
        "phenomenon_time": Range(datetime(2024, 1, 1, tzinfo=timezone.utc),
                                 datetime(2024, 2, 1, tzinfo=timezone.utc)) if phenomenon_time else None,
        "result_time": None,
        "observed_area": {"type": "Point", "coordinates": [9.0, 46.0]},
        "observation_type": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
        "unit_of_measurement": {"name": "ug.m-3", "symbol": "ug.m-3", "definition": ""},
        "observed_property": {"id": 1, "name": "NO2", "definition": definition, "description": ""},
        "sensor": {"id": 1, "name": "s1", "description": "", "encoding_type": "", "metadata": ""},
        "network_id": None,
        "network_name": None,
    }


def _catalog():
    return HarvestedCatalog(
        things=[
            # no description: the DatasetSeries must still get dct:description
            HarvestedThing(id=1, name="Station A", description=None, properties=None,
                           locations=[], datastreams=[_datastream(1), _datastream(2)]),
            # a Datastream with no phenomenonTime (no Observations yet): published
            # in DCAT-AP without dct:temporal (STAC has to skip it)
            HarvestedThing(id=2, name="Station B", description="b", properties=None,
                           locations=[], datastreams=[_datastream(3, phenomenon_time=False)]),
            # no Datastreams at all: no DatasetSeries
            HarvestedThing(id=3, name="Station C", description="c", properties=None,
                           locations=[], datastreams=[]),
        ],
        harvested_at="2026-10-13T00:00:00Z",
    )


@pytest.fixture
def dcat_graph(monkeypatch) -> Graph:
    # The root Catalog's title, description and publisher come from operator
    # configuration (the scheduler skips DCAT when they are unset); set them so
    # the SHACL check below judges the harvested content, not missing config.
    from app.v1.connector.config import get_settings
    for k, v in {"DCAT_CATALOG_TITLE": "Test catalog", "DCAT_CATALOG_DESCRIPTION": "Test",
                 "DCAT_PUBLISHER_NAME": "Test publisher", "DCAT_DEFAULT_LICENSE": "CC-BY-4.0"}.items():
        monkeypatch.setenv(k, v)
    get_settings.cache_clear()
    yield dcat_transformer.build_dcat_catalog(_catalog())["root"]
    get_settings.cache_clear()


def test_theme_concepts_have_preflabel(dcat_graph):
    concepts = set(dcat_graph.subjects(RDF.type, SKOS.Concept))
    assert concepts, "expected the http(s) ObservedProperty definition to become a skos:Concept"
    for concept in concepts:
        assert (concept, SKOS.prefLabel, None) in dcat_graph, f"{concept} has no skos:prefLabel"


def test_series_without_thing_description_still_has_description(dcat_graph):
    series = set(dcat_graph.subjects(RDF.type, DCAT.DatasetSeries))
    assert len(series) == 2, "Stations A and B have Datasets; C has none and gets no series"
    for s in series:
        assert (s, DCT.description, None) in dcat_graph


def test_dataset_without_phenomenon_time_is_published_without_temporal(dcat_graph):
    datasets = [d for d in dcat_graph.subjects(RDF.type, DCAT.Dataset) if str(d).endswith("datastream-3")]
    assert datasets, "a Datastream with no phenomenonTime must still be published in DCAT-AP"
    assert (datasets[0], DCT.temporal, None) not in dcat_graph


def test_no_empty_dataset_series(dcat_graph):
    for s in dcat_graph.subjects(RDF.type, DCAT.DatasetSeries):
        assert list(dcat_graph.objects(s, DCAT.seriesMember)), f"{s} has no member"


def test_blank_node_publisher_is_linked_to_catalog(monkeypatch):
    # DCAT_PUBLISHER_NAME without DCAT_PUBLISHER_URI used to leave the Catalog
    # with no dct:publisher at all, a DCAT-AP mandatory-field violation.
    from app.v1.connector.config import get_settings
    for k, v in {"DCAT_CATALOG_TITLE": "T", "DCAT_CATALOG_DESCRIPTION": "D",
                 "DCAT_PUBLISHER_NAME": "P", "DCAT_PUBLISHER_URI": ""}.items():
        monkeypatch.setenv(k, v)
    get_settings.cache_clear()
    try:
        g = dcat_transformer.build_dcat_catalog(_catalog())["root"]
    finally:
        get_settings.cache_clear()
    for catalog in g.subjects(RDF.type, DCAT.Catalog):
        assert (catalog, DCT.publisher, None) in g


def test_license_on_distributions_not_on_dataset(dcat_graph):
    for ds in dcat_graph.subjects(RDF.type, DCAT.Dataset):
        assert (ds, DCT.license, None) not in dcat_graph
    dists = list(dcat_graph.subjects(RDF.type, DCAT.Distribution))
    assert dists and all((d, DCT.license, None) in dcat_graph for d in dists)


@pytest.mark.skipif(not SHAPES.exists(), reason="DCAT-AP shapes not downloaded (run tests/connector/validate once)")
def test_real_vocabulary_path_conforms_to_dcat_ap(dcat_graph):
    pyshacl = pytest.importorskip("pyshacl")
    conforms, _g, text = pyshacl.validate(dcat_graph, shacl_graph=Graph().parse(SHAPES), inference="none")
    assert conforms, text


def test_fast_turtle_round_trips(dcat_graph):
    back = Graph().parse(data=serialize_turtle(dcat_graph), format="turtle")
    assert len(back) == len(dcat_graph)
    ground = lambda g: {t for t in g if not any(type(x).__name__ == "BNode" for x in t)}  # noqa: E731
    assert ground(back) == ground(dcat_graph)


# ---------------------------------------------------------------- cache generations


class _FakeRedis:
    """Just enough of redis-py for cache.py: get/set/delete/scan_iter/pipeline."""

    def __init__(self):
        self.data: dict = {}

    def get(self, key):
        v = self.data.get(key)
        return v.encode() if isinstance(v, str) else v

    def set(self, key, value):
        self.data[key] = value

    def delete(self, *keys):
        for k in keys:
            self.data.pop(k.decode() if isinstance(k, bytes) else k, None)

    def scan_iter(self, match="*", count=None):
        return [k.encode() for k in list(self.data) if fnmatch.fnmatchcase(k, match)]

    def pipeline(self, transaction=False):
        outer = self

        class _Pipe:
            def __init__(self):
                self.ops = []

            def set(self, k, v):
                self.ops.append((k, v))

            def execute(self):
                for k, v in self.ops:
                    outer.set(k, v)

        return _Pipe()


@pytest.fixture
def fake_redis(monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(cache, "redis", fake)
    return fake


def _root_graph(title):
    g = Graph()
    g.add((URIRef("http://example.org/catalog"), DCT.title, __import__("rdflib").Literal(title)))
    return {"root": g}


def _served_title(raw):
    g = Graph().parse(data=raw, format="turtle")
    return str(next(g.objects(None, DCT.title)))


def _generations(fake):
    prefix = f"{cache.CONNECTOR_KEY_PREFIX}:{cache._DCAT}:"
    return {k[len(prefix):].split(":", 1)[0] for k in fake.data
            if k.startswith(prefix)} - {"current", "previous"}


def test_generation_swap_and_garbage_collection(fake_redis):
    cache.write_dcat_catalog(_root_graph("one"))
    first = cache._current_generation(cache._DCAT)
    cache.write_dcat_catalog(_root_graph("two"))
    second = cache._current_generation(cache._DCAT)
    assert _served_title(asyncio.run(cache.get_dcat_root())) == "two"
    assert first != second
    gens = _generations(fake_redis)
    assert gens == {first, second}, "the previous generation is kept for in-flight readers"
    cache.write_dcat_catalog(_root_graph("three"))
    gens = _generations(fake_redis)
    assert first not in gens, "generations older than the previous one are deleted"


def test_failed_serialization_keeps_previous_catalog(fake_redis, monkeypatch):
    cache.write_dcat_catalog(_root_graph("good"))

    def boom(_graph):
        raise RuntimeError("serializer failed")

    monkeypatch.setattr(cache, "serialize_turtle", boom)
    with pytest.raises(RuntimeError):
        cache.write_dcat_catalog(_root_graph("bad"))
    assert _served_title(asyncio.run(cache.get_dcat_root())) == "good"


def test_key_prefix_isolates_deployments(fake_redis, monkeypatch):
    monkeypatch.setattr(cache, "CONNECTOR_KEY_PREFIX", "connector:deployment_a")
    cache.write_dcat_catalog(_root_graph("a"))
    monkeypatch.setattr(cache, "CONNECTOR_KEY_PREFIX", "connector:deployment_b")
    assert asyncio.run(cache.get_dcat_root()) is None
    cache.write_dcat_catalog(_root_graph("b"))
    monkeypatch.setattr(cache, "CONNECTOR_KEY_PREFIX", "connector:deployment_a")
    assert _served_title(asyncio.run(cache.get_dcat_root())) == "a"


# ---------------------------------------------------------------- scheduler isolation


def test_stac_failure_does_not_block_dcat(monkeypatch):
    calls = []

    async def fake_harvest(_conn):
        return _catalog()

    def stac_boom(_catalog):
        raise RuntimeError("STAC transformer failed")

    monkeypatch.setattr(scheduler, "NETWORK", 0)
    monkeypatch.setattr(scheduler, "STAC_TRANSFORMER", 1)
    monkeypatch.setattr(scheduler, "DCAT_TRANSFORMER", 1)
    monkeypatch.setattr(scheduler, "harvest", fake_harvest)
    monkeypatch.setattr(scheduler, "build_stac_catalog", stac_boom)
    monkeypatch.setattr(scheduler, "build_dcat_catalog", lambda c: calls.append("dcat-build") or {"root": Graph()})
    monkeypatch.setattr(scheduler, "write_dcat_catalog", lambda d: calls.append("dcat-write"))
    config = type("Cfg", (), {"has_mandatory_dcat_fields": True})()
    asyncio.run(scheduler._run_cycle(object(), config))
    assert calls == ["dcat-build", "dcat-write"]


# ---------------------------------------------------------------- geometry encoding


@pytest.mark.parametrize("encoding", ["node", "literal"])
def test_geometry_encoding_setting(monkeypatch, encoding):
    # The DCAT-AP 3.0.x shapes require locn:geometry to point at a locn:Geometry node; ckanext-dcat
    # (2.4.x) reads only a literal and fails on the node. DCAT_GEOMETRY_ENCODING chooses between them.
    from rdflib import Literal as RdfLiteral
    from app.v1.connector.config import get_settings
    LOCN = Namespace("http://www.w3.org/ns/locn#")
    for k, v in {"DCAT_CATALOG_TITLE": "T", "DCAT_CATALOG_DESCRIPTION": "D", "DCAT_PUBLISHER_NAME": "P",
                 "DCAT_GEOMETRY_ENCODING": encoding}.items():
        monkeypatch.setenv(k, v)
    get_settings.cache_clear()
    try:
        g = dcat_transformer.build_dcat_catalog(_catalog())["root"]
    finally:
        get_settings.cache_clear()
    values = list(g.objects(None, LOCN.geometry))
    assert values
    if encoding == "literal":
        assert all(isinstance(v, RdfLiteral) and v.datatype is not None for v in values)
    else:
        assert not any(isinstance(v, RdfLiteral) for v in values)
