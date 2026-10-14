"""
Schema checks for STAC output built from edge-case harvest data, with no
running stack: open-ended Datastreams, missing geometry, empty names and
missing units are what the dummy data never produces, so validate/ alone
can't catch regressions there.
"""

from datetime import datetime, timedelta, timezone

import pytest
from asyncpg import Range

from app.v1.connector.harvester import HarvestedThing
from app.v1.connector.stac_transformer import _build_collection_dict, _build_item_dict

pystac = pytest.importorskip("pystac", reason="could not import pystac")

_CET = timezone(timedelta(hours=1))


def _datastream(ds_id, phenomenon_time, observed_area=None):
    return {
        "id": ds_id,
        "name": f"ds{ds_id}",
        "description": "test datastream",
        "properties": None,
        "phenomenon_time": phenomenon_time,
        "observed_area": observed_area,
        "observation_type": None,
        "unit_of_measurement": None,
        "observed_property": None,
        "sensor": None,
    }


@pytest.fixture
def thing():
    """Unnamed Thing, no Locations: one live Datastream without geometry,
    one closed Datastream with a point footprint."""
    return HarvestedThing(
        id=1,
        name="",
        description=None,
        properties=None,
        locations=[],
        datastreams=[
            _datastream(1, Range(datetime(2024, 1, 1, 12, tzinfo=_CET), None)),
            _datastream(
                2,
                Range(
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 2, 1, tzinfo=timezone.utc),
                ),
                {"type": "Point", "coordinates": [9.0, 46.0]},
            ),
        ],
    )


def _strip_tracking(collection: dict) -> dict:
    """Drop the cache-only keys api.py strips before serving."""
    return {k: v for k, v in collection.items() if k not in ("items", "item_ids")}


def test_open_ended_item_uses_interval_to_build_time(thing):
    # A live stream is published as an interval from its start to the build
    # time, so that queries for a recent period find it; "sta:ongoing" keeps
    # the Collection extent open-ended.
    item = _build_item_dict(thing, thing.datastreams[0], "thing-1")
    props = item["properties"]
    assert props["datetime"] is None
    assert props["start_datetime"] == "2024-01-01T11:00:00Z"
    assert props["end_datetime"] >= props["start_datetime"]
    assert props["sta:ongoing"] is True


def test_closed_item_uses_utc_interval(thing):
    item = _build_item_dict(thing, thing.datastreams[1], "thing-1")
    props = item["properties"]
    assert props["datetime"] is None
    assert props["start_datetime"] == "2024-01-01T00:00:00Z"
    assert props["end_datetime"] == "2024-02-01T00:00:00Z"


def test_item_without_geometry_has_no_bbox(thing):
    item = _build_item_dict(thing, thing.datastreams[0], "thing-1")
    assert item["geometry"] is None
    assert "bbox" not in item


def test_items_validate(thing):
    for ds in thing.datastreams:
        pystac.Item.from_dict(_build_item_dict(thing, ds, "thing-1")).validate()


def test_collection_validates_and_is_open_ended(thing):
    items = [_build_item_dict(thing, ds, "thing-1") for ds in thing.datastreams]
    collection = _build_collection_dict(thing, items)

    assert "title" not in collection
    assert "summaries" not in collection
    assert collection["extent"]["temporal"]["interval"] == [["2024-01-01T00:00:00Z", None]]

    pystac.Collection.from_dict(_strip_tracking(collection)).validate()
