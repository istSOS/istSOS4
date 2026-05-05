# Copyright 2025 SUPSI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module: STA2REST Test

This module provides unit tests for the STA2REST module.

NOTE: The tests previously located in api/app/sta2rest/test.py were
migrated to pytest and moved to: test/unit/test_sta2rest.py
Prior unittest coverage was extended in PR #37 by lightning-sagar.

Usage:
    pytest test/unit/test_sta2rest.py -v

Author: Filippo Finke
Migrated to pytest by: Vishmayraj
"""

import pytest
from app.sta2rest.sta2rest import STA2REST

"""
Test the conversion of entities.
"""


ENTITY_MAPPINGS = [
    # Plural forms
    ("Networks", "Network"),
    ("Commits", "Commit"),
    ("Things", "Thing"),
    ("Locations", "Location"),
    ("Sensors", "Sensor"),
    ("ObservedProperties", "ObservedProperty"),
    ("Datastreams", "Datastream"),
    ("Observations", "Observation"),
    ("FeaturesOfInterest", "FeaturesOfInterest"),
    ("HistoricalLocations", "HistoricalLocation"),
    # Singular forms
    ("Network", "Network"),
    ("Commit", "Commit"),
    ("Thing", "Thing"),
    ("Location", "Location"),
    ("Sensor", "Sensor"),
    ("ObservedProperty", "ObservedProperty"),
    ("Datastream", "Datastream"),
    ("Observation", "Observation"),
    ("FeatureOfInterest", "FeaturesOfInterest"),
    ("HistoricalLocation", "HistoricalLocation"),
]


@pytest.mark.parametrize("entity,expected", ENTITY_MAPPINGS)
def test_convert_entity(entity, expected):
    assert STA2REST.convert_entity(entity) == expected


"""
Test the parsing of URIs.
"""


PARSE_URI_CASES = [
    (
        "/v1.1/ObservedProperties",
        {
            "version": "/v1.1",
            "entity": ("ObservedProperty", None),
            "entities": [],
            "property_name": "",
            "ref": False,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Things(1)",
        {
            "version": "/v1.1",
            "entity": ("Thing", "1"),
            "entities": [],
            "property_name": "",
            "ref": False,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Observations(1)/resultTime",
        {
            "version": "/v1.1",
            "entity": ("Observation", "1"),
            "entities": [],
            "property_name": "resultTime",
            "ref": False,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Observations(1)/resultTime/$value",
        {
            "version": "/v1.1",
            "entity": ("Observation", "1"),
            "entities": [],
            "property_name": "resultTime",
            "ref": False,
            "value": True,
            "single": False,
        },
    ),
    (
        "/v1.1/Datastreams(1)/Observations",
        {
            "version": "/v1.1",
            "entity": ("Observation", None),
            "entities": [("Datastream", "1")],
            "property_name": "",
            "ref": False,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Datastreams(1)/Observations/$ref",
        {
            "version": "/v1.1",
            "entity": ("Observation", None),
            "entities": [("Datastream", "1")],
            "property_name": "",
            "ref": True,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Datastreams(1)/Observations(1)",
        {
            "version": "/v1.1",
            "entity": ("Observation", "1"),
            "entities": [("Datastream", "1")],
            "property_name": "",
            "ref": False,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Datastreams(1)/Observations(1)/resultTime",
        {
            "version": "/v1.1",
            "entity": ("Observation", "1"),
            "entities": [("Datastream", "1")],
            "property_name": "resultTime",
            "ref": False,
            "value": False,
            "single": False,
        },
    ),
    (
        "/v1.1/Datastreams(1)/Observations(1)/FeatureOfInterest",
        {
            "version": "/v1.1",
            "entity": ("FeaturesOfInterest", None),
            "entities": [("Observation", "1"), ("Datastream", "1")],
            "property_name": "",
            "ref": False,
            "value": False,
            "single": True,
        },
    ),
]


@pytest.mark.parametrize("uri,expected", PARSE_URI_CASES)
def test_parse_uri(uri, expected):
    assert STA2REST.parse_uri(uri) == expected


"""
Test conversion behavior with full SensorThings paths.
"""


def test_convert_query_requires_full_path():
    """Bare OData params without a path prefix must raise."""
    with pytest.raises(Exception):
        STA2REST.convert_query("$top=5")


def test_convert_query_invalid_field_raises():
    """
    Legacy FeatureOfInterest filter path currently raises an invalid field error.
    """
    with pytest.raises(Exception):
        STA2REST.convert_query(
            "/v1.1/Observations?$filter=result le 3.5 and FeatureOfInterest/id eq 1"
        )


CONVERT_QUERY_CASES = [
    {
        "id": "top",
        "path": "/v1.1/Observations?$top=5",
        "main_entity": "Observation",
        "single_result": False,
        "is_count": False,
        "contains": ['FROM sensorthings."Observation"', "LIMIT 6"],
    },
    {
        "id": "skip",
        "path": "/v1.1/Observations?$skip=5",
        "main_entity": "Observation",
        "single_result": False,
        "is_count": False,
        "contains": ["OFFSET 5"],
    },
    {
        "id": "count",
        "path": "/v1.1/Observations?$count=true",
        "main_entity": "Observation",
        "single_result": False,
        "is_count": True,
        "contains": ['FROM sensorthings."Observation"'],
    },
    {
        "id": "related_top",
        "path": "/v1.1/Datastreams(1)/Observations?$top=5",
        "main_entity": "Observation",
        "single_result": False,
        "is_count": False,
        "contains": ["datastream_id", "LIMIT 6"],
    },
    {
        "id": "property",
        "path": "/v1.1/Observations(1)/resultTime",
        "main_entity": "Observation",
        "single_result": True,
        "is_count": False,
        "contains": ["resultTime", "id = 1"],
    },
    {
        "id": "property_value",
        "path": "/v1.1/Observations(1)/resultTime/$value",
        "main_entity": "Observation",
        "single_result": True,
        "is_count": False,
        "contains": ["resultTime", "id = 1"],
    },
]


@pytest.mark.parametrize(
    "case", CONVERT_QUERY_CASES, ids=[c["id"] for c in CONVERT_QUERY_CASES]
)
def test_convert_query(case):
    converted = STA2REST.convert_query(case["path"])

    assert isinstance(converted, dict)
    assert converted["main_entity"] == case["main_entity"]
    assert converted["single_result"] == case["single_result"]
    assert converted["is_count"] == case["is_count"]
    assert "main_query" in converted

    for fragment in case["contains"]:
        assert (
            fragment in converted["main_query"]
        ), f"Expected {fragment!r} not found in:\n{converted['main_query']}"

    if case["is_count"]:
        assert len(converted["count_queries"]) > 0
    else:
        assert converted["count_queries"] == []
