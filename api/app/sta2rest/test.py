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

Author: Filippo Finke

This module provides unit tests for the STA2REST module.
"""
import unittest

from .sta2rest import STA2REST


class STA2RESTTestCase(unittest.TestCase):
    """
    Test case for STA2REST module.
    """

    def test_convert_entity(self):
        """
        Test the conversion of entities.
        """
        entity_mappings = {
            "Networks": "Network",
            "Commits": "Commit",
            "Things": "Thing",
            "Locations": "Location",
            "Sensors": "Sensor",
            "ObservedProperties": "ObservedProperty",
            "Datastreams": "Datastream",
            "Observations": "Observation",
            "FeaturesOfInterest": "FeaturesOfInterest",
            "HistoricalLocations": "HistoricalLocation",
            "Network": "Network",
            "Commit": "Commit",
            "Thing": "Thing",
            "Location": "Location",
            "Sensor": "Sensor",
            "ObservedProperty": "ObservedProperty",
            "Datastream": "Datastream",
            "Observation": "Observation",
            "FeatureOfInterest": "FeaturesOfInterest",
            "HistoricalLocation": "HistoricalLocation",
        }

        for entity, expected in entity_mappings.items():
            self.assertEqual(STA2REST.convert_entity(entity), expected)

    def test_parse_uri(self):
        """
        Test the parsing of URIs.
        """

        tests = {
            "/v1.1/ObservedProperties": {
                "version": "/v1.1",
                "entity": ("ObservedProperty", None),
                "entities": [],
                "property_name": "",
                "ref": False,
                "value": False,
                "single": False,
            },
            "/v1.1/Things(1)": {
                "version": "/v1.1",
                "entity": ("Thing", "1"),
                "entities": [],
                "property_name": "",
                "ref": False,
                "value": False,
                "single": False,
            },
            "/v1.1/Observations(1)/resultTime": {
                "version": "/v1.1",
                "entity": ("Observation", "1"),
                "entities": [],
                "property_name": "resultTime",
                "ref": False,
                "value": False,
                "single": False,
            },
            "/v1.1/Observations(1)/resultTime/$value": {
                "version": "/v1.1",
                "entity": ("Observation", "1"),
                "entities": [],
                "property_name": "resultTime",
                "ref": False,
                "value": True,
                "single": False,
            },
            "/v1.1/Datastreams(1)/Observations": {
                "version": "/v1.1",
                "entity": ("Observation", None),
                "entities": [("Datastream", "1")],
                "property_name": "",
                "ref": False,
                "value": False,
                "single": False,
            },
            "/v1.1/Datastreams(1)/Observations/$ref": {
                "version": "/v1.1",
                "entity": ("Observation", None),
                "entities": [("Datastream", "1")],
                "property_name": "",
                "ref": True,
                "value": False,
                "single": False,
            },
            "/v1.1/Datastreams(1)/Observations(1)": {
                "version": "/v1.1",
                "entity": ("Observation", "1"),
                "entities": [("Datastream", "1")],
                "property_name": "",
                "ref": False,
                "value": False,
                "single": False,
            },
            "/v1.1/Datastreams(1)/Observations(1)/resultTime": {
                "version": "/v1.1",
                "entity": ("Observation", "1"),
                "entities": [("Datastream", "1")],
                "property_name": "resultTime",
                "ref": False,
                "value": False,
                "single": False,
            },
            "/v1.1/Datastreams(1)/Observations(1)/FeatureOfInterest": {
                "version": "/v1.1",
                "entity": ("FeaturesOfInterest", None),
                "entities": [("Observation", "1"), ("Datastream", "1")],
                "property_name": "",
                "ref": False,
                "value": False,
                "single": True,
            },
        }

        for uri, expected in tests.items():
            with self.subTest(uri=uri):
                self.assertEqual(STA2REST.parse_uri(uri), expected)

    def test_convert_query_requires_full_path(self):
        with self.assertRaises(Exception):
            STA2REST.convert_query("$top=5")

    def test_convert_query_current_behavior(self):
        """
        Test conversion behavior with full SensorThings paths.
        """

        cases = [
            {
                "path": "/v1.1/Observations?$top=5",
                "main_entity": "Observation",
                "single_result": False,
                "contains": ["FROM sensorthings.\"Observation\"", "LIMIT 6"],
                "is_count": False,
            },
            {
                "path": "/v1.1/Observations?$skip=5",
                "main_entity": "Observation",
                "single_result": False,
                "contains": ["OFFSET 5"],
                "is_count": False,
            },
            {
                "path": "/v1.1/Observations?$count=true",
                "main_entity": "Observation",
                "single_result": False,
                "contains": ["FROM sensorthings.\"Observation\""],
                "is_count": True,
            },
            {
                "path": "/v1.1/Datastreams(1)/Observations?$top=5",
                "main_entity": "Observation",
                "single_result": False,
                "contains": ["datastream_id", "LIMIT 6"],
                "is_count": False,
            },
            {
                "path": "/v1.1/Observations(1)/resultTime",
                "main_entity": "Observation",
                "single_result": True,
                "contains": ["resultTime", "id = 1"],
                "is_count": False,
            },
            {
                "path": "/v1.1/Observations(1)/resultTime/$value",
                "main_entity": "Observation",
                "single_result": True,
                "contains": ["resultTime", "id = 1"],
                "is_count": False,
            },
        ]

        for case in cases:
            with self.subTest(path=case["path"]):
                converted = STA2REST.convert_query(case["path"])

                self.assertIsInstance(converted, dict)
                self.assertEqual(converted["main_entity"], case["main_entity"])
                self.assertEqual(converted["single_result"], case["single_result"])
                self.assertEqual(converted["is_count"], case["is_count"])
                self.assertIn("main_query", converted)

                for expected_fragment in case["contains"]:
                    self.assertIn(expected_fragment, converted["main_query"])

                if case["is_count"]:
                    self.assertGreater(len(converted["count_queries"]), 0)
                else:
                    self.assertEqual(converted["count_queries"], [])

    def test_convert_query_invalid_field_raises(self):
        """
        Legacy FeatureOfInterest filter path currently raises an invalid field error.
        """
        with self.assertRaises(Exception):
            STA2REST.convert_query(
                "/v1.1/Observations?$filter=result le 3.5 and FeatureOfInterest/id eq 1"
            )


if __name__ == "__main__":
    # Run all tests
    unittest.main()
