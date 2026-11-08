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

import unittest

from app.utils.utils import validate_geojson_geometry


class ValidateGeojsonGeometryTest(unittest.TestCase):
    def test_valid_geometries_pass(self):
        valid = [
            {"type": "Point", "coordinates": [8.97, 45.87]},
            {"type": "Point", "coordinates": [8.97, 45.87, 350.0]},
            {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
            {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]],
            },
            {
                "type": "MultiPolygon",
                "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 0]]]],
            },
            {
                "type": "GeometryCollection",
                "geometries": [{"type": "Point", "coordinates": [1, 2]}],
            },
            {
                "type": "Point",
                "coordinates": [1, 2],
                "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
            },
        ]
        for geometry in valid:
            with self.subTest(geometry=geometry):
                validate_geojson_geometry(geometry, "location")

    def test_unknown_type_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_geojson_geometry(
                {"type": "Banana", "coordinates": [1, 2]}, "location"
            )

    def test_missing_coordinates_are_rejected(self):
        with self.assertRaises(ValueError):
            validate_geojson_geometry({"type": "Point"}, "location")

    def test_non_numeric_coordinates_are_rejected(self):
        with self.assertRaises(ValueError):
            validate_geojson_geometry(
                {"type": "Point", "coordinates": ["a", "b"]}, "location"
            )

    def test_wrong_nesting_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_geojson_geometry(
                {"type": "Point", "coordinates": [[1, 2]]}, "location"
            )

    def test_plain_string_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_geojson_geometry("not geojson", "location")

    def test_invalid_geometry_inside_collection_is_rejected(self):
        with self.assertRaises(ValueError):
            validate_geojson_geometry(
                {
                    "type": "GeometryCollection",
                    "geometries": [{"type": "Point"}],
                },
                "feature",
            )


if __name__ == "__main__":
    unittest.main()
