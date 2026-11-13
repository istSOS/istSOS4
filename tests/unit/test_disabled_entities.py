# Copyright 2026 SUPSI
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
from unittest.mock import patch

from app.sta2rest.odata_query.exceptions import InvalidCollectionException
from app.sta2rest.sta2rest import STA2REST, VERSION

MODULE = "app.sta2rest.sta2rest"


def uri(path):
    return f"/istsos4{VERSION}/{path}"


class DisabledEntitiesTest(unittest.TestCase):
    def flags(self, network=False, versioning=False, authorization=False):
        return (
            patch(f"{MODULE}.NETWORK", network),
            patch(f"{MODULE}.VERSIONING", versioning),
            patch(f"{MODULE}.AUTHORIZATION", authorization),
        )

    def test_disabled_collections_are_unknown(self):
        n, v, a = self.flags()
        with n, v, a:
            for path in ("Networks", "Networks(1)", "Commits", "Commits(1)"):
                with self.subTest(path=path):
                    with self.assertRaises(InvalidCollectionException):
                        STA2REST.parse_uri(uri(path))

    def test_disabled_navigation_is_unknown(self):
        n, v, a = self.flags()
        with n, v, a:
            for path in ("Datastreams(1)/Network", "Things(1)/Commit"):
                with self.subTest(path=path):
                    with self.assertRaises(InvalidCollectionException):
                        STA2REST.parse_uri(uri(path))

    def test_enabled_entities_still_parse(self):
        cases = [
            (self.flags(network=True), "Networks(1)", ("Network", "1")),
            (self.flags(versioning=True), "Commits", ("Commit", None)),
            (self.flags(authorization=True), "Commits", ("Commit", None)),
        ]
        for (n, v, a), path, expected in cases:
            with self.subTest(path=path), n, v, a:
                self.assertEqual(
                    expected, STA2REST.parse_uri(uri(path))["entity"]
                )

    def test_other_entities_unaffected(self):
        n, v, a = self.flags()
        with n, v, a:
            result = STA2REST.parse_uri(uri("Things(1)/Datastreams"))
            self.assertEqual(("Datastream", None), result["entity"])


if __name__ == "__main__":
    unittest.main()
