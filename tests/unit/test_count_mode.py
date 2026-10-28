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

from app.sta2rest.sta2rest import STA2REST

QUERY = "/istsos4/v1.1/Things?$count=true&$top=1"


class CountModeTest(unittest.TestCase):
    def test_full_mode_builds_one_count_query(self):
        with patch("app.sta2rest.visitors.COUNT_MODE", "FULL"):
            result = STA2REST.convert_query(QUERY)

        self.assertEqual(1, len(result["count_queries"]))

    def test_estimate_modes_build_limited_count_query(self):
        for mode in ("LIMIT_ESTIMATE", "ESTIMATE_LIMIT"):
            with self.subTest(mode=mode):
                with patch("app.sta2rest.visitors.COUNT_MODE", mode):
                    result = STA2REST.convert_query(QUERY)

                count_queries = result["count_queries"]
                self.assertEqual(2, len(count_queries))
                limited = (
                    count_queries[0]
                    if mode == "LIMIT_ESTIMATE"
                    else count_queries[1]
                )
                self.assertIn("count(*)", limited)
                self.assertIn("LIMIT", limited)


if __name__ == "__main__":
    unittest.main()
