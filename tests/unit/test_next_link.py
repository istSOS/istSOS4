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

"""Regression tests for collection pagination links."""

import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from app.utils.utils import build_nextLink


class NextLinkTest(unittest.TestCase):
    """Pagination must advance the requested offset."""

    def test_zero_top_has_no_next_link(self):
        """An empty requested page must not link back to the same offset."""
        for query in ("$top=0", "$top=0&$skip=20&$count=true"):
            with self.subTest(query=query):
                self.assertIsNone(build_nextLink(f"/Things?{query}", 1))

    @patch("app.utils.utils.TOP_VALUE", 0)
    def test_zero_default_top_has_no_next_link(self):
        """The configured page size must also make progress."""
        self.assertIsNone(build_nextLink("/Things?$count=true", 1))

    def test_positive_top_advances_skip_and_preserves_query(self):
        """An extra fetched row should still produce the next page link."""
        link = build_nextLink("/Things?$top=10&$skip=20&$count=true", 11)
        query = parse_qs(urlparse(link).query)
        self.assertEqual(
            query, {"$top": ["10"], "$skip": ["30"], "$count": ["true"]}
        )

    def test_last_page_has_no_next_link(self):
        """Without an extra row there is no next page."""
        self.assertIsNone(build_nextLink("/Things?$top=10&$skip=20", 10))
