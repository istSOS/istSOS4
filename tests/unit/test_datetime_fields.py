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

from app.utils.utils import handle_datetime_fields


class HandleDatetimeFieldsTest(unittest.TestCase):
    def test_valid_instant_sets_start_and_end(self):
        payload = {"phenomenonTime": "2020-01-01T00:00:00Z"}
        handle_datetime_fields(payload)
        self.assertEqual(
            payload["phenomenonTimeStart"], payload["phenomenonTimeEnd"]
        )
        self.assertEqual(payload["phenomenonTimeStart"].year, 2020)

    def test_valid_interval_sets_start_and_end(self):
        payload = {
            "phenomenonTime": "2020-01-01T00:00:00Z/2020-01-02T00:00:00Z"
        }
        handle_datetime_fields(payload)
        self.assertEqual(payload["phenomenonTimeStart"].day, 1)
        self.assertEqual(payload["phenomenonTimeEnd"].day, 2)

    def test_missing_phenomenon_time_is_left_to_the_caller(self):
        payload = {"result": 1}
        handle_datetime_fields(payload)
        self.assertNotIn("phenomenonTimeStart", payload)

    def test_invalid_instant_is_rejected(self):
        with self.assertRaises(ValueError):
            handle_datetime_fields({"phenomenonTime": "notadate"})

    def test_interval_with_invalid_end_is_rejected(self):
        with self.assertRaises(ValueError):
            handle_datetime_fields(
                {"phenomenonTime": "2020-01-01T00:00:00Z/garbage"}
            )

    def test_non_string_time_is_rejected(self):
        with self.assertRaises(ValueError):
            handle_datetime_fields({"phenomenonTime": 123})

    def test_invalid_result_time_is_rejected(self):
        with self.assertRaises(ValueError):
            handle_datetime_fields({"resultTime": "yesterday-ish"})


if __name__ == "__main__":
    unittest.main()
